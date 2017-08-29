# import os
import json
import time
import random
import numpy as np
import logging
from collections import Counter
from core.ranking import jaccard_similarity
from .utils import (
    OkLogger, Vocab, Post, Comment
)
from core.utils import (
    PsqlQuery, PsqlQueryScript,
)

# from linebot import LineBotApi, WebhookParser
# from linebot.exceptions import InvalidSignatureError, LineBotApiError
# from linebot.models import (
#     FollowEvent, ImageSendMessage, JoinEvent, LeaveEvent,
#     MessageEvent, SourceGroup, SourceRoom, SourceUser,
#     TextMessage, TextSendMessage, UnfollowEvent,
# )

chat_logger = OkLogger('retrievalbase')


class PsqlChatCacheScript(object):
    insert_chattree_sql = '''
        INSERT INTO chat_app_chattree(user_id, ancestor, query, keyword, reply, time, post, push_num)
        SELECT %(user_id)s, %(ancestor)s, %(query)s, %(keyword)s,
               %(reply)s, %(time)s, %(post)s, %(push_num)s
        RETURNING id;
    '''

    update_chattree_sql = '''
        UPDATE chat_app_chattree SET successor = %(successor)s WHERE id = %(id_)s;
    '''

    query_chatuser_sql = '''
        SELECT * FROM chat_app_chatuser
        WHERE uid = %(uid)s AND platform = %(platform)s;
    '''

    upsert_chatuser_sql = '''
        INSERT INTO chat_app_chatuser(platform, uid, idtype, active, state, chat_count)
        SELECT %(platform)s, %(uid)s, %(idtype)s, %(active)s, %(state)s, %(chat_count)s
        ON CONFLICT (platform, uid) DO
        UPDATE SET
        active = EXCLUDED.active,
        state = EXCLUDED.state,
        chat_count = chat_app_chatuser.chat_count + 1;
    '''

    query_chatcache_sql = '''
        SELECT * FROM chat_app_chatcache WHERE user_id = %s;
    '''

    upsert_chatcache_sql = '''
        INSERT INTO chat_app_chatcache(user_id, query, keyword, reply, time, repeat, post, push_num, tree_node)
        SELECT %(user_id)s, %(query)s, %(keyword)s, %(reply)s, %(time)s, %(repeat)s, %(post)s, %(push_num)s, %(tree_node)s
        ON CONFLICT (user_id) DO
        UPDATE SET
        query = EXCLUDED.query,
        keyword = EXCLUDED.keyword,
        reply = EXCLUDED.reply,
        time = EXCLUDED.time,
        repeat = EXCLUDED.repeat,
        post = EXCLUDED.post,
        push_num = EXCLUDED.push_num,
        tree_node = EXCLUDED.tree_node;
    '''


class RetrievalBase(PsqlQueryScript):
    max_query_post_num = 20000
    max_vocab_postfreq = 10000

    query_post_by_id_sql = '''
        SELECT * FROM pttcorpus_post WHERE id IN %s ORDER BY publish_date DESC;
    '''

    def __init__(
        self,
        tokenizer_tag,
        excluded_post_ids=[],
        pweight={},
        ranker=jaccard_similarity,
        logger_name='retrievalbase'
    ):
        self.excluded_post_ids = excluded_post_ids
        self.tokenizer_tag = tokenizer_tag
        self.pweight = pweight
        self.ranker = ranker
        self.logger = logging.getLogger(logger_name)

    def _build_vocab(self, tp, schema):
        v = Vocab(
            tp[schema['word']],
            pos=tp[schema['pos']],
            weight=self.pweight.get(tp[schema['pos']], 1.0),
            tokenizer=tp[schema['tokenizer']],
            postfreq=tp[schema['postfreq']],
            commentfreq=tp[schema['commentfreq']],
            quality=tp[schema['quality']],
            stopword=tp[schema['stopword']]
        )
        return v

    def retrieve(self, words):
        raise NotImplementedError

    def query_vocab_by_words(self, wds, relative_words=None):
        words = list(wds)
        if bool(relative_words):
            try:
                words += list(relative_words)
            except Exception as err:
                self.logger.warning(err)

        bundle = [(w.word, w.pos, self.tokenizer_tag) for w in words]

        psql = PsqlQuery()
        qvocab, vschema = psql.query_all(self.query_vocab_sql, (tuple(bundle),))

        return qvocab, vschema

    def query_vocab_by_post_id(self, post_id):
        pid = list(set(post_id))

        psql = PsqlQuery()
        vocab2post, schema = psql.query_all(
            self.query_vocab2post_by_pid_sql, (tuple(pid),)
        )

        vocab_id = list({v2p[schema['vocabulary_id']] for v2p in vocab2post})
        vocab, vschema = psql.query_all(
            self.query_vocab_by_id, (tuple(vocab_id),)
        )

        return vocab, vschema

    def query_vocab_by_comment_id(self, comment_id):
        cmtid = list(set(comment_id))

        psql = PsqlQuery()
        vocab2comment, schema = psql.query_all(
            self.query_vocab2comment_by_cmtid_sql, (tuple(cmtid),)
        )

        vocab_id = list({v2c[schema['vocabulary_id']] for v2c in vocab2comment})
        vocab, vschema = psql.query_all(
            self.query_vocab_by_id, (tuple(vocab_id),)
        )

        return vocab, vschema

    def query_vocab2post(self, vocab_id):
        psql = PsqlQuery()
        vocab2post, schema = psql.query_all(
            self.query_vocab2post_by_vid_sql, (tuple(vocab_id),)
        )

        return [v2p[schema['post_id']] for v2p in vocab2post]

    def query_post_by_id(self, post_id):
        psql = PsqlQuery()
        post = psql.query(self.query_post_by_id_sql, (tuple(post_id),))
        schema = psql.schema

        return post, schema

    def query_title_by_post(self, post_id):
        bundle = [(id_, self.tokenizer_tag) for id_ in post_id]
        psql = PsqlQuery()
        title, schema = psql.query_all(self.query_title_sql, (tuple(bundle),))

        return title, schema

    def query_comment_by_post(self, post_id):
        bundle = [(id_, self.tokenizer_tag) for id_ in post_id]
        psql = PsqlQuery()
        comment, schema = psql.query_all(self.query_comment_sql, (tuple(bundle),))

        return comment, schema

    def get_comment_obj(self, post_id):
        if not bool(post_id):
            return [], []
        comments, cmtschema = self.query_comment_by_post(post_id)
        cmt_id = [cmt[cmtschema['id']] for cmt in comments]
        cmtvocab, vschema = self.query_vocab_by_comment_id(cmt_id)
        cmtvocab_dict = {
            (v[vschema['word']], v[vschema['pos']], self.tokenizer_tag): v
            for v in cmtvocab
        }

        query_comments = []
        for idx, cmt in enumerate(comments):
            unique_v = list({
                (w, p, self.tokenizer_tag)
                for w, p in zip(
                    cmt[cmtschema['tokenized']].split(), cmt[cmtschema['grammar']].split()
                )
            })
            doc_vocabs = [
                self._build_vocab(cmtvocab_dict[v], vschema)
                for v in unique_v if v in cmtvocab_dict
            ]

            query_comments.append(
                Comment(
                    doc_vocabs,
                    self.tokenizer_tag,
                    post_id=cmt[cmtschema['post_id']],
                    quality=cmt[cmtschema['quality']],
                    ctype=cmt[cmtschema['ctype']],
                    category=cmt[cmtschema['category']],
                    retrieval_count=cmt[cmtschema['retrieval_count']],
                    floor=cmt[cmtschema['floor']],
                    body=''.join(cmt[cmtschema['tokenized']].split())
                )
            )

        return query_comments, cmt_id

    def get_post_obj(self, vocab_id):
        if not bool(vocab_id):
            return [], []
        allpost_id = self.query_vocab2post(vocab_id)
        posts_generator, pschema = self.query_post_by_id(allpost_id)

        posts = []
        for i, p in enumerate(posts_generator):
            if p[pschema['id']] not in self.excluded_post_ids:
                posts.append(p)
            if i > self.max_query_post_num:
                break
        post_id = [p[pschema['id']] for p in posts]
        if not bool(post_id):
            return [], []
        pvocab, vschema = self.query_vocab_by_post_id(post_id)
        pvocab_dict = {
            (v[vschema['word']], v[vschema['pos']], self.tokenizer_tag): v
            for v in pvocab
        }
        post_dict = {p[pschema['id']]: p for p in posts}
        titles_generator, tschema = self.query_title_by_post(post_id)
        titles = list(titles_generator)

        query_posts = []
        for tt in titles:
            unique_v = list({
                (w, p, self.tokenizer_tag)
                for w, p in zip(
                    tt[tschema['tokenized']].split(), tt[tschema['grammar']].split()
                )
            })

            doc_vocabs = [
                self._build_vocab(pvocab_dict[v], vschema)
                for v in unique_v if v in pvocab_dict
            ]
            pid = tt[tschema['post_id']]
            query_posts.append(
                Post(
                    doc_vocabs,
                    self.tokenizer_tag,
                    publish_date=post_dict[pid][pschema['publish_date']],
                    quality=post_dict[pid][pschema['quality']],
                    similarity_score=0.0,
                    retrieval_count=tt[tschema['retrieval_count']],
                    category=None,
                    author=post_dict[pid][pschema['author_id']],
                    post_id=post_dict[pid][pschema['id']],
                    url=post_dict[pid][pschema['url']],
                    body=''.join(tt[tschema['tokenized']].split())
                )
            )
            # p = query_posts[-1] ??
        return query_posts, post_id

    def get_vocab_obj(self, words):
        qvocab, vschema = self.query_vocab_by_words(words)
        query_vocabs = [
            self._build_vocab(v, vschema)
            for v in qvocab
        ]

        vocab_id = [
            v[vschema['id']]
            for v in qvocab
            if not (v[vschema['stopword']]) and
            v[vschema['postfreq']] < self.max_vocab_postfreq
        ]

        return query_vocabs, vocab_id

    def ranking(self, rank, target, top_num, threshold=0.0):
        idx_ranking = np.asarray(rank).argsort()[::-1]
        top_results = []
        max_rank = max(rank)
        scores = []
        for m, idx in enumerate(idx_ranking):
            if m > top_num or (rank[idx] / max_rank) < threshold:
                break
            top_results.append(target[idx])
            scores.append(rank[idx])

        return top_results, scores


class RetrievalEvaluate(RetrievalBase):

    max_top_post_num = 10
    max_top_comment_num = 50
    similarity_ranking_threshold = 0.8

    def __call__(self, words):
        return self.retrieve(words)

    def get_top_posts(self, words):
        if not bool(words):
            return []
        tic = time.time()
        query_vocabs, vocab_id = self.get_vocab_obj(words)
        self.logger.info('Elapsed time @query_vocabs: {:.2f}'.format(time.time() - tic))
        tic = time.time()
        query_posts, pschema = self.get_post_obj(vocab_id)
        if not bool(query_posts):
            return []
        self.logger.info('Elapsed time @query_posts: {:.2f}'.format(time.time() - tic))
        tic = time.time()

        post_score = [self.ranker(query_vocabs, p.vocabs) for p in query_posts]

        top_posts, top_post_scores = self.ranking(post_score, query_posts, self.max_top_post_num, self.similarity_ranking_threshold)

        for post, score in zip(top_posts, top_post_scores):
            post.similarity_score = score

            # self.logger.warning('Retrieved: [{:.2f}] {}'.format(post.similarity_score, post.body))

        return top_posts

    def get_top_comments(self, top_posts):
        if not bool(top_posts):
            return []
        post_score = [p.similarity_score for p in top_posts]
        max_post_score = max(post_score)

        tic = time.time()
        post_id = [p.post_id for p in top_posts]
        post_score_dict = {p.post_id: p.similarity_score for p in top_posts}
        query_comments_, _ = self.get_comment_obj(post_id)
        self.logger.info('Elapsed time @query_comments: {:.2f}'.format(time.time() - tic))
        query_comments = [cmt for cmt in query_comments_ if cmt.ctype == 'text']

        '''
            So now we have query_vocabs(Vocab),
                           top_posts(list of Post) and
                           query_comments(list of Comment)
        '''

        # Calculate document frequency
        cmt_vocab = []
        for cmt in query_comments:
            vocab = list({(v.word, v.pos) for v in cmt.vocabs if v.pos[0] == 'n' or v.pos[0] == 'v'})
            cmt_vocab.extend(vocab)

        docfreq = Counter(cmt_vocab)

        # Calculate total score
        cmt_score = []
        w1, w2, w3 = 0.1, 20.0, 0.01
        for cmt in query_comments:
            doc_score = sum([
                docfreq[(v.word, v.pos)] - 1
                for v in cmt.vocabs
                if (v.word, v.pos) in docfreq
            ]) / (len(cmt.vocabs) + 1.0)

            cmt_score.append(
                w1 * doc_score +
                w2 * post_score_dict[cmt.post_id] / max_post_score +
                w3 * len(cmt.vocabs)
            )

        top_comments, top_comment_scores = self.ranking(cmt_score, query_comments, self.max_top_comment_num)
        for cmt, score in zip(top_comments, top_comment_scores):
            cmt.score = score

        return top_comments

    def retrieve(self, words):

        top_posts = self.get_top_posts(words)
        top_comments = self.get_top_comments(top_posts)

        return top_comments


class RetrievalJaccard(RetrievalBase):
    """

    """
    max_top_post_num = 10
    max_top_comment_num = 20
    similarity_ranking_threshold = 0.9

    def __call__(self, words):
        return self.retrieve(words)

    def _get_comment_obj(self, post_id):
        comments, cmtschema = self.query_comment_by_post(post_id)
        cmt_id = [cmt[cmtschema['id']] for cmt in comments]
        cmtvocab, vschema = self.query_vocab_by_comment_id(cmt_id)
        cmtvocab_dict = {
            (v[vschema['word']], v[vschema['pos']], self.tokenizer_tag): v
            for v in cmtvocab
        }

        query_comments = []
        for idx, cmt in enumerate(comments):
            unique_v = list({
                (w, p, self.tokenizer_tag)
                for w, p in zip(
                    cmt[cmtschema['tokenized']].split(), cmt[cmtschema['grammar']].split()
                )
            })

            doc_vocabs = [
                self._build_vocab(cmtvocab_dict[v], vschema)
                for v in unique_v if v in cmtvocab_dict
            ]

            query_comments.append(
                Comment(
                    doc_vocabs,
                    self.tokenizer_tag,
                    post_id=cmt[cmtschema['post_id']],
                    quality=cmt[cmtschema['quality']],
                    ctype=cmt[cmtschema['ctype']],
                    category=cmt[cmtschema['category']],
                    retrieval_count=cmt[cmtschema['retrieval_count']],
                    floor=cmt[cmtschema['floor']],
                    body=''.join(cmt[cmtschema['tokenized']].split())
                )
            )

        return query_comments, cmt_id

    def _get_post_obj(self, vocab_id):
        allpost_id = self.query_vocab2post(vocab_id)
        posts_generator, pschema = self.query_post_by_id(allpost_id)

        posts = []
        for i, p in enumerate(posts_generator):
            posts.append(p)
            if i > self.max_query_post_num:
                break
        post_id = [p[pschema['id']] for p in posts]
        pvocab, vschema = self.query_vocab_by_post_id(post_id)
        pvocab_dict = {
            (v[vschema['word']], v[vschema['pos']], self.tokenizer_tag): v
            for v in pvocab
        }
        post_dict = {p[pschema['id']]: p for p in posts}
        titles_generator, tschema = self.query_title_by_post(post_id)
        titles = list(titles_generator)

        query_posts = []
        for tt in titles:
            unique_v = list({
                (w, p, self.tokenizer_tag)
                for w, p in zip(
                    tt[tschema['tokenized']].split(), tt[tschema['grammar']].split()
                )
            })

            doc_vocabs = [
                self._build_vocab(pvocab_dict[v], vschema)
                for v in unique_v if v in pvocab_dict
            ]
            pid = tt[tschema['post_id']]
            query_posts.append(
                Post(
                    doc_vocabs,
                    self.tokenizer_tag,
                    publish_date=post_dict[pid][pschema['publish_date']],
                    quality=post_dict[pid][pschema['quality']],
                    similarity_score=0.0,
                    retrieval_count=tt[tschema['retrieval_count']],
                    category=None,
                    author=post_dict[pid][pschema['author_id']],
                    post_id=post_dict[pid][pschema['id']],
                    url=post_dict[pid][pschema['url']],
                    body=''.join(tt[tschema['tokenized']].split())
                )
            )
            p = query_posts[-1]
        return query_posts, post_id

    def _get_query_vocab_obj(self, words):
        qvocab, vschema = self.query_vocab_by_words(words)
        query_vocabs = [
            self._build_vocab(v, vschema)
            for v in qvocab
        ]

        vocab_id = [
            v[vschema['id']]
            for v in qvocab
            if not (v[vschema['stopword']]) and
            v[vschema['postfreq']] < self.max_vocab_postfreq
        ]

        return query_vocabs, vocab_id

    def _ranking(self, rank, target, top_num, threshold=0.0):
        idx_ranking = np.asarray(rank).argsort()[::-1]
        top_results = []
        max_rank = max(rank)
        scores = []
        for m, idx in enumerate(idx_ranking):
            if m > top_num or (rank[idx] / max_rank) < threshold:
                break
            top_results.append(target[idx])
            scores.append(rank[idx])

        return top_results, scores

    def retrieve(self, words):
        tic = time.time()
        query_vocabs, vocab_id = self._get_query_vocab_obj(words)
        self.logger.info('Elapsed time @query_vocabs: {:.2f}'.format(time.time() - tic))
        tic = time.time()
        query_posts, _ = self._get_post_obj(vocab_id)

        self.logger.info('Elapsed time @query_posts: {:.2f}'.format(time.time() - tic))
        tic = time.time()
        post_score = [jaccard_similarity(query_vocabs, p.vocabs) for p in query_posts]
        max_post_score = max(post_score)
        self.logger.info('Elapsed time @calculate_post_similarity: {:.2f}'.format(time.time() - tic))

        top_posts, top_post_scores = self._ranking(post_score, query_posts, self.max_top_post_num, self.similarity_ranking_threshold)

        post_id = [p.post_id for p in top_posts]
        post_score_dict = {p.post_id: p.similarity_score for p in top_posts}
        tic = time.time()
        query_comments, _ = self._get_comment_obj(post_id)
        self.logger.info('Elapsed time @query_comments: {:.2f}'.format(time.time() - tic))

        '''
            So now we have query_vocabs(Vocab),
                           top_posts(list of Post) and
                           query_comments(list of Comment)
        '''

        # Calculate document frequency
        cmt_vocab = []
        for cmt in query_comments:
            vocab = list({(v.word, v.pos) for v in cmt.vocabs if v.pos[0] == 'n' or v.pos[0] == 'v'})
            cmt_vocab.extend(vocab)

        docfreq = Counter(cmt_vocab)

        # Calculate total score
        cmt_score = []
        w1, w2, w3, w4 = 0.1, 20.0, 0.01, 2.0
        for cmt in query_comments:
            doc_score = sum([
                docfreq[(v.word, v.pos)] - 1
                for v in cmt.vocabs
                if (v.word, v.pos) in docfreq
            ]) / (len(cmt.vocabs) + 1.0)

            cmt_score.append(
                w1 * doc_score +
                w2 * post_score_dict[cmt.post_id] / max_post_score +
                w3 * len(cmt.vocabs) +
                w4 * (cmt.ctype == 'url')
            )
        top_comments, top_comment_scores = self._ranking(cmt_score, query_comments, self.max_top_comment_num)

        random.seed(time.time())
        return random.choice(top_comments)


# class RetrievalBot(RetrievalBase, PsqlChatCacheScript):
#     disclaimer = None
#     activate_key = None
#     activate_response = []

#     repeat_time = 10
#     repeat_cold_interval = 60
#     repeat_response = []

#     longquery_limit = 40
#     longquery_response = []

#     kickout_key = []
#     kickout_response = []

#     def __init__(self, query, rule_orm, **kwargs):
#         super(RetrievalBot, self).__init__(query, kwargs)

#         if bool(rule_orm):
#             if not bool(RetrievalBot.disclaimer):
#                 disclaimer = rule_orm.objects.get(rtype='disclaimer')
#                 RetrievalBot.disclaimer = disclaimer.response

#             if not bool(RetrievalBot.repeat_response):
#                 repeat = rule_orm.objects.get(rtype='repeat')
#                 RetrievalBot.repeat_response = [r.strip() for r in repeat.response.split('\n')]
#                 RetrievalBot.repeat_time = int(repeat.keyword)

#             if not(bool(RetrievalBot.kickout_key) and bool(RetrievalBot.kickout_response)):
#                 kickout = rule_orm.objects.get(rtype='kickout')
#                 RetrievalBot.kickout_key = [k.strip() for k in kickout.keyword.split(',')]
#                 RetrievalBot.kickout_response = [r.strip() for r in kickout.response.split('\n')]

#             if not (bool(RetrievalBot.activate_key) and bool(RetrievalBot.activate_response)):
#                 activate = rule_orm.objects.get(rtype='activate')
#                 RetrievalBot.activate_key = [k.strip() for k in activate.keyword.split(',')]
#                 RetrievalBot.activate_response = [r.strip() for r in activate.response.split('\n')]

#             if not bool(RetrievalBot.longquery_response):
#                 longquery = rule_orm.objects.get(rtype='longquery')
#                 RetrievalBot.longquery_limit = int(longquery.keyword)
#                 RetrievalBot.longquery_response = [r.strip() for r in longquery.response.split('\n')]
