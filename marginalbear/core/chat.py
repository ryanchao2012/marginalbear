# import os
import json
import time
import random
import numpy as np
import logging
from collections import Counter
from core.ranking import jaccard_similarity
from .utils import (
    OkLogger, Vocab, Post, Comment, Title, Word
)

from core.utils import (
    PsqlQuery, PsqlQueryScript,
    deprecated
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
    max_query_title_num = 20000
    max_query_comment_num = 1000
    max_vocab_postfreq = 10000

    query_title_by_id_sql = '''
        SELECT pttcorpus_title.* FROM pttcorpus_title, pttcorpus_post
        WHERE pttcorpus_title.id IN %(id_)s
        AND tokenizer = %(tok)s
        AND post_id = pttcorpus_post.id
        ORDER BY (pttcorpus_post.quality, pttcorpus_title.quality, publish_date) DESC;
    '''

    query_comment_by_id_sql = '''
        SELECT * FROM pttcorpus_comment
        WHERE id IN %(id_)s
        AND tokenizer = %(tok)s
        ORDER BY quality DESC;
    '''

    guery_vocab_group_by_title_using_vocab_id_sql = '''
        SELECT title_id, array_agg(vocabulary_id) AS vocabulary_group
        FROM pttcorpus_vocabulary_title
        WHERE title_id IN
            (SELECT title_id
             FROM pttcorpus_vocabulary_title
             WHERE vocabulary_id IN %(vid)s
             AND title_id NOT IN %(tid)s)
        GROUP BY title_id;
    '''

    guery_vocab_group_by_comment_id_sql = '''
        SELECT comment_id, array_agg(vocabulary_id) AS vocabulary_group
        FROM pttcorpus_vocabulary_comment
        WHERE comment_id IN %s
        GROUP BY comment_id;
    '''

    guery_vocab_group_by_title_id_sql = '''
        SELECT title_id, array_agg(vocabulary_id) AS vocabulary_group
        FROM pttcorpus_vocabulary_title
        WHERE title_id IN %s
        GROUP BY title_id;
    '''

    def __init__(
        self,
        tokenizer_tag,
        excluded_post_ids=[],
        excluded_title_ids=[],
        excluded_comment_ids=[],
        pweight={},
        title_ranker=jaccard_similarity,
        logger_name='retrievalbase'
    ):
        self.excluded_post_ids = excluded_post_ids
        self.excluded_title_ids = excluded_title_ids
        self.excluded_comment_ids = excluded_comment_ids
        self.tokenizer_tag = tokenizer_tag
        self.pweight = pweight
        self.title_ranker = title_ranker
        self.logger = logging.getLogger(logger_name)

    def _construct_vocab(self, tp, schema):
        v = Vocab(
            tp[schema['word']],
            pos=tp[schema['pos']],
            weight=self.pweight.get(tp[schema['pos']], 1.0),
            tokenizer=tp[schema['tokenizer']],
            commentfreq=tp[schema['commentfreq']],
            titlefreq=tp[schema['titlefreq']],
            quality=tp[schema['quality']],
            stopword=tp[schema['stopword']],
            id_=tp[schema['id']]
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

    @deprecated
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

    def query_vocab_quality_by_id(self, vocab_word):
        psql = PsqlQuery()
        vocab, vschema = psql.query_all(
            self.query_vocab_quality_by_word_sql, (vocab_word,)
        )
        return vocab, vschema

    def query_title_quality_by_id(self, title_id):
        psql = PsqlQuery()
        vocab, vschema = psql.query_all(
            self.query_title_quality_by_id_sql, (title_id,)
        )
        return vocab, vschema

    def query_comment_quality_by_id(self, comment_id):
        psql = PsqlQuery()
        vocab, vschema = psql.query_all(
            self.query_comment_quality_by_id_sql, (comment_id,)
        )
        return vocab, vschema

    @deprecated
    def query_comment_quality_by_id(self, comment_id):
        psql = PsqlQuery()
        vocab, vschema = psql.query_all(
            self.query_comment_quality_by_id_sql, (comment_id,)
        )
        return vocab, vschema

    @deprecated
    def query_vocab_by_title_id(self, title_id):
        tid = list(set(title_id))

        psql = PsqlQuery()
        vocab2title, schema = psql.query_all(
            self.query_vocab2post_by_tid_sql, (tuple(tid),)
        )

        vocab_id = list({v2t[schema['vocabulary_id']] for v2t in vocab2title})
        vocab, vschema = psql.query_all(
            self.query_vocab_by_id, (tuple(vocab_id),)
        )

        return vocab, vschema

    @deprecated
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

    @deprecated
    def query_vocab2post(self, vocab_id):
        psql = PsqlQuery()
        vocab2post, schema = psql.query_all(
            self.query_vocab2post_by_vid_sql, (tuple(vocab_id),)
        )

        return [v2p[schema['post_id']] for v2p in vocab2post]

    def query_vocab2title(self, vocab_id):
        psql = PsqlQuery()
        vocab2post, schema = psql.query_all(
            self.query_vocab2title_by_vid_sql, (tuple(vocab_id),)
        )

        return vocab2post, schema

    def query_post_by_id(self, post_id):
        psql = PsqlQuery()
        post = psql.query(self.query_post_by_id_sql, (tuple(post_id),))
        schema = psql.schema

        return post, schema

    def query_title_by_id(self, title_id):
        psql = PsqlQuery()
        title = psql.query(
            self.query_title_by_id_sql,
            {'id_': tuple(title_id), 'tok': self.tokenizer_tag}
        )
        schema = psql.schema

        return title, schema

    def query_comment_by_id(self, comment_id):
        psql = PsqlQuery()
        comment = psql.query(
            self.query_comment_by_id_sql,
            {'id_': tuple(comment_id), 'tok': self.tokenizer_tag}
        )
        schema = psql.schema

        return comment, schema

    @deprecated
    def query_title_by_post(self, post_id):
        bundle = [(id_, self.tokenizer_tag) for id_ in post_id]
        psql = PsqlQuery()
        title, schema = psql.query_all(self.query_title_by_unique_sql, (tuple(bundle),))

        return title, schema

    def query_comment_by_post(self, post_id):
        bundle = [(id_, self.tokenizer_tag) for id_ in post_id]
        psql = PsqlQuery()
        comment, schema = psql.query_all(self.query_comment_by_unique_sql, (tuple(bundle),))

        return comment, schema

    def get_comment_obj(self, post_id):
        if not bool(post_id):
            return []

        # Bottleneck ?
        comments, cmtschema = self.query_comment_by_post(post_id)
        #

        cmtid = [cmt[cmtschema['id']] for cmt in comments]
        cmt2vocab, c2vschema = self.guery_vocab_group_by_comment_id(cmtid)

        vid = list({v for c2v in cmt2vocab for v in c2v[c2vschema['vocabulary_group']]})

        if not bool(cmtid):
            return []

        psql = PsqlQuery()
        cvocab, vschema = psql.query_all(self.query_vocab_by_id_sql, (tuple(vid),))

        c2v_dict = {
            c2v[c2vschema['comment_id']]: c2v[c2vschema['vocabulary_group']]
            for c2v in cmt2vocab
        }

        v_dict = {
            v[vschema['id']]: v
            for v in cvocab
        }
        comment_objs = []
        for i, cmt in enumerate(comments):
            if cmt[cmtschema['id']] not in self.excluded_comment_ids:
                if cmt[cmtschema['id']] in c2v_dict:
                    vocabs = [
                        self._construct_vocab(v_dict[vid], vschema)
                        for vid in c2v_dict[cmt[cmtschema['id']]]
                    ]
                else:
                    vocabs = []

                comment_objs.append(
                    Comment(
                        vocabs,
                        self.tokenizer_tag,
                        post_id=cmt[cmtschema['post_id']],
                        audience=cmt[cmtschema['audience_id']],
                        quality=cmt[cmtschema['quality']],
                        ctype=cmt[cmtschema['ctype']],
                        retrieval_count=cmt[cmtschema['retrieval_count']],
                        floor=cmt[cmtschema['floor']],
                        id_=cmt[cmtschema['id']],
                        body=''.join(cmt[cmtschema['tokenized']].split())
                    )
                )

            if i > self.max_query_comment_num:
                break

        return comment_objs

    @deprecated
    def query_vocab_group_by_title(self, title_id):
        psql = PsqlQuery()
        title2vocab, schema = psql.query_all(
            self.query_vocab_group_by_title, (tuple(title_id),)
        )

        return title2vocab, schema

    @deprecated
    def guery_vocab_group_by_title_using_vocab(self, vocab_id, ex_title_id):
        psql = PsqlQuery()

        if not bool(ex_title_id):
            ex_title_id = [-1]

        title2vocab, schema = psql.query_all(
            self.guery_vocab_group_by_title_using_vocab_id_sql,
            {'vid': tuple(vocab_id), 'tid': tuple(ex_title_id)}
        )

        return title2vocab, schema

    def guery_vocab_group_by_title_id(self, title_id):
        psql = PsqlQuery()

        title2vocab, schema = psql.query_all(
            self.guery_vocab_group_by_title_id_sql, (tuple(title_id),)
        )

        return title2vocab, schema

    def guery_vocab_group_by_comment_id(self, comment_id):
        psql = PsqlQuery()

        comment2vocab, schema = psql.query_all(
            self.guery_vocab_group_by_comment_id_sql, (tuple(comment_id),)
        )

        return comment2vocab, schema

    def get_post_obj(self, post_id):
        post, pschema = self.query_post_by_id(post_id)

        post_objs = []

        for p in post:
            if p[pschema['id']] not in self.excluded_post_ids:
                post_objs.append(
                    Post(
                        publish_date=p[pschema['publish_date']],
                        quality=p[pschema['quality']],
                        spider=p[pschema['spider']],
                        author=p[pschema['author_id']],
                        url=p[pschema['url']],
                        id_=p[pschema['id']]
                    )
                )

        return post_objs

    def get_title_obj(self, vocab_id):
        if not bool(vocab_id):
            return []

        # Bottleneck ?
        v2t, v2tschema = self.query_vocab2title(vocab_id)
        fltr_tid = [
            q[v2tschema['title_id']]
            for q in v2t
            if q[v2tschema['title_id']] not in self.excluded_title_ids
        ]
        #

        title2vocab, t2vschema = self.guery_vocab_group_by_title_id(fltr_tid)

        tid = list({t2v[t2vschema['title_id']] for t2v in title2vocab})
        vid = list({v for t2v in title2vocab for v in t2v[t2vschema['vocabulary_group']]})

        if not bool(tid):
            return []

        title_generator, tschema = self.query_title_by_id(tid)

        psql = PsqlQuery()
        tvocab, vschema = psql.query_all(self.query_vocab_by_id_sql, (tuple(vid),))

        t2v_dict = {
            t2v[t2vschema['title_id']]: t2v[t2vschema['vocabulary_group']]
            for t2v in title2vocab
        }
        v_dict = {
            v[vschema['id']]: v
            for v in tvocab
        }
        title_objs = []
        for i, tt in enumerate(title_generator):
            if tt[tschema['post_id']] not in self.excluded_post_ids:
                vocabs = [
                    self._construct_vocab(v_dict[vid], vschema)
                    for vid in t2v_dict[tt[tschema['id']]]
                ]

                title_objs.append(
                    Title(
                        vocabs,
                        self.tokenizer_tag,
                        post_id=tt[tschema['post_id']],
                        quality=tt[tschema['quality']],
                        ctype=tt[tschema['quality']],
                        retrieval_count=tt[tschema['quality']],
                        body=''.join(tt[tschema['tokenized']].split()),
                        id_=tt[tschema['id']]
                    )
                )
            if i >= self.max_query_title_num:
                break

        return title_objs

    def get_vocab_obj(self, words):
        qvocab, vschema = self.query_vocab_by_words(words)
        query_vocabs = [
            self._construct_vocab(v, vschema)
            for v in qvocab
        ]

        return query_vocabs

    def ranking(self, rank, target, top_num, threshold=0.0):
        idx_ranking = np.asarray(rank).argsort()[::-1]
        top_results = []
        max_rank = max(rank)
        scores = []
        for m, idx in enumerate(idx_ranking, 1):
            if m > top_num or (rank[idx] / max_rank) < threshold:
                break
            top_results.append(target[idx])
            scores.append(rank[idx])

        return top_results, scores


class RetrievalEvaluate(RetrievalBase):

    max_top_title_num = 100
    max_top_comment_num = 1000
    similarity_ranking_threshold = 0.8

    comp_words = {
        '你': [Word('大家', pos='n')],
        '你們': [Word('大家', pos='n')],
        '我': [Word('肥宅', pos='n')]
    }

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

    def get_top_titles(self, words):
        if not bool(words):
            return []
        tic = time.time()
        vocabs_objs = self.get_vocab_obj(words)
        for v in vocabs_objs:
            self.logger.info('{}, {}, {}, {}'.format(v.word, v.pos, v.stopword, v.titlefreq))
        self.logger.info('Fetch {} vocab objs'.format(len(vocabs_objs)))
        self.logger.info('Elapsed time @get_vocab_obj: {:.2f}'.format(time.time() - tic))

        fltr_vocab_id = [v.id_ for v in vocabs_objs if not (v.stopword or v.pos == 'x')]

        tic = time.time()
        title_objs = self.get_title_obj(fltr_vocab_id)
        self.logger.info('Fetch {} title objs'.format(len(title_objs)))
        self.logger.info('Elapsed time @get_title_obj: {:.2f}'.format(time.time() - tic))

        if not bool(title_objs):
            return []

        title_score = [self.title_ranker(vocabs_objs, tt.vocabs) for tt in title_objs]
        top_titles, top_title_scores = self.ranking(
            title_score,
            title_objs,
            self.max_top_title_num,
            self.similarity_ranking_threshold
        )

        for title, score in zip(top_titles, top_title_scores):
            title.score = score

        return top_titles

    def get_top_comments(self, top_titles):
        if not bool(top_titles):
            return []
        title_score = [tt.score for tt in top_titles]
        max_title_score = max(title_score)
        post_id = [tt.post_id for tt in top_titles]

        title_score_dict = {tt.post_id: tt.score for tt in top_titles}

        tic = time.time()
        comment_objs = self.get_comment_obj(post_id)
        self.logger.info('Fetch {} comment objs'.format(len(comment_objs)))
        self.logger.info('Elapsed time @query_comments: {:.2f}'.format(time.time() - tic))
        # query_comments = [cmt for cmt in comment_objs if cmt.ctype == 'text']

        '''
            So now we have query_vocabs(Vocab),
                           top_posts(list of Post) and
                           query_comments(list of Comment)
        '''

        # Calculate document frequency
        cmt_vocab = []
        for cmt in comment_objs:
            vocab = list({
                (v.word, v.pos)
                for v in cmt.vocabs
                if (v.pos[0] in 'nvr') and v.quality > 0.5
            })
            cmt_vocab.extend(vocab)
        docfreq = Counter(cmt_vocab)

        # Calculate total score
        cmt_score = []
        w_docfreq = 2.0
        w_title_sim_score = 1.0
        w_vocab_count = -0.1
        w_is_url = -1.0
        w_cmt_quality = 1.0
        for cmt in comment_objs:
            doc_score = sum([
                docfreq[(v.word, v.pos)]
                for v in cmt.vocabs
                if (v.word, v.pos) in docfreq
            ]) / (len(cmt.vocabs) + 1.0)

            # tokenized = ' '.join([v.word for v in cmt.vocabs])

            cmt_score.append(
                w_docfreq * doc_score +
                w_title_sim_score * title_score_dict[cmt.post_id] / max_title_score +
                w_vocab_count * len(cmt.vocabs) +
                w_is_url * (cmt.ctype == 'url') +
                w_cmt_quality * cmt.quality

            )

        top_comments, top_comment_scores = self.ranking(
            cmt_score,
            comment_objs,
            self.max_top_comment_num,
        )
        for cmt, score in zip(top_comments, top_comment_scores):
            cmt.score = score

        return top_comments

    def retrieve(self, words):
        complement_words = self.add_compwords(words)
        top_titles = self.get_top_titles(complement_words)
        top_comments = self.get_top_comments(top_titles)

        return top_comments

    def add_compwords(self, words):
        complement_words = []
        for w in words:
            if w.word in self.comp_words:
                complement_words.extend(self.comp_words[w.word])
        complement_words.extend(words)

        return complement_words



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
