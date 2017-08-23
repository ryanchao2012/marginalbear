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


# class RetrievalBase(PsqlQueryScript):
#     # SQL = PsqlQuery
#     pos_weight = {}
#     w2v_model = None

#     ranking_factor = 0.85
#     max_query_post_num = 50000
#     vocab_docfreq_th = 10000
#     max_top_post_num = 5

#     def __init__(self, query, w2v=False, w2v_model_path=None, pos=False, pos_orm=None, tokenizer='jieba', logger_name='retrievalbase'):
#         self.event_time = datetime.now()
#         self.query = query
#         self.logger = logging.getLogger(logger_name)

#         if not bool(RetrievalBase.pos_weight):
#             try:
#                 jpos = pos_orm.objects.all()
#                 for jp in jpos:
#                     RetrievalBase.pos_weight[jp.name] = {'weight': jp.weight, 'punish': jp.punish_factor}
#             except Exception as err:
#                 self.logger.error(err)
#                 RetrievalBase.pos_weight = {}

#         if not bool(RetrievalBase.w2v_model) and w2v:
#             try:
#                 self.logger.info('loading word2vec model...')
#                 RetrievalBase.w2v_model = gensim.models.KeyedVectors.load_word2vec_format(w2v_model_path, binary=True, unicode_errors='ignore')
#                 self.logger.info('loading completed')
#             except Exception as err:
#                 self.logger.error(err)
#                 RetrievalBase.w2v_model = None

#     def retrieve(self, w2v=False, scorer=pos_jaccard_similarity, debug=True):
#         push = ''
#         try:
#             vocab, vid = self._query_vocab(w2v=w2v)
#             allpost, pschema = self.query_post(vocab, vid)
#             similar_post, similar_score = self.cal_similarity(allpost, pschema, scorer=scorer)
#             top_post, top_score, post_ref = self.ranking_post(similar_post, pschema, similar_score, debug=debug)

#             push_pool = self.clean_push(top_post, pschema, top_score)
#             top_push = self.ranking_push(push_pool)
#             push = self.pick_push(top_push)

#         except Exception as err:
#             default_reply = ['嗄', '三小', '滾喇', '嘻嘻']
#             push = default_reply[random.randint(0, len(default_reply) - 1)]
#             self.logger.error(err)
#             self.logger.warning('Query failed: {}'.format(self.query))

#         return push

#     def query_vocab(self, w2v=False):
#         vocab_name = ['--+--'.join([t.word, t.flag, self.default_tokenizer]) for t in self.tok]
#         vocab_score = {name: 1.0 for name in vocab_name}

#         # TODO: merge word2vec model here
#         # ===============================
#         if w2v and bool(RetrievalBase.w2v_model):
#             try:
#                 w2v_query = ['{}:{}'.format(word, flag) for word, flag in zip(self.words, self.flags) if flag[0] in ['v', 'n'] or flag == 'eng']
#                 if bool(w2v_query):
#                     w2v_neighbor = RetrievalBase.w2v_model.most_similar(positive=w2v_query, topn=min(3, len(w2v_query)))

#                     w2v_name = ['--+--'.join('{}:{}'.format(w[0], self.default_tokenizer).split(':')) for w in w2v_neighbor]
#                     w2v_score = [w[1] for w in w2v_neighbor]

#                     for name, score in zip(w2v_name, w2v_score):
#                         vocab_score[name] = score

#                     vocab_name.extend(w2v_name)
#             except:
#                 pass

#         psql = PsqlQuery()
#         qvocab = list(psql.query(self.query_vocab_by_name_sql, (tuple(vocab_name),)))

#         vschema = psql.schema
#         _tag_weight = {
#             q[vschema['tag']]: RetrievalBase.pos_weight[q[vschema['tag']]]['weight']
#             if q[vschema['tag']] in RetrievalBase.pos_weight else 1.0 for q in qvocab
#         }
#         # ===============================
#         vocab = [
#             {
#                 'word': ':'.join([q[vschema['word']], q[vschema['tag']]]),
#                 'termweight': _tag_weight[q[vschema['tag']]] * vocab_score[q[vschema['name']]],
#                 'docfreq': q[vschema['doc_freq']]
#             } for q in qvocab
#         ]

#         vid = [
#             q[vschema['id']]
#             for q in qvocab
#             if not (q[vschema['stopword']]) and q[vschema['doc_freq']] < self.vocab_docfreq_th
#         ]

#         return vocab, vid

#     def query_post(self, vocab, vid):
#         # self.keyword = json.dumps(vocab, indent=4, ensure_ascii=False, sort_keys=True)
#         # self.logger.info(self.keyword)
#         query_pid = list(PsqlQuery().query(
#             self.query_vocab2post_sql, (tuple(vid),))
#         )
#         psql = PsqlQuery()
#         allpost = psql.query(self.query_post_by_id_sql, (tuple(query_pid),))
#         pschema = psql.schema

#         return allpost, pschema

#     def cal_similarity(self, allpost, pschema, scorer=pos_jaccard_similarity):
#         similar_post = []
#         similar_score = []

#         for i, post in enumerate(allpost):
#             if i >= self.max_query_post_num:
#                 break
#             doc = [':'.join([t, g]) for t, g in zip(post[pschema['tokenized']].split(), post[pschema['grammar']].split())]
#             similar_post.append(post)
#             similar_score.append(scorer(self.vocab, doc))

#         return similar_post, similar_score

#     def ranking_post(self, similar_post, pschema, similar_score, debug=False):
#         # TODO: add other feature weighting here
#         # ======================================
#         w_pushcount = 0.2
#         w_pdate = 0.4
#         w_similar = 5.0
#         now = similar_post[0][pschema['publish_date']].timestamp()
#         score = []
#         for i, post in enumerate(similar_post):
#             s = w_pushcount * len(post[pschema['push']].split('\n')) \
#                 + w_pdate * post[pschema['publish_date']].timestamp() / now \
#                 + w_similar * similar_score[i]

#             score.append(s)
#         idx_ranking = np.asarray(score).argsort()[::-1]
#         top_post = []
#         top_score = []
#         max_score = score[idx_ranking[0]]
#         for m, idx in enumerate(idx_ranking):
#             if (score[idx] / max_score) < self.ranking_factor or m > self.max_top_post_num:
#                 break
#             else:
#                 top_post.append(similar_post[idx])
#                 top_score.append(score[idx])

#         post_ref = '\n'
#         # ======================================
#         if debug:
#             ref = []
#             for p, s in zip(top_post, top_score):
#                 ref.append('[{:.2f}]{}\n{}'.format(s, p[pschema['tokenized']], p[pschema['url']]))
#             post_ref = '\n\n'.join(ref)
#             self.logger.info(post_ref)

#         return top_post, top_score, post_ref

#     def clean_push(self, top_post, pschema, post_score):
#         push_pool = []

#         for post, score in zip(top_post, post_score):
#             union_push = {}
#             anony_num = 0
#             for line, mix in enumerate(post[pschema['push']].split('\n')):
#                 idx = mix.find(':')
#                 if idx < 0:
#                     anony_num += 1
#                     name = 'anony@' + str(anony_num)
#                     union_push[name] = {}
#                     union_push[name]['push'] = [{'content': mix.strip(), 'line': line}]
#                 else:
#                     audience, push = mix[:idx].strip(), mix[idx + 1:].strip()

#                     # TODO: add blacklist
#                     # ====================

#                     # ====================

#                     if audience in union_push:
#                         union_push[audience]['push'].append({'content': push, 'line': line})
#                     else:
#                         union_push[audience] = {}
#                         union_push[audience]['push'] = [{'content': push, 'line': line}]

#             for key, allpush in union_push.items():
#                 appendpush = []
#                 line = -10
#                 for p in allpush['push']:

#                     if (p['line'] - line) < 2:
#                         appendpush[-1] += p['content']

#                     else:
#                         appendpush.append(p['content'])

#                     line = p['line']

#                 push_pool.append({'push': appendpush, 'post_score': score})

#         return push_pool

#     def ranking_push(self, push_pool):
#         # TODO: ranking push
#         # ==================
#         idx_weight, len_weight = 2.0, 1.0

#         push = []
#         for pool in push_pool:
#             push.extend(pool['push'])

#         score = []
#         for i, p in enumerate(push):
#             score.append(idx_weight / (1 + i) - len_weight * len(p))

#         idx_ranking = np.asarray(score).argsort()[::-1]

#         top_push = [push[idx] for idx in idx_ranking]

#         return top_push

#     def pick_push(self, top_push, debug=True):
#         push_num = len(top_push)
#         centre = push_num >> 1
#         pick = centre + centre * np.random.normal(0, 1) / 2.0
#         if debug:
#             self.logger.info('len: {}, centre: {}, pick: {}'.format(push_num, centre, pick))
#         final_push = top_push[int(min(push_num - 1, max(0, pick)))]

#         return final_push


class RetrievalBase(PsqlQueryScript):
    max_query_post_num = 20000
    max_vocab_postfreq = 10000

    query_post_by_id_sql = '''
        SELECT * FROM pttcorpus_post WHERE id IN %s ORDER BY publish_date DESC;
    '''

    def __init__(self, words, tokenizer_tag, query=None, logger_name='retrievalbase'):
        self.words = words
        self.query = query
        self.tokenizer_tag = tokenizer_tag
        self.logger = logging.getLogger(logger_name)

    def retrieve(self):
        raise NotImplementedError

    def query_vocab_by_words(self, relative_words=None):
        words = list(self.words)
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


class RetrievalJaccard(RetrievalBase):
    """

    """
    max_top_post_num = 10
    max_top_comment_num = 20
    similarity_ranking_threshold = 0.9

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
                Vocab(
                    cmtvocab_dict[v][vschema['word']],
                    pos=cmtvocab_dict[v][vschema['pos']],
                    tokenizer=cmtvocab_dict[v][vschema['tokenizer']],
                    postfreq=cmtvocab_dict[v][vschema['postfreq']],
                    commentfreq=cmtvocab_dict[v][vschema['commentfreq']],
                    quality=cmtvocab_dict[v][vschema['quality']],
                    stopword=cmtvocab_dict[v][vschema['stopword']]
                )
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
                Vocab(
                    pvocab_dict[v][vschema['word']],
                    pos=pvocab_dict[v][vschema['pos']],
                    tokenizer=pvocab_dict[v][vschema['tokenizer']],
                    postfreq=pvocab_dict[v][vschema['postfreq']],
                    commentfreq=pvocab_dict[v][vschema['commentfreq']],
                    quality=pvocab_dict[v][vschema['quality']],
                    stopword=pvocab_dict[v][vschema['stopword']]
                )
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

    def _get_query_vocab_obj(self):
        qvocab, vschema = self.query_vocab_by_words()
        query_vocabs = [
            Vocab(
                v[vschema['word']],
                pos=v[vschema['pos']],
                tokenizer=v[vschema['tokenizer']],
                postfreq=v[vschema['postfreq']],
                commentfreq=v[vschema['commentfreq']],
                quality=v[vschema['quality']],
                stopword=v[vschema['stopword']]
            )
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

    def retrieve(self):
        tic = time.time()
        query_vocabs, vocab_id = self._get_query_vocab_obj()
        self.logger.info('Elapsed time @query_vocabs: {:.2f}'.format(time.time() - tic))
        tic = time.time()
        query_posts, _ = self._get_post_obj(vocab_id)
        print('@@@', 'query_posts', len(query_posts))
        self.logger.info('Elapsed time @query_posts: {:.2f}'.format(time.time() - tic))
        tic = time.time()
        post_score = [jaccard_similarity(query_vocabs, p.vocabs) for p in query_posts]
        max_post_score = max(post_score)
        self.logger.info('Elapsed time @calculate_post_similarity: {:.2f}'.format(time.time() - tic))

        top_posts, top_post_scores = self._ranking(post_score, query_posts, self.max_top_post_num, self.similarity_ranking_threshold)
        [print('{:.2f}'.format(score), p.body, p.url) for p, score in zip(top_posts, top_post_scores)]
        print('#####')
        post_id = [p.post_id for p in top_posts]
        post_score_dict = {p.post_id: p.similarity_score for p in top_posts}
        tic = time.time()
        query_comments, _ = self._get_comment_obj(post_id)
        self.logger.info('Elapsed time @query_comments: {:.2f}'.format(time.time() - tic))
        print('@@@', 'query_comments', len(query_comments))

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
        # print(docfreq)
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
        print('######')
        [print('{:.2f}'.format(score), cmt.body) for cmt, score in zip(top_comments, top_comment_scores)]
        random.seed(time.time())
        return random.choice(top_comments)


class RetrievalBot(RetrievalBase, PsqlChatCacheScript):
    disclaimer = None
    activate_key = None
    activate_response = []

    repeat_time = 10
    repeat_cold_interval = 60
    repeat_response = []

    longquery_limit = 40
    longquery_response = []

    kickout_key = []
    kickout_response = []

    def __init__(self, query, rule_orm, **kwargs):
        super(RetrievalBot, self).__init__(query, kwargs)

        if bool(rule_orm):
            if not bool(RetrievalBot.disclaimer):
                disclaimer = rule_orm.objects.get(rtype='disclaimer')
                RetrievalBot.disclaimer = disclaimer.response

            if not bool(RetrievalBot.repeat_response):
                repeat = rule_orm.objects.get(rtype='repeat')
                RetrievalBot.repeat_response = [r.strip() for r in repeat.response.split('\n')]
                RetrievalBot.repeat_time = int(repeat.keyword)

            if not(bool(RetrievalBot.kickout_key) and bool(RetrievalBot.kickout_response)):
                kickout = rule_orm.objects.get(rtype='kickout')
                RetrievalBot.kickout_key = [k.strip() for k in kickout.keyword.split(',')]
                RetrievalBot.kickout_response = [r.strip() for r in kickout.response.split('\n')]

            if not (bool(RetrievalBot.activate_key) and bool(RetrievalBot.activate_response)):
                activate = rule_orm.objects.get(rtype='activate')
                RetrievalBot.activate_key = [k.strip() for k in activate.keyword.split(',')]
                RetrievalBot.activate_response = [r.strip() for r in activate.response.split('\n')]

            if not bool(RetrievalBot.longquery_response):
                longquery = rule_orm.objects.get(rtype='longquery')
                RetrievalBot.longquery_limit = int(longquery.keyword)
                RetrievalBot.longquery_response = [r.strip() for r in longquery.response.split('\n')]
