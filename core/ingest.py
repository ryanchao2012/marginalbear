import logging
import collections
from datetime import datetime
from core.utils import (
    PsqlQuery, PsqlQueryScript,
    LengthNotMatchException
)

logger = logging.getLogger('psql_ingester')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s [%(levelname)s] @%(filename)s.%(funcName)s : %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setFormatter(chformatter)
logger.addHandler(ch)


class PsqlIngestScript(PsqlQueryScript):

    upsert_post_sql = '''
            INSERT INTO pttcorpus_post(title_raw, title_cleaned,
                                       comment_raw, comment_cleaned,
                                       tag, spider, url,
                                       author_id, quality,
                                       publish_date, last_update,
                                       update_count, allow_update)
            SELECT unnest( %(title_raw)s ), unnest( %(title_cleaned)s ),
                   unnest( %(comment_raw)s ), unnest( %(comment_cleaned)s ),
                   unnest( %(tag)s ), unnest( %(spider)s ), unnest( %(url)s ),
                   unnest( %(author)s ), unnest( %(quality)s ),
                   unnest( %(publish_date)s ), unnest( %(last_update)s ),
                   unnest( %(update_count)s ), unnest( %(allow_update)s )
            ON CONFLICT (url) DO
            UPDATE SET
                last_update = EXCLUDED.last_update,
                allow_update = EXCLUDED.allow_update,
                update_count = pttcorpus_post.update_count + 1
            WHERE pttcorpus_post.allow_update = True
            RETURNING id;
    '''

    insert_vocab_sql = '''
            INSERT INTO pttcorpus_vocabulary(word, tokenizer, pos,
                                             quality,
                                             postfreq, commentfreq, stopword)
            SELECT unnest( %(word)s ), unnest( %(tokenizer)s ), unnest( %(pos)s ),
                   unnest( %(quality)s ),
                   unnest( %(postfreq)s ), unnest( %(commentfreq)s ), unnest( %(stopword)s )
            ON CONFLICT (word, tokenizer, pos) DO
            UPDATE SET
                stopword=pttcorpus_vocabulary.stopword
            RETURNING id, word, pos, tokenizer;
    '''

    upsert_vocab2post_sql = '''
            INSERT INTO pttcorpus_vocabulary_post (vocabulary_id, post_id)
            SELECT unnest( %(vocabulary_id)s ), unnest( %(post_id)s )
            ON CONFLICT (vocabulary_id, post_id) DO
            UPDATE SET
                post_id = pttcorpus_vocabulary_post.post_id
            RETURNING vocabulary_id;
    '''

    upsert_vocab2comment_sql = '''
            INSERT INTO pttcorpus_vocabulary_comment (vocabulary_id, comment_id)
            SELECT unnest( %(vocabulary_id)s ), unnest( %(comment_id)s )
            ON CONFLICT (vocabulary_id, comment_id) DO
            UPDATE SET
                comment_id = pttcorpus_vocabulary_comment.comment_id
            RETURNING vocabulary_id;
    '''

    update_vocab_postfreq_sql = '''
            UPDATE pttcorpus_vocabulary AS old
            SET postfreq = new.postfreq
            FROM (SELECT unnest( %(id_)s ) as id,
                         unnest( %(postfreq)s ) as postfreq) as new
            WHERE old.id = new.id;
    '''

    update_vocab_commentfreq_sql = '''
            UPDATE pttcorpus_vocabulary AS old
            SET commentfreq = new.commentfreq
            FROM (SELECT unnest( %(id_)s ) as id,
                         unnest( %(commentfreq)s ) as commentfreq) as new
            WHERE old.id = new.id;
    '''

    insert_netizen_sql = '''
        INSERT INTO pttcorpus_netizen(name, quality, posts, comments)
        SELECT unnest( %(name)s ), unnest( %(quality)s ),
               unnest( %(posts)s ), unnest( %(comments)s )
        ON CONFLICT(name) DO
        UPDATE SET
            quality = pttcorpus_netizen.quality
        RETURNING id;
    '''

    insert_title_sql = '''
            INSERT INTO pttcorpus_title(ctype, tokenizer,
                                   tokenized, grammar,
                                   quality,
                                   retrieval_count, post_id)
            SELECT unnest( %(ctype)s ), unnest( %(tokenizer)s ),
                   unnest( %(tokenized)s ), unnest( %(grammar)s ),
                   unnest( %(quality)s ),
                   unnest( %(retrieval_count)s ), unnest( %(post_id)s )
            ON CONFLICT (post_id, tokenizer) DO
            UPDATE SET
                retrieval_count = pttcorpus_title.retrieval_count
            RETURNING id;
    '''

    insert_comment_sql = '''
            INSERT INTO pttcorpus_comment(ctype, tokenizer,
                                   tokenized, grammar,
                                   floor, quality,
                                   retrieval_count, audience_id, post_id)
            SELECT unnest( %(ctype)s ), unnest( %(tokenizer)s ),
                   unnest( %(tokenized)s ), unnest( %(grammar)s ),
                   unnest( %(floor)s ), unnest( %(quality)s ),
                   unnest( %(retrieval_count)s ),
                   unnest( %(audience)s ), unnest( %(post_id)s )
            ON CONFLICT (post_id, floor, tokenizer) DO
            UPDATE SET
                retrieval_count = pttcorpus_comment.retrieval_count
            RETURNING id;
    '''


class PsqlIngester(PsqlIngestScript):
    """Ingester for postgresql."""

    """
        Attributes:
            str:tokenizer
        Methods:
            query_post(post_url) -> post_id

            insert_vocab_ignore_docfreq(batch, tokenized_field) -> vocab_bundle, vschema
            upsert_post(batch_post, fields=...) -> post_id
            insert_title(batch, post_url, tokenized_field, type_field) -> title_id
            insert_comment(comments, fields=...) -> cmt_id

            upsert_vocab2post(batch_post, vocab_bundle, post_url, tokenized_field) -> vocab_id
            upsert_vocab2comment(batch_comment, vocab_bundle, cmt_bundle, tokenized_field) -> vocab_id

            update_vocab_postfreq(vocab_bundle)
            update_vocab_commentfreq(vocab_id)
    """

    def __init__(self, tokenizer, logger_name='psql_ingester'):
        self.tokenizer = tokenizer
        self.logger = logging.getLogger(logger_name)

    def _query_all(self, sql_string, data=None):
        psql = PsqlQuery()
        fetched, schema = psql.query_all(sql_string, data)
        return fetched, schema

    def query_post(self, url):
        qpost, schema = self._query_all(self.query_post_by_url_sql, (tuple(url),))
        aligned_post = [None] * len(url)
        for p in qpost:
            idx = url.index(p[schema['url']])
            aligned_post[idx] = p
        return aligned_post, schema

    def insert_vocab_ignore_docfreq(self, batch, tokenized_field='title_tokenized'):
        allpairs = [pair for body in batch for pair in body[tokenized_field]]

        distinct = list({(pair['word'], pair['pos']) for pair in allpairs})
        num = len(distinct)
        word = [d[0] for d in distinct]
        pos = [d[1] for d in distinct]
        tokenizer = [self.tokenizer for _ in range(num)]
        quality = [0.0 for _ in range(num)]
        postfreq = [-1 for _ in range(num)]
        commentfreq = [-1 for _ in range(num)]
        stopword = [False for _ in range(num)]
        psql = PsqlQuery()
        vocab_bundle = psql.upsert(self.insert_vocab_sql, locals())
        returned_schema = dict(id=0, word=1, pos=2, tokenizer=3)
        return vocab_bundle, returned_schema

    def upsert_vocab2post(self, batch_post, post_id,
                          vocab_bundle, vschema,
                          tokenized_field='title_tokenized'):
        tokenized = [[(k['word'], k['pos'], self.tokenizer) for k in p[tokenized_field]] for p in batch_post]

        vocab2post = []
        for vocab in vocab_bundle:
            vtubple = (vocab[vschema['word']], vocab[vschema['pos']], vocab[vschema['tokenizer']])
            post_id_with_vocab = [
                p
                for idx, p in enumerate(post_id)
                if vtubple in tokenized[idx]
            ]
            vocab2post.append([(vocab[vschema['id']], pid) for pid in post_id_with_vocab])

        flatten_vocab2post = [tup for v2p in vocab2post for tup in v2p]

        vocabulary_id = [v2p[0] for v2p in flatten_vocab2post]
        flatten_post_id = [v2p[1] for v2p in flatten_vocab2post]

        psql = PsqlQuery()
        psql.upsert(self.upsert_vocab2post_sql, {'vocabulary_id': vocabulary_id, 'post_id': flatten_post_id})

        return vocabulary_id

    def upsert_vocab2comment(self, batch_comment, comment_id,
                             vocab_bundle, vschema,
                             tokenized_field='comment_tokenized'):

        tokenized = [[(k['word'], k['pos'], self.tokenizer) for k in p[tokenized_field]] for p in batch_comment]
        vocab2comment = []

        for vocab in vocab_bundle:
            vtuple = (vocab[vschema['word']], vocab[vschema['pos']], vocab[vschema['tokenizer']])
            comment_id_with_vocab = [cmt for idx, cmt in enumerate(comment_id) if vtuple in tokenized[idx]]
            vocab2comment.append([(vocab[vschema['id']], cid) for cid in comment_id_with_vocab])

        flatten_vocab2cmt = [tup for v2c in vocab2comment for tup in v2c]

        vocabulary_id = [v2c[0] for v2c in flatten_vocab2cmt]
        cmt_id = [v2c[1] for v2c in flatten_vocab2cmt]

        psql = PsqlQuery()
        psql.upsert(self.upsert_vocab2comment_sql, {'vocabulary_id': vocabulary_id, 'comment_id': cmt_id})

        return vocabulary_id

    def update_vocab_postfreq(self, vocab_id):
        qvocab2post, schema = self._query_all(
            self.query_vocab2post_by_vid_sql,
            (tuple(vocab_id),)
        )
        qvocab_id = [v2p[schema['vocabulary_id']] for v2p in qvocab2post]
        vocab_cnt = collections.Counter(qvocab_id)
        psql = PsqlQuery()
        psql.upsert(
            self.update_vocab_postfreq_sql,
            {'id_': list(vocab_cnt.keys()), 'postfreq': list(vocab_cnt.values())}
        )

    def update_vocab_commentfreq(self, vocab_id):
        qvocab2comment, schema = self._query_all(
            self.query_vocab2comment_by_vid_sql,
            (tuple(vocab_id),)
        )
        qvocab_id = [v2c[schema['vocabulary_id']] for v2c in qvocab2comment]
        vocab_cnt = collections.Counter(qvocab_id)
        psql = PsqlQuery()
        psql.upsert(
            self.update_vocab_commentfreq_sql,
            {'id_': list(vocab_cnt.keys()), 'commentfreq': list(vocab_cnt.values())}
        )

    def insert_netizen(self, raw_name):
        name = list(set(raw_name))
        num = len(name)
        quality = [0.0 for _ in range(num)]
        posts = [0 for _ in range(num)]
        comments = [0 for _ in range(num)]
        psql = PsqlQuery()
        ids = psql.upsert(self.insert_netizen_sql, locals())
        return [i[0] for i in ids]

    def upsert_post(self, batch_post,
                    title_raw_field='title_raw', title_cleaned_field='title_cleaned',
                    comment_raw_field='comment_raw', comment_cleaned_field='comment_cleaned',
                    tag_field='tag', url_field='url', spider_field='spider',
                    author_field='author', publish_date_field='date'):

        post_num = len(batch_post)

        title_raw = [p[title_raw_field] for p in batch_post]
        title_cleaned = [p[title_cleaned_field] for p in batch_post]
        comment_raw = [p[comment_raw_field] for p in batch_post]
        comment_cleaned = [p[comment_cleaned_field] for p in batch_post]
        url = [p[url_field] for p in batch_post]
        if len(url) != len(set(url)):
            raise LengthNotMatchException

        tag = [p[tag_field] for p in batch_post]
        author = [
            p[author_field][:p[author_field].find('(')].strip()
            for p in batch_post
        ]
        self.insert_netizen(author)
        publish_date = [p[publish_date_field] for p in batch_post]
        spider = [p[spider_field] for p in batch_post]
        last_update = [datetime.now()] * post_num
        quality = [0.0 for _ in range(post_num)]
        update_count = [1] * post_num
        allow_update = [True] * post_num

        # qpost, schema = self.query_post(url)
        # for i, q in enumerate(qpost):
        #     if q:
        #         if len(q[schema['push']]) == len(push[i]):
        #             allow_update[i] = False
        post_id = []
        try:
            psql = PsqlQuery()
            post_id = psql.upsert(self.upsert_post_sql, locals())
        except Exception as e:
            self.logger.error(e)
            raise e

        return [p[0] for p in post_id]

    def insert_title(self, batch_post, post_id,
                     tokenized_field='title_tokenized', type_field='ctype'):

        num = len(batch_post)
        # qpost, pschema = self.query_post(post_url)
        tokenized = [' '.join([k['word'] for k in p[tokenized_field]]) for p in batch_post]
        grammar = [' '.join([k['pos'] for k in p[tokenized_field]]) for p in batch_post]
        # post_id = [p[pschema['id']] for p in post_bundle]
        ctype = [p[type_field] for p in batch_post]
        tokenizer = [self.tokenizer] * num
        retrieval_count = [0] * num
        quality = [0.0 for _ in range(num)]

        psql = PsqlQuery()
        title_id = psql.upsert(self.insert_title_sql, locals())

        return [t[0] for t in title_id]

    def insert_comment(self, comments,
                       batch_field='comments', url_field='url',
                       tokenized_field='comment_tokenized', type_field='ctype',
                       floor_field='floor', audience_field='audience'):

        batch_comment = []
        for batch in comments:
            batch_comment.extend(batch[batch_field])

        post_url = [batch['url'] for batch in comments]
        if len(post_url) != len(set(post_url)):
            raise LengthNotMatchException

        num = len(batch_comment)
        qpost, pschema = self.query_post(post_url)

        tokenized = [' '.join([k['word'] for k in cmt[tokenized_field]]) for cmt in batch_comment]
        grammar = [' '.join([k['pos'] for k in cmt[tokenized_field]]) for cmt in batch_comment]

        ctype = [cmt[type_field] for cmt in batch_comment]
        floor = [cmt[floor_field] for cmt in batch_comment]

        audience = [cmt[audience_field] for cmt in batch_comment]
        self.insert_netizen(audience)

        tokenizer = [self.tokenizer] * num
        retrieval_count = [0] * num
        quality = [0.0 for _ in range(num)]

        post_id = []
        try:
            for idx, (batch, p) in enumerate(zip(comments, qpost)):
                post_id.extend([p[pschema['id']]] * len(batch[batch_field]))
        except Exception as err:
            self.logger.error('It\'s impossible to insert Comments while Post doesn\'t exist. url: {}'.format(post_url[idx]))
            raise err

        psql = PsqlQuery()
        comment_id = psql.upsert(self.insert_comment_sql, locals())

        return [cmt[0] for cmt in comment_id], batch_comment
