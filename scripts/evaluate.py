import random
import logging
from core.utils import (
    OkLogger,
    PsqlAbstract,
    PsqlQuery,
    QBag, Query,
    to_halfwidth,
    to_lower,
)
from core.chat import RetrievalEvaluate
from core.pipelines import OkPipeline
from core.tokenizer import SplitTokenizer
from configparser import RawConfigParser
from core.metrics import MetricApiWrapper

config_parser = RawConfigParser()
config_parser.read('../config.ini')

PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)

oklogger = OkLogger('evaluate', level=logging.WARNING)


query_post_sql = '''
    SELECT * FROM pttcorpus_post;
'''
query_title_sql = '''
    SELECT * FROM pttcorpus_title WHERE post_id=%(pid)s AND tokenizer=%(tok)s;
'''

query_random_post_sql = '''
    SELECT id FROM pttcorpus_post TABLESAMPLE SYSTEM(1);
'''

def generate_random_post(ref):
    psql = PsqlQuery()

    posts = psql.query(query_random_post_sql)

    return [p[0] for p in posts][:len(ref)]

def shuffle_comments(comments):
    random.shuffle(comments)
    return comments



def extract_words(comments):
    if not bool(comments):
        return []
    def extract(cmt):
        return [v for v in cmt.vocabs]

    return [extract(cmt) for cmt in comments]

if __name__ == '__main__':

    psql = PsqlQuery()
    posts = psql.query(query_post_sql)
    pschema = psql.schema

    valid_post = 0

    for idx, p in enumerate(posts):
        titles, tschema = psql.query_all(
            query_title_sql, dict(pid=p[pschema['id']], tok='jieba')
        )
        oklogger.logger.warning(p[pschema['title_raw']])
#        retriever = RetrievalEvaluate('jieba', logger_name='evaluate')
        retriever = RetrievalEvaluate('jieba', excluded_post_ids=[p[pschema['id']]],  logger_name='evaluate')
        query = ' '.join(['{}:{}'.format(w, p) for w, p in zip(titles[0][tschema['tokenized']].split(), titles[0][tschema['grammar']].split()) ])

        q = QBag(Query(query))
        p = OkPipeline(
            q, ['query.query'],
            [
                ((['query.query'], ['query.topic_words']), SplitTokenizer(),),
                ((['query.topic_words'], ['top_posts']), retriever.get_top_posts,),
                ((['top_posts'], ['top_comments']), retriever.get_top_comments,),
                ((['top_posts'], ['random_post_id']), generate_random_post,),
                ((['random_post_id'], ['random_comments', 'random_comment_ids']), retriever.get_comment_obj),
#                ((['random_comments'], ['random_comments']), shuffle_comments),
#                ((['random_comments'], ['random_words_ls']), extract_words,),
#                ((['top_comments'], ['predict_words_ls']), extract_words,),
#                ((['query.topic_words', 'predict_words_ls'], ['ndcg_score']), MetricApiWrapper('http://localhost:1234/doc2vec/'),),
#                ((['query.topic_words', 'random_words_ls'], ['rand_ndcg_score']), MetricApiWrapper('http://localhost:1234/doc2vec/'),),
            ],
            logger_name='evaluate'
        )
        result = p.run()

        if len(q.top_posts) > 0:
            valid_post += 1

    # print('\n'.join([cmt.body for cmt in q.ranked_comments]))
    print(valid_post)
