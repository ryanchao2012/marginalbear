import random
import logging
from core.utils import (
    OkLogger,
    PsqlAbstract,
    PsqlQuery,
    QBag, Query,
    to_halfwidth,
    to_lower,
    Title, Post, Vocab, Comment
)
from core.chat import RetrievalEvaluate
from core.pipelines import OkPipeline
from core.tokenizer import (
    SplitTokenizer,
    JiebaPosWeight
)
from configparser import RawConfigParser
from core.metrics import MetricApiWrapper

config_parser = RawConfigParser()
config_parser.read('../config.ini')

PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)

oklogger = OkLogger('evaluate', level=logging.INFO)
oklogger2 = OkLogger('retrieve', level=logging.WARNING)


file_out = 'eval0909.csv'


query_post_sql = '''
    SELECT * FROM pttcorpus_post;
'''
query_title_sql = '''
    SELECT * FROM pttcorpus_title WHERE tokenizer=%(tok)s;
'''

query_random_post_sql = '''
    SELECT id FROM pttcorpus_post WHERE tokenizer=%(tok)s TABLESAMPLE SYSTEM(1);
'''


draw_random_title_sql = '''
    SELECT * FROM pttcorpus_title WHERE random() < 0.01 LIMIT 1;
'''


def draw_title():
    psql = PsqlQuery()
    title, schema = psql.query_all(draw_random_title_sql)

    return title[0], schema


def generate_random_post(ref):
    psql = PsqlQuery()
    post = psql.query(query_random_post_sql, {'tok': 'ccjieba'})

    return [p[0] for p in post][:len(ref)]


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
    with open(file_out, 'w') as f:
        f.write('random, base, pweight\n')
    psql = PsqlQuery()
    posts = psql.query(query_post_sql)
    pschema = psql.schema

    valid_post = 0

    for _ in range(5):

        title, tschema = draw_title()

        # titles, tschema = psql.query_all(
        #     query_title_sql, dict(pid=p[pschema['id']], tok='jieba')
        # )

        basic_retriever = RetrievalEvaluate(
            'ccjieba',
            excluded_title_ids=[title[tschema['id']]],
            logger_name='retrieve'
        )

        pweight_retriever = RetrievalEvaluate(
            'ccjieba',
            excluded_title_ids=[title[tschema['id']]],
            pweight=JiebaPosWeight.weight,
            logger_name='retrieve'
        )

        query = ' '.join(
            [
                '{}:{}'.format(w, p)
                for w, p in zip(
                    title[tschema['tokenized']].split(),
                    title[tschema['grammar']].split()
                )
            ]
        )

        qbag = QBag(Query(query))
        pipe = OkPipeline(
            qbag, ['query.query'],
            [
                ((['query.query'], ['query.topic_words']), SplitTokenizer(),),
                ((['query.topic_words'], ['top_titles_basic']), basic_retriever.get_top_titles,),
                ((['top_titles_basic'], ['top_comments_basic']), basic_retriever.get_top_comments,),
                ((['top_comments_basic'], ['predict_words_ls_basic']), extract_words,),

                ((['query.topic_words'], ['top_titles_pweight']), pweight_retriever.get_top_titles,),
                ((['top_titles_pweight'], ['top_comments_pweight']), pweight_retriever.get_top_comments,),
                ((['top_comments_pweight'], ['predict_words_ls_pweight']), extract_words,),

                ((['top_titles_basic'], ['random_post_id']), generate_random_post,),
                ((['random_post_id'], ['random_comments']), basic_retriever.get_comment_obj),
                ((['random_comments'], ['random_comments']), shuffle_comments),
                ((['random_comments'], ['random_words_ls']), extract_words,),

                ((['query.topic_words', 'predict_words_ls_basic'], ['ndcg_score_basic']), MetricApiWrapper('http://localhost:1234/doc2vec/'),),
                ((['query.topic_words', 'predict_words_ls_pweight'], ['ndcg_score_pweight']), MetricApiWrapper('http://localhost:1234/doc2vec/'),),
                ((['query.topic_words', 'random_words_ls'], ['rand_ndcg_score']), MetricApiWrapper('http://localhost:1234/doc2vec/'),),
            ],
            logger_name='evaluate'
        )
        pipe.run()
        with open(file_out, 'a') as f:
            f.write(
                '{:.2f}, {:.2f}, {:.2f}\n'.format(
                    float(qbag.rand_ndcg_score.text),
                    float(qbag.ndcg_score_basic.text),
                    float(qbag.ndcg_score_pweight.text)
                )
            )
        # if idx > 5:
        #     break

