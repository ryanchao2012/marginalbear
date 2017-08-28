from core.utils import (
    PsqlAbstract,
    PsqlQuery,
    QBag, Query,
    to_halfwidth,
    to_lower,
)
from core.chat import RetrievalJaccard
from core.pipelines import OkPipeline
from core.tokenizer import SplitTokenizer
from configparser import RawConfigParser


config_parser = RawConfigParser()
config_parser.read('../config.ini')

PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)

query_post_sql = '''
    SELECT * FROM pttcorpus_post;
'''
query_title_sql = '''
    SELECT * FROM pttcorpus_title WHERE post_id=%(pid)s AND tokenizer=%(tok)s;
'''


def extract_words(comments):

    def extract(cmt):
        return [v.word for v in cmt.vocabs]

    return [extract(cmt) for cmt in comments]

if __name__ == '__main__':

    psql = PsqlQuery()
    posts = psql.query(query_post_sql)
    pschema = psql.schema

    for p in posts:
        fake_query = p[pschema['title_cleaned']]
        titles, tschema = psql.query_all(
            query_title_sql, dict(pid=p[pschema['id']], tok='jieba')
        )
        q = QBag(Query(titles[0][tschema['tokenized']]))
        p = OkPipeline(
            q, ['query.query'],
            [
                ((['query.query'], ['query.topic_words']), SplitTokenizer(),),
                ((['query.topic_words'], ['ranked_comments']), RetrievalJaccard('jieba'),),
                ((['ranked_comments'], ['predict_words_ls']), extract_words,),
                ((['query.topic_words', 'predict_words_ls'], ['ndcg_score']), MetricApiWrapper('http://localhost:1234/doc2vec'),),
            ]
        )
        result = p.run()
        break
    print(result)
