from core.utils import (
    PsqlAbstract,
    QBag, Query,
    to_halfwidth,
    to_lower,
)
from core.chat import RetrievalJaccard
from core.pipelines import OkPipeline
from core.tokenizer import JiebaTokenizer
from configparser import RawConfigParser


config_parser = RawConfigParser()
config_parser.read('../config.ini')

PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)


if __name__ == '__main__':
    q = QBag(Query('放假肥宅都在做什麼？'))
    p = OkPipeline(
        q, ['query.query'],
        [
            ((['query.query'],), to_halfwidth,),
            (to_lower,),
            (JiebaTokenizer(),),
            ((['query.query__to_halfwidth__to_lower__jiebatokenizer'], ['comment']), RetrievalJaccard('jieba'),),
        ]
    )

    result = p.run()
    print(result.body)
    print(q.comment.body)
