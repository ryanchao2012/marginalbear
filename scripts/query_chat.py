from configparser import RawConfigParser
import sys
from core.utils import PsqlAbstract
from core.chat import RetrievalJaccard
from core.tokenizer import JiebaTokenizer


config_parser = RawConfigParser()
config_parser.read('../config.ini')

PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(
            '''
            usage: python query_chat <query-string>
            ex: python query_chat 安安幾歲住哪
            '''
        )
        sys.exit(0)
    query = sys.argv[1]

    words = JiebaTokenizer().cut(query)

    comment = RetrievalJaccard(words, 'jieba', query=query).retrieve()
    response = comment.body

    print('\n@', response)
