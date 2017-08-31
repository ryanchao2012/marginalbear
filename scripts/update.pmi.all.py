from configparser import RawConfigParser
from core.utils import PsqlAbstract, PsqlQuery

from core.ingest import PsqlIngester
import time
import logging


config_parser = RawConfigParser()
config_parser.read('../config.ini')

PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)

ingester = PsqlIngester('jieba')

logger = logging.getLogger('update.pmi')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s [%(levelname)s] @%(filename)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setFormatter(chformatter)
logger.addHandler(ch)


def query_vocab_id(batch_size=1000):
    sql = 'SELECT id FROM pttcorpus_vocabulary;'
    psql = PsqlQuery()
    vocabs = psql.query(sql)
    batch = []
    i = 0
    for v in vocabs:
        batch.append(v[0])
        i += 1
        if i > batch_size:
            i = 0
            yield batch
            batch = []
    yield batch


if __name__ == '__main__':
    start = time.time()
    ingester = PsqlIngester('jieba')

    consumed = 0
    # for post_ids in [[1, 2, 3]]:
    for post_ids in query_vocab_id(batch_size=10):
        if len(post_ids) > 0:
            try:
                ingester.upsert_association(post_ids, 10000)
            except Exception as err:
                logger.error(post_ids)
                raise err

            consumed += len(post_ids)
            logger.info('{} vocab\'s pmi are updated'.format(consumed))
    print('Elapsed time @update_pmi: {:.2f}sec.'.format(time.time() - start))
