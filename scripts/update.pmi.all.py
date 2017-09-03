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


def query_freq_sum():

    query_freq_sum_sql = '''
        SELECT SUM(postfreq) AS postfreq_sum,
               SUM(commentfreq) AS commentfreq_sum
        FROM pttcorpus_vocabulary;
    '''

    query_vocab_pairfreq_sum_sql = '''
        SELECT SUM(pxy) AS sum
        FROM pttcorpus_association;
    '''

    psql = PsqlQuery()
    postfreq_sum, commentfreq_sum = list(psql.query(query_freq_sum_sql))[0]
    logger.info('postfreq_sum:{}, commentfreq_sum:{}'.format(postfreq_sum, commentfreq_sum))
    vocab_pairfreq_sum = list(psql.query(query_vocab_pairfreq_sum_sql))[0][0]
    logger.info('vocab_pairfreq_sum:{}'.format(vocab_pairfreq_sum))

    return postfreq_sum, commentfreq_sum, vocab_pairfreq_sum


if __name__ == '__main__':
    start = time.time()
    ingester = PsqlIngester('jieba')

    consumed = 0
    for vocab_ids in query_vocab_id(batch_size=10):
        if len(vocab_ids) > 0:
            try:
                ingester.upsert_vocab_pairfreq(vocab_ids, 1000)
            except Exception as err:
                logger.error(vocab_ids)
                raise err
            consumed += len(vocab_ids)
            logger.info('{} vocab\'s vocab_pairfreq are updated'.format(consumed))
    print('Elapsed time @update_vocab_pairfreq: {:.2f}sec.'.format(time.time() - start))

    postfreq_sum, commentfreq_sum, vocab_pairfreq_sum = query_freq_sum()
    consumed = 0
    for vocab_ids in query_vocab_id(batch_size=50):
        start = time.time()
        if len(vocab_ids) > 0:
            try:
                ingester.upsert_association(postfreq_sum, commentfreq_sum, vocab_pairfreq_sum, vocab_ids, batch_size=1000)
            except Exception as err:
                logger.error(vocab_ids)
                raise err

            consumed += len(vocab_ids)
            logger.info('{} vocab\'s association are updated in {}s'.format(consumed, time.time() - start))
    print('Elapsed time @update_association: {:.2f}sec.'.format(time.time() - start))
