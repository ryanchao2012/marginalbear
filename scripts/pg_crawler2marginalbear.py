from configparser import RawConfigParser
from core.utils import (
    PsqlAbstract,
    PsqlQuery,
    OkLogger
)
from datetime import datetime
import time
import json
import logging


config_parser = RawConfigParser()
config_parser.read('../config.ini')

PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)


oklogger = OkLogger('crawlparser')


upsert_post_sql = '''
    INSERT INTO pttcrawler_post(url, author,
                                title, content,
                                comment, publish_date)
        SELECT unnest( %(url)s ), unnest( %(author)s ),
               unnest( %(title)s ), unnest( %(content)s ),
               unnest( %(comment)s ), unnest( %(publish_date)s )
        ON CONFLICT (url) DO
        UPDATE SET
            author = EXCLUDED.author,
        RETURNING id;
'''


def upsert_post(batch_post):
    title = [post['title'] for post in batch_post]
    url = [post['url'] for post in batch_post]
    author = [post['author'] for post in batch_post]
    content = [post['content'] for post in batch_post]
    comment = [post['comment'] for post in batch_post]
    publish_date = [post['date'] for post in batch_post]

    post_id = []
    try:
        psql = PsqlQuery()
        post_id = psql.upsert(upsert_post_sql, locals())
    except Exception as e:
        oklogger.logger.error(e)
        raise e

    return post_id


class BatchParser(object):
    def __init__(self, fpath, logger_name='crawlparser'):
        self.fpath = fpath
        self.logger = logging.getLogger(logger_name)

    def batch_parse(self, batch_size=1000):
        with open(self.fpath, 'r') as f:
            batch = []
            i = 0
            for line in f:
                parsed = self.parse(line)
                if bool(parsed):
                    batch.append(parsed)
                    i += 1
                    if i >= batch_size:
                        i = 0
                        yield batch
                        batch = []
            yield batch

    def parse(self, line):
        raise NotImplementedError


class CrawlPostParser(BatchParser):
    fields = [
        'author',
        'date',
        'title',
        'content',
        'comment',
        'url',
    ]

    def parse(self, line):
        try:
            post = json.loads(line)
            post['date'] = datetime.strptime(
                post['publish_date'], '%Y-%m-%dT%H:%M:%S+00:00'
            )
            return post
        except Exception as err:
            self.logger.warning(err)
            self.logger.warning(
                'object CrawlPostParser, '
                'jsonline record faild to parse in, ignored. line: {}'.format(
                    line.encode('utf-8').decode('unicode-escape')
                )
            )
            return {}

if __name__ == '__main__':
    start = time.time()
    post_parser = CrawlPostParser(
        '/var/local/marginalbear/data/okbot.spider.20170419_to_20170903.jsonline'
    )

    consumed = 0
    for batch_post in post_parser.batch_parse(batch_size=1000):
        if len(batch_post) > 0:
            upsert_post(batch_post)
            consumed += len(batch_post)
            oklogger.logger.info(
                '{} lines are ingested from {}'.format(
                    consumed,
                    post_parser.fpath
                )
            )
        break

    print('Elapsed time @post: {:.2f}sec.'.format(time.time() - start))
