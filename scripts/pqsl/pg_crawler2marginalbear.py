from init import (
    dbuser, dbname, dbpassword,
    line_channel_secret
)

import re
import numpy as np
from marginalbear.core.utils import (
    PsqlAbstract,
    PsqlQuery,
    OkLogger
)
from datetime import datetime
import time
import json
import logging


PsqlAbstract.set_database_info(
    dbuser, dbname, dbpassword
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
            author = EXCLUDED.author
        RETURNING id;
'''


def upsert_post(batch_post):
    timestamp = [post['timestamp'] for post in batch_post]
    _title = [post['title'] for post in batch_post]
    _url = [post['url'] for post in batch_post]
    _author = [post['author'] for post in batch_post]
    _content = [post['content'] for post in batch_post]
    _comment = [post['comment'] for post in batch_post]
    comment_len = [len(cmt) for cmt in _comment]
    sorted_idx = np.argsort(comment_len)[::-1]
    title, url, author, content, comment, publish_date = [], [], [], [], [], []

    for idx in sorted_idx:
        if _url[idx] not in url:
            url.append(_url[idx])
            title.append(_title[idx])
            author.append(_author[idx])
            content.append(_content[idx])
            comment.append(_comment[idx])
            publish_date.append(datetime.fromtimestamp(timestamp[idx]))

    post_id = []
    try:
        psql = PsqlQuery()
        post_id = psql.upsert(upsert_post_sql, locals())
    except Exception as e:
        oklogger.logger.error(e)
        oklogger.logger.error(title)
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
        'timestamp',
        'title',
        'content',
        'comment',
        'url',
    ]

    def parse(self, line):
        try:
            post = json.loads(line)
            url = post['url']
            timestamp = re.search(r'\d{10}', url).group()
            post['timestamp'] = int(timestamp)
            post['comment'] = '\n'.join(post['push'])
            return post
        except Exception as err:
            self.logger.warning(err)
            self.logger.warning(
                'object CrawlPostParser, '
                'jsonline record faild to parse in, ignored. line: {}'.format(
                    line
                )
            )
            return {}

# if __name__ == '__main__':
#     start = time.time()
#     post_parser = CrawlPostParser(
#         '/var/local/marginalbear/data/okbot.spider.20170903_to_20170419.reverse.jsonline'
#     )
# 
#     consumed = 0
#     posts = 0
#     for batch_post in post_parser.batch_parse(batch_size=1000):
#         if len(batch_post) > 0:
#             post_id = upsert_post(batch_post)
#             consumed += len(batch_post)
#             posts += len(post_id)
#             oklogger.logger.info(
#                 '{} lines are ingested, posts: {}'.format(
#                     consumed, posts
#                 )
#             )
# 
#     print('Elapsed time @post: {:.2f}sec.'.format(time.time() - start))
