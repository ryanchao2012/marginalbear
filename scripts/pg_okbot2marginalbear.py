from configparser import RawConfigParser
from core.utils import PsqlAbstract

from core.ingest import PsqlIngester
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


ingester = PsqlIngester('corenlp')


logger = logging.getLogger('okgotparser')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
chformatter = logging.Formatter('%(asctime)s [%(levelname)s] @%(filename)s: %(message)s', datefmt='[%d/%b/%Y %H:%M:%S]')
ch.setFormatter(chformatter)
logger.addHandler(ch)


class BatchParser(object):
    def __init__(self, fpath, logger_name='okgot_parser'):
        self.fpath = fpath
        self.logger = logging.getLogger(logger_name)

    def batch_parse(self, batch_size=100):
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


class OkbotPostParser(BatchParser):
    fields = [
        'author',
        'batch_comment',
        'comment_cleaned',
        'comment_raw',
        'ctype',
        'date',
        'spider',
        'tag',
        'title_cleaned',
        'title_raw',
        'title_tokenized',
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
                'object OkbotPostParser, '
                'jsonline record faild to parse in, ignored. line: {}'.format(
                    line.encode('utf-8').decode('unicode-escape')
                )
            )
            return {}


class OkbotCommentParser(BatchParser):
    fields = ['url', 'comments']

    def parse(self, line):
        try:
            comment = json.loads(line)
            return comment
        except Exception as err:
            self.logger.warning(err)
            self.logger.warning(
                'object OkbotCommentParser, '
                'jsonline record faild to parse in, ignored. line: {}'.format(
                    line.encode('utf-8').decode('unicode-escape')
                )
            )
            return {}


if __name__ == '__main__':
    start = time.time()
    post_parser = OkbotPostParser('tokenize_by_corenlp/formatted.post.corenlp.jl')
    comment_parser = OkbotCommentParser('tokenize_by_corenlp/formatted.comment.corenlp.jl')

    consumed = 0
    for batch_post in post_parser.batch_parse(batch_size=100):
        if len(batch_post) > 0:
            post_id = ingester.upsert_post(batch_post)
            vocab_bundle, vschema = ingester.insert_vocab_ignore_docfreq(batch_post)

            vocab_id = ingester.upsert_vocab2post(
                batch_post, post_id,
                vocab_bundle, vschema,
            )
            ingester.insert_title(batch_post, post_id)
            # ingester.update_vocab_postfreq(vocabulary_id)
            consumed += len(batch_post)
            logger.info('{} lines are ingested from {}'.format(consumed, post_parser.fpath))

    print('Elapsed time @post: {:.2f}sec.'.format(time.time() - start))

    consumed = 0
    for comments in comment_parser.batch_parse(batch_size=10):
        if len(comments) > 0:
            comment_id, batch_comment = ingester.insert_comment(comments)
            vocab_bundle, vschema = ingester.insert_vocab_ignore_docfreq(
                batch_comment, tokenized_field='comment_tokenized'
            )
            vocab_id = ingester.upsert_vocab2comment(
                batch_comment, comment_id,
                vocab_bundle, vschema,
            )
            # ingester.update_vocab_commentfreq(vocab_id)
            consumed += len(comments)
            logger.info('{} lines are ingested from {}'.format(consumed, comment_parser.fpath))
    print('Elapsed time @total: {:.2f}sec.'.format(time.time() - start))
