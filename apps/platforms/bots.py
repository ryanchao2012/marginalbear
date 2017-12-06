import random
import os
from marginalbear.core.utils import PsqlAbstract, clean_query
from marginalbear.core.tokenizer import (
    JiebaTokenizer,
    OpenCCTokenizer,
)

from linebot import LineBotApi, WebhookParser
from linebot.exceptions import LineBotApiError

from settings import (
    dbuser, dbname, dbpassword,
    config_parser
)


PsqlAbstract.set_database_info(dbuser, dbname, dbpassword)


class LineBot:
    code_leave = 1
    code_normal = 0
    topn = 15
    line_bot_api = LineBotApi(os.environ['LINE_CHANNEL_ACCESS_TOKEN'])
    line_webhook_parser = WebhookParser(os.environ['LINE_CHANNEL_SECRET'])

    kickout_key = ['滾喇', '滾', '邊緣熊袞']
    kickout_response = ['掰掰']

    def __init__(self, retriever, user_id, user_type):
        self.retriever = retriever
        self.user_id = user_id
        self.user_type = user_type

    def retrieve(self, query):
        if query in self.kickout_key and self.user_type != 'user':
            return random.choice(self.kickout_response), LineBot.code_leave

        else:
            cleaned, ctype = clean_query(query)
            words = [w for w in OpenCCTokenizer(JiebaTokenizer()).cut(cleaned) if bool(w.word.strip())]
            comments = self.retriever.retrieve(words)
            candidates = []
            unique_cache = []
            rank = 0
            for cmt in comments:
                if cmt.body not in unique_cache:
                    rank += 1
                    unique_cache.append(cmt.body)
                    candidates.append({
                        'rank': rank,
                        'score': cmt.score,
                        'answer': cmt.body
                    })
                if rank >= self.topn:
                        break
            return random.choice(candidates)['answer'], LineBot.code_normal

    def leave(self):
        try:
            if self.user_type == 'group':
                LineBot.line_bot_api.leave_group(self.uid)
            elif self.user_type == 'room':
                LineBot.line_bot_api.leave_room(self.uid)

        except LineBotApiError as err:
            print('LineBot.leave, message: {}'.format(err))

        # self._upsert_user(active=False)
