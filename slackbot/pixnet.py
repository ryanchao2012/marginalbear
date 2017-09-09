# coding=UTF-8

import sys, os, datetime, time, re
import random
from slackbot import bot
from configparser import RawConfigParser
from slackbot.bot import Bot
from slackbot.bot import respond_to
from slackbot.bot import listen_to
from slackbot.redischannel import RedisChannel
from core.chat import RetrievalEvaluate
from core.ranking import pos_idf_jaccard_similarity
from core.utils import PsqlAbstract, clean_query

from core.tokenizer import (
    JiebaTokenizer,
    OpenCCTokenizer,
    JiebaPosWeight
)

rc = RedisChannel()
rc.subscribe('from_okcomputer')
rc.get_all_pkgs()
config_parser = RawConfigParser()
config_parser.read('slackbot_config.ini')
## 請預先將主辦單位分發的 Bot token 設成環境變數，以避免放置在程式中有外流之疑慮
bot.settings.API_TOKEN  = config_parser.get('pixnet', 'token')


config_parser = RawConfigParser()
config_parser.read('../config.ini')

PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)

topn = 15
def algorithm(raw):
    query, ctype = clean_query(raw)
    words = [w for w in OpenCCTokenizer(JiebaTokenizer()).cut(query) if bool(w.word.strip())]
    comments = RetrievalEvaluate('ccjieba', pweight=JiebaPosWeight.weight, title_ranker=pos_idf_jaccard_similarity).retrieve(words)
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
            if rank >= topn:
                break
    return candidates


@listen_to(r'^『問題』(.*)')
def receive_question(message, question_string):
    if message._client.users[message._get_user_id()][u'name'] == "pixbot":
        candidates = algorithm(question_string)
        package = {'query': question_string, 'candidates': candidates}
        rc.send_to_channel('from_pixnet', package)
    for _ in range(10):
        ok_ans = rc.get_channel_data('from_okcomputer', wait=False)
        if ok_ans is None:
            time.sleep(1)
        else:
            break

    if ok_ans is None:
        pixnet_ans = random.choice(candidates[:1])['answer']
        message.send(pixnet_ans)
    else:
        message.send(ok_ans)


def main():
    bot = Bot(rc=rc)
    bot.run()


if __name__ == '__main__':
    main()

