# coding=UTF-8
from settings import (
    dbuser, dbname, dbpassword,
    config_parser
)

import sys, os, datetime, time, re
import random
from slackbot import bot
from slackbot.bot import Bot
from slackbot.bot import respond_to
from slackbot.bot import listen_to
from marginalbear.core.ranking import pos_idf_jaccard_similarity
from configparser import RawConfigParser
from marginalbear.core.utils import PsqlAbstract, clean_query
from marginalbear.core.chat import RetrievalEvaluate
from marginalbear.core.tokenizer import (
    JiebaTokenizer,
    OpenCCTokenizer,
    JiebaPosWeight
)

PsqlAbstract.set_database_info(dbuser, dbname, dbpassword)
default_reply = ['我累了', '❤️累了', '等等', '沒空']
topn = 15

# 請預先將主辦單位分發的 Bot token設成環境變數，以避免放置在程式中有外流之疑慮

bot.settings.API_TOKEN = config_parser.get('slack', 'bear')


def algorithm(raw):
    # reply = MessengerBot(string, 'messenger', 'slack').retrieve()
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
            candidates.append('[{}] <{:.2f}> {}'.format(rank, cmt.score, cmt.body))
            if rank >= topn:
                break

    reply = '\n'.join(candidates)

    return reply


@listen_to(r'(.*)')
def receive_question(message, question_string):
    if message._body['channel'] == 'C719E5X38':
        try:
            answer = algorithm(question_string)
            message.send(answer)
        except Exception as err:
            print(err)
            message.send(random.choice(default_reply))


def main():
    bot = Bot()
    bot.run()


if __name__ == '__main__':
    main()

