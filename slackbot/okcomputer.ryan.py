# coding=UTF-8

import sys, os, datetime, time, re
import random
from slackbot import bot
from configparser import RawConfigParser
from slackbot.bot import Bot
from slackbot.bot import respond_to
from slackbot.bot import listen_to
from core.ranking import pos_idf_jaccard_similarity
from configparser import RawConfigParser
from core.utils import PsqlAbstract, clean_query
from core.chat import RetrievalEvaluate
from core.tokenizer import (
    JiebaTokenizer,
    OpenCCTokenizer,
    JiebaPosWeight
)


config_parser = RawConfigParser()
config_parser.read('../config.ini')

PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)

config_parser = RawConfigParser()
config_parser.read('slackbot_config.ini')

## 請預先將主辦單位分發的 Bot token 設成環境變數，以避免放置在程式中有外流之疑慮
bot.settings.API_TOKEN  = config_parser.get('okcomputer', 'token')


def algorithm(raw):
    # reply = MessengerBot(string, 'messenger', 'slack').retrieve()
    query, ctype = clean_query(raw)
    words = [w for w in OpenCCTokenizer(JiebaTokenizer()).cut(query) if bool(w.word.strip())]
    comments = RetrievalEvaluate('ccjieba', pweight=JiebaPosWeight.weight, title_ranker=pos_idf_jaccard_similarity).retrieve(words)
    candidates = []
    for i, cmt in enumerate(comments, 1):
        candidates.append('[{}] <{:.2f}> {}'.format(i, cmt.score, cmt.body))
    reply = '\n'.join(candidates)

    return reply


@listen_to(r'(.*)')
def receive_question(message, question_string):
    if message._body['channel'] == 'C719E5X38':
        answer = algorithm(question_string)
        message.send(answer)


def main():
    bot = Bot()
    bot.run()


if __name__ == '__main__':
    main()

