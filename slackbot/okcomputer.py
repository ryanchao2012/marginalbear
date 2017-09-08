# coding=UTF-8

import sys, os, datetime, time, re
import random
from slackbot import bot
from configparser import RawConfigParser
from slackbot.bot import Bot
from slackbot.bot import respond_to
from slackbot.bot import listen_to
from core.ingest import PsqlIngester

# from bots import MessengerBot

config_parser = RawConfigParser()
config_parser.read('slackbot_config.ini')
## 請預先將主辦單位分發的 Bot token 設成環境變數，以避免放置在程式中有外流之疑慮
bot.settings.API_TOKEN  = config_parser.get('okcomputer', 'token')

ingester = PsqlIngester('ccjieba')


def algorithm(string):
    # reply = MessengerBot(string, 'messenger', 'slack').retrieve()
    reply = '嘻嘻'
    return string + ', ' + reply


@listen_to(r'『問題』(.*)')
def receive_question(message, question_string):
    answer = algorithm(question_string)
    message.send(answer)


@listen_to(r' /qvocab(.*)')
def update_vocab_quality(word_to_update):
    ingester.update_vocab_quality(word_to_update)


@listen_to(r' /qtitle(.*)')
def update_title_quality(id_to_update):
    ingester.update_title_quality(id_to_update)


@listen_to(r' /qcomment(.*)')
def update_comment_quality(id_to_update):
    ingester.update_comment_quality(id_to_update)


def main():
    bot = Bot()
    bot.run()


if __name__ == '__main__':
    main()

