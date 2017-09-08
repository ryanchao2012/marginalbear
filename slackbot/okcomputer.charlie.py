# coding=UTF-8

import sys, os, datetime, time, re
import random
from slackbot import bot
from configparser import RawConfigParser
from slackbot.bot import Bot
from slackbot.bot import respond_to
from slackbot.bot import listen_to
from core.ingest import PsqlIngester
from core.utils import PsqlAbstract, PsqlQuery
from core.chat import RetrievalBase

# from bots import MessengerBot

config_parser = RawConfigParser()
config_parser.read('slackbot_config.ini')
## 請預先將主辦單位分發的 Bot token 設成環境變數，以避免放置在程式中有外流之疑慮
bot.settings.API_TOKEN  = config_parser.get('okcomputer', 'token')

config_parser.read('config.ini')
PsqlAbstract.set_database_info(
    config_parser.get('global', 'dbuser'),
    config_parser.get('global', 'dbname'),
    config_parser.get('global', 'dbpassword')
)
ingester = PsqlIngester('ccjieba')

feedback = RetrievalBase('ccjieba')


def algorithm(string):
    # reply = MessengerBot(string, 'messenger', 'slack').retrieve()
    reply = '嘻嘻'
    return string + ', ' + reply


@listen_to(r'『問題』(.*)')
def receive_question(message, question_string):
    answer = algorithm(question_string)
    message.send(answer)


@listen_to(r'qvocab (.*)')
def update_vocab_quality(message, word_to_update):
    help_msg = 'Format error:\nCommand template is:\n`qvocab <word:str> <quality:float>`'
    data = word_to_update.split()
    if len(data) != 2:
        message.send(help_msg)
    else:
        try:
            word = data[0].strip()
            quality = float(data[1])
            returning = ingester.update_vocab_quality(word, quality)
            reply = 'Vocab updated: {}'.format(returning[0])
        except Exception as err:
            reply = 'Exception occurred:\n```{}```'.format(err)
        finally:
            message.send(reply)



@listen_to(r'qtitle(.*)')
def update_title_quality(message, id_to_update):
    ingester.update_title_quality(id_to_update)
    response = feedback.query_title_quality_by_id(id_to_update)
    message.send(response)


@listen_to(r'qcomment(.*)')
def update_comment_quality(message, id_to_update):
    ingester.update_comment_quality(id_to_update)
    response = feedback.query_comment_quality_by_id(id_to_update)
    message.send(response)


def main():
    bot = Bot()
    bot.run()


if __name__ == '__main__':
    main()

