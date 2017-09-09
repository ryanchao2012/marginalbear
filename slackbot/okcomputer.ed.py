# coding=UTF-8

import sys, os, datetime, time, re
import random
import time
from slackbot import bot
from configparser import RawConfigParser
from slackbot.bot import Bot
from slackbot.bot import respond_to
from slackbot.bot import listen_to

from collections import Counter

# from bots import MessengerBot

config_parser = RawConfigParser()
config_parser.read('slackbot_config.ini')
## 請預先將主辦單位分發的 Bot token 設成環境變數，以避免放置在程式中有外流之疑慮
bot.settings.API_TOKEN = config_parser.get('okcomputer', 'token')


vote_result_temp = []
candidate_answers_temp = []
sent_answer = []


@listen_to(r'『問題』(.*)')
def receive_question(message, question_string):

    candidate_answers_temp.clear()
    vote_result_temp.clear()
    sent_answer.clear()

    # get candidate answer with query
    candidate_answers = query_for_candidate_answers(question_string)
    print(question_string)

    for item in candidate_answers:
        candidate_answers_temp.append(item)

    temp = []
    for item in candidate_answers:
        answer_string = "[{}]\t<{}>\t{}".format(item.get('rank'), str(item.get('score')), item.get('answer'))
        temp.append(answer_string)

    candidate_answers_string = "\n".join(temp)
    print(candidate_answers_string)

    message.send("@edhsu says: 問題: {}\n\n答案選項:\n {}\n\n都幾!".format(question_string, candidate_answers_string))

    # Send default answer
    time.sleep(3)
    if len(vote_result_temp) < 1 and len(sent_answer) == 0:
        default_answer = candidate_answers_temp[0].get('answer')
        sent_answer.append(default_answer)

        """emit answer to pixnetbot"""
        message.send('@edhsu says: Send Default Answer 選項[{}]: "{}"'.format(str(0+1), default_answer))
        print("Send default_answer: {}".format(default_answer))




def query_for_candidate_answers(question_string):

    # query here

    answers = [
        {"rank": "1", "score": "100", "answer": "candidate_answers_1"},
        {"rank": "2", "score": "90", "answer": "candidate_answers_2"},
        {"rank": "3", "score": "80", "answer": "candidate_answers_3"},
        {"rank": "4", "score": "70", "answer": "candidate_answers_4"},
        {"rank": "5", "score": "60", "answer": "candidate_answers_5"}
    ]

    return answers


@listen_to(r'([0-9]){1}')
def vote_for_candidate_answers(message, answer_number):

    vote_result_temp.append(int(answer_number) - 1)

    count = Counter()
    for item in vote_result_temp:
        count[item] += 1

    leading_answer_index = count.most_common(1)[0][0]
    leading_answer_count = count.most_common(1)[0][1]

    if len(vote_result_temp) == 2 and len(sent_answer) == 0:
        sent_answer.append(candidate_answers_temp[leading_answer_index].get('answer'))

        """emit answer to pixnetbot"""
        message.send('@edhsu says: Send Candidate Answer 選項[{}]: "{}" 票數+{}!!!'.format(str(leading_answer_index+1),
                                                                                       candidate_answers_temp[leading_answer_index].get('answer'),
                                                                                       str(leading_answer_count)))

        print('[{}]: "{}"'.format(str(leading_answer_index+1), candidate_answers_temp[leading_answer_index].get('answer')))

    else:
        pass


def main():
    bot = Bot()
    bot.run()


if __name__ == '__main__':
    main()

