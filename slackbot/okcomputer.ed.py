# coding=UTF-8

import sys, os, datetime, time, re
import random
import time
from slackbot import bot
from configparser import RawConfigParser
from slackbot.bot import Bot
from slackbot.bot import respond_to
from slackbot.bot import listen_to

from slackbot.redischannel import RedisChannel

from collections import Counter

# from bots import MessengerBot

config_parser = RawConfigParser()
config_parser.read('slackbot_config.ini')
## 請預先將主辦單位分發的 Bot token 設成環境變數，以避免放置在程式中有外流之疑慮
bot.settings.API_TOKEN = config_parser.get('okcomputer', 'token')


vote_result_temp = []
candidate_answers_temp = []
sent_answer = []

rc = RedisChannel()
rc.subscribe('from_okcomputer')

bot = Bot(rc=rc)
client = bot._client

@rc.listen_to(channel='from_pixnet')
def receive_question(question_candidates):

    candidate_answers_temp.clear()
    vote_result_temp.clear()
    sent_answer.clear()

    # get candidate answer with query
    # candidate_answers = query_for_candidate_answers(question_string)
    print(question_candidates)
    print(question_candidates.get('query'))
    print(question_candidates.get('candidates'))
    # query candidates
    
    for item in question_candidates.get('candidates'):
        candidate_answers_temp.append(item)

    temp = []
    for item in candidate_answers_temp:
        answer_string = "[{}]\t{}\t{}".format(item.get('rank'), str(round(item.get('score'), 2)), item.get('answer'))
        temp.append(answer_string)

    candidate_answers_string = "\n".join(temp)
    print(candidate_answers_string)

    client.rtm_read()
    client.send_message('general', "問題: {}\n\n答案選項:\n {}\n\n都幾!".format(question_candidates.get('query'), candidate_answers_string))

    time.sleep(8)

    for pkg in client.rtm_read():

        if 'channel' in pkg and pkg['type'] == 'message':
            if client.channels[pkg['channel']]['name'] == 'general':
                try:
                    ans = int(pkg['text'])
                    if ans in range(len(candidate_answers_temp)):
                        vote_result_temp.append(ans)
                except ValueError:
                    pass

    result = Counter(vote_result_temp)
    client.send_message('general', f'{result.most_common(5)}')

    if len(result) > 0:
        top1 = result.most_common(1)[0][0]
        cands = question_candidates['candidates']
        top1 = cands[top1-1]['answer']
        print(top1)
        print(type(top1))
        client.send_message('general', top1 + '\n')
        rc.send_to_channel('from_okcomputer', top1)

'''

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

'''

'''
@listen_to(r'(\d{1,2})')
def vote_for_candidate_answers(message, answer_number):



    if int(answer_number) - 1 in range(len(candidate_answers_temp)):
        vote_result_temp.append(int(answer_number) - 1)

    else:
        pass
'''

'''
for pkg in bot._client.rtm_read():
    if 'channel' in pkg and pkg['type'] == 'message':
        if bot._client.channels[pkg['channel']]['name'] == 'random':
#             print(pkg)
            print(pkg['text'])
'''

'''
# @listen_to(r'(\q{5})')
def count_and_send_candidate_answers(message, stop_command):

    result = Counter(vote_result_temp)

    if len(result) == 0:
        message.send("Final Answer: " + candidate_answers_temp[0].get('answer'))
        rc.send_to_channel('from_okcomputer', candidate_answers_temp[0].get('answer'))

    else:
        # count result with max voted number, return it's index
        best_answer_index = max(result, key=result.get)

        # send answer
        message.send("Final Answer: " + candidate_answers_temp[best_answer_index].get('answer'))
        rc.send_to_channel('from_okcomputer', candidate_answers_temp[best_answer_index].get('answer'))

'''
rc.get_all_pkgs()
bot.run()
