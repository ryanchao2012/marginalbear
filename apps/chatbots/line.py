import os
import re
import requests

from flask import (
    Flask, request
)

from marginalbear.core.chat import RetrievalEvaluate
from marginalbear.core.ranking import pos_idf_jaccard_similarity
from marginalbear.core.tokenizer import JiebaPosWeight

from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import (
    FollowEvent, ImageSendMessage, JoinEvent, LeaveEvent,
    MessageEvent, SourceGroup, SourceRoom, SourceUser,
    TextMessage, TextSendMessage, UnfollowEvent,
)

from bots import LineBot
from settings import (
    line_webhook_parser,
    line_bot_api,
    slack_webhook
)

app = Flask(__name__)


@app.route('/linecallback', methods=['POST'])
def line_webhook():
    signature = request.headers['X_LINE_SIGNATURE']
    body = request.get_data(as_text=True)
    # body = request.body.decode('utf-8')

    try:
        events = line_webhook_parser.parse(body, signature)
    except InvalidSignatureError:
        return '', 404
    except LineBotApiError:
        return '', 400

    for event in events:
        if isinstance(event, MessageEvent):
            if isinstance(event.message, TextMessage):
                try:
                    query = event.message.text
                    utype, uid = _user_id(event.source)
                    retriever = RetrievalEvaluate('ccjieba', pweight=JiebaPosWeight.weight, title_ranker=pos_idf_jaccard_similarity)
                    bot = LineBot(retriever, uid, utype)
                    reply, state_code = bot.retrieve(query)
                    if bool(reply):
                        line_bot_api.reply_message(
                            event.reply_token,
                            _message_obj(reply)
                        )
                        app.logger.info('reply message: utype: {}, uid: {}, query: {}, reply: {}'.format(utype, uid, query, reply))

                        slack_log = 'query: {}, reply: {}'.format(query, reply) + '\n====================\n'
                        data = '{"text": \"' + slack_log + '\"}'
                        requests.post(slack_webhook, headers={'Content-type': 'application/json'}, data=data.encode('utf8'))

                    if state_code == LineBot.code_leave:
                        bot.leave()

                except Exception as err:
                    app.logger.error('platforms.line.line_webhook, message: {}'.format(err))

        elif isinstance(event, FollowEvent) or isinstance(event, JoinEvent):
            try:
                query = '<FollowEvent or JoinEvent>'
                utype, uid = _user_id(event.source)
                bot = LineBot(query, 'line', uid, utype)
                reply, state_code = bot.retrieve()

                if bool(reply):
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=reply))
                    app.logger.info('reply message: utype: {}, uid: {}, query: {}, reply: {}'.format(utype, uid, query, reply))

            except Exception as err:
                app.logger.error('okbot.chat_app.line_webhook, message: {}'.format(err))

        elif isinstance(event, UnfollowEvent) or isinstance(event, LeaveEvent):
            try:
                query = '<UnfollowEvent or LeaveEvent>'
                utype, uid = _user_id(event.source)
                app.logger.info('leave or unfollow: utype: {}, uid: {}, query: {}'.format(utype, uid, query))
            except Exception as err:
                app.logger.error('okbot.chat_app.line_webhook, message: {}'.format(err))

    return '', 200


def _message_obj(reply):
    if 'imgur' in reply:
        match_web = re.search(r'http:\/\/imgur\.com\/[a-z0-9A-Z]{7}', reply)
        match_jpg = re.search(r'http:\/\/(i|m)\.imgur\.com\/[a-z0-9A-Z]{7}\.jpg', reply)
        if match_web:
            match = match_web.group()
        elif match_jpg:
            match = match_jpg.group()
        else:
            match = reply
        imgur_url = re.sub('http', 'https', match)
        return ImageSendMessage(original_content_url=imgur_url,
                                preview_image_url=imgur_url)
    else:
        return TextSendMessage(text=reply)


def _user_id(source):
    if isinstance(source, SourceUser):
        utype = 'user'
        uid = source.user_id
    elif isinstance(source, SourceGroup):
        utype = 'group'
        uid = source.group_id
    elif isinstance(source, SourceRoom):
        utype = 'room'
        uid = source.room_id
    return utype, uid


if __name__ == "__main__":
    app.run(host='localhost')
