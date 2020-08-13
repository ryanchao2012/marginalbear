from linebot import LineBotApi, WebhookParser

from ..settings import config_parser

line_bot_api = LineBotApi(config_parser.get('line', 'channel_access_token'))
line_webhook_parser = WebhookParser(config_parser.get('line', 'channel_secret'))
slack_webhook = config_parser.get('slack', 'webhook')
dbuser = config_parser.get('django', 'dbuser')
dbname = config_parser.get('django', 'dbname')
dbpassword = config_parser.get('django', 'dbpassword')
