import os
from configparser import RawConfigParser


home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

config_parser = RawConfigParser()
config_parser.read(os.path.join(home, 'config.ini'))

line_bot_api = config_parser.get('line', 'channel_access_token')
line_webhook_parser = config_parser.get('line', 'channel_secret')
slack_webhook = config_parser.get('slack', 'webhook')
dbuser = config_parser.get('django', 'dbuser')
dbname = config_parser.get('django', 'dbname')
dbpassword = config_parser.get('django', 'dbpassword')
