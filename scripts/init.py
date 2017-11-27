import os
import sys
from configparser import RawConfigParser


home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(home)

config_parser = RawConfigParser()
config_parser.read(os.path.join(home, 'config.ini'))

dbuser = config_parser.get('django', 'dbuser')
dbname = config_parser.get('django', 'dbname')
dbpassword = config_parser.get('django', 'dbpassword')
line_channel_secret = config_parser.get('line', 'channel_secret')
