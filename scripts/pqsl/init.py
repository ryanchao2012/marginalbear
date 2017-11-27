import os
import sys

home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(home)

from scripts.init import (
    dbuser, dbname, dbpassword,
    line_channel_secret
)

