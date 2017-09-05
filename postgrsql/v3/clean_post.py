import fileinput
from core.utils import (
    to_halfwidth,
    to_lower
)
import datetime
import re as regex


for line in fileinput.input():
    fields = line.split('\t')
    id_field = fields[0]
    url_field = fields[1]
    author_field = fields[2]
    date_field = fields[3]
    title_field = fields[4]
    title_clean = to_halfwidth(title_field)

    m = regex.search(r'\[(.+)\]', title_clean)
    if bool(m):
        tag = to_lower(m.group(1))
    else:
        tag = ''

    spider = to_lower(regex.search(r'www.ptt.cc/bbs/(\w+)/', url_field).group(1))

    author = author_field[:to_halfwidth(author_field).find('(')]

    update = datetime.datetime.now()

    print(
        '{id_}\t{tag}\t{spider}\t{url}\t{publish}\t{allow}\t{quality}'.format(
            id_=id_field,
            tag=tag,
            spider=spider,
            url=url_field,
            publish=date_field,
            update=update,
            allow='t',
            quality=0.0
        )
    )
