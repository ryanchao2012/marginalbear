import fileinput
import json
from marginalbear.core.utils import (
    to_halfwidth,
    to_lower
)
import datetime
import re as regex


for line in fileinput.input():
    fields = json.loads(line)
    id_field = fields['id']
    url_field = fields['url']
    author_field = fields['author']
    date_field = fields['publish_date']
    title_field = fields['title']
    title_clean = to_halfwidth(title_field)

    m = regex.search(r'\[(.+)\]', title_clean)
    if bool(m):
        tag = to_lower(m.group(1))
    else:
        tag = ''

    spider = to_lower(regex.search(r'www.ptt.cc/bbs/(\w+)/', url_field).group(1))

    author = author_field[:to_halfwidth(author_field).find('(')].strip()

    update = datetime.datetime.now()

    print(
        '"{id_}"\t"{tag}"\t"{spider}"\t"{url}"\t"{author}"\t"{publish}"\t"{update}"\t"{allow}"\t"{quality}"'.format(
            id_=id_field,
            tag=tag,
            spider=spider,
            url=url_field,
            author=author,
            publish=date_field,
            update=update,
            allow='t',
            quality=0.0
        )
    )
