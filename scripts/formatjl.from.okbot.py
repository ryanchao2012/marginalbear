# -*- coding: utf-8 -*-

# pv push0814.jl | parallel -j3 --pipe --round-robin --block 10K python formatjl.from.okbot.py >! formatted.post.jieba.jl

import json
from core.tokenizer import JiebaTokenizer
import fileinput
from core.utils import (
    clean_query, clean_comment
)


tokenizer = JiebaTokenizer

for line in fileinput.input():
    post = json.loads(line)

    title = post['title']
    title_raw = title.strip()
    title_cleaned, ctype = clean_query(title_raw)

    if ctype == 'text':
        title_tokenized = tokenizer().cut(title_cleaned)
    else:
        title_tokenized = [{'word': title_cleaned, 'pos': ctype}]

    # url = post['url']
    # date = post['publish_date']
    # tag = post['tag']
    # spider = post['spider']
    comment_raw = post['push']
    try:
        comments = clean_comment(comment_raw)
    except Exception as err:
        print(comment_raw)
        raise err
    # for cmt in comments:
    #    if cmt['ctype'] == 'text':
    #        tokenized = tokenizer(cmt['comment']).cut()
    #    else:
    #        tokenized = [{'word': cmt['comment'], 'pos': cmt['ctype']}]
    #    cmt['comment_tokenized'] = tokenized

    comment_sorted = sorted(comments, key=lambda cmt: cmt['floor'])
    comment_cleaned = '\n'.join(
        [
            '{}: {}'.format(cmt['audience'], cmt['comment'])
            for cmt in comment_sorted
        ]
    )

    formatted = dict(
        title_raw=title_raw,
        title_cleaned=title_cleaned,
        title_tokenized=title_tokenized,
        comment_raw=comment_raw,
        comment_cleaned=comment_cleaned,
        # 'comments': comments,
        tag=post['tag'],
        url=post['url'],
        spider=post['spider'],
        author=post['author'],
        publish_date=post['publish_date'],
        ctype=ctype,
    )

    print(json.dumps(formatted, ensure_ascii=False, sort_keys=True))
