import fileinput
import json
from core.tokenizer import JiebaTokenizer
from core.utils import (
    clean_comment, clean_query
)

tokenizer = JiebaTokenizer

# pv formatted.post.jieba.jl | parallel -j3 --pipe --round-robin --block 10K --line-buffer python extract.comment.from.formatted.post.py >! formatted.comment.jieba.jl

for line in fileinput.input():
    post = json.loads(line)
    comment_cleaned = post['comment_cleaned']
    try:
        comments = clean_comment(comment_cleaned)
    except Exception as err:
        print(err)
        print(comment_cleaned)

    for cmt in comments:
        content, ctype = clean_query(cmt['comment'])
        # cmt['comment'] = content
        if ctype == 'text':
            tokenized = tokenizer().cut(content)
        else:
            tokenized = [{'word': content, 'pos': ctype}]
        cmt['comment_tokenized'] = tokenized
        cmt['ctype'] = ctype

    batch_comment = dict(comments=comments, url=post['url'])
    print(json.dumps(batch_comment, ensure_ascii=False, sort_keys=True))
