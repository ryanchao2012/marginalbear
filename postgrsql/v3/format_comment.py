import fileinput
import json
from core.tokenizer import (
    OpenCCTokenizer,
    JiebaTokenizer
)
from core.utils import (
    clean_query,
    to_halfwidth,
    Word,
    aggregate_comment
)

tokenizer = 'ccjieba'

for line in fileinput.input():
    fields = json.loads(line)
    id_field = fields['id']
    comment_field = fields['comment']

    comments = aggregate_comment(comment_field)

    outputs = []

    for cmt in comments:
        content, ctype = clean_query(cmt['comment'])
        if ctype == 'text':
            wds = OpenCCTokenizer(JiebaTokenizer()).cut(content)
            words = [w for w in wds if bool(w.word.strip())]
        else:
            words = [Word(content, 'url')]

        tokenized = ' '.join([w.word.strip() for w in words])
        tokenized = tokenized.replace('\\', '\\\\')
        tokenized = tokenized.replace('"', '\\"').strip()
        grammar = ' '.join([w.pos.strip() for w in words]).strip()
        # cmt['tokenized'] = tokenized
        # cmt['grammar'] = grammar
        # cmt['ctype'] = ctype

        outputs.append(
            '"{audience}"\t"{floor}"\t"{ctype}"\t"{tokenizer}"\t"{tokenized}"\t"{grammar}"\t"{retrieval_count}"\t"{post}"\t"{quality}"'.format(
                audience=cmt['audience'],
                floor=cmt['floor'],
                ctype=ctype,
                tokenizer=tokenizer,
                tokenized=tokenized,
                grammar=grammar,
                retrieval_count=0,
                post=id_field,
                quality=0.0
            )
        )

    print('\n'.join(outputs))

