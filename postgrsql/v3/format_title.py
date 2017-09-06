import fileinput
import json
from core.tokenizer import (
    OpenCCTokenizer,
    JiebaTokenizer
)
from core.utils import (
    clean_query,
    to_halfwidth,
    Word
)

tokenizer = 'ccjieba'

for line in fileinput.input():
    fields = json.loads(line)
    id_field = fields['id']
    title_field = fields['title']

    title_half = to_halfwidth(title_field)

    idx = title_half.find(']')
    if idx > 0:
        title = title_field[idx + 1:]
    else:
        title = title_field

    title_cleaned, ctype = clean_query(title)

    if ctype == 'text':
        wds = OpenCCTokenizer(JiebaTokenizer()).cut(title_cleaned)
        words = [w for w in wds if bool(w.word.strip())]
    else:
        words = [Word(title_cleaned, 'url')]

    tokenized = ' '.join([w.word.strip() for w in words])
    tokenized = tokenized.replace('\\', '\\\\')
    tokenized = tokenized.replace('"', '\\"').strip()
    grammar = ' '.join([w.pos.strip() for w in words]).strip()

    print(
        '"{ctype}"\t"{tokenizer}"\t"{tokenized}"\t"{grammar}"\t"{retrieval_count}"\t"{post}"\t"{quality}"'.format(
            ctype=ctype,
            tokenizer=tokenizer,
            tokenized=tokenized,
            grammar=grammar,
            retrieval_count=0,
            post=id_field,
            quality=0.0
        )
    )
