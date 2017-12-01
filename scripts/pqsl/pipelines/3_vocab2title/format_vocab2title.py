import fileinput
import json

tokenizer = 'ccjieba'
vocabulary = {}

vocabt_path = '/var/local/marginalbear/data/5_vocabt.jl'


def load_vocabulary(path=vocabt_path):
    global vocabulary

    with open(path, 'r') as f:
        for line in f:
            vocabt = json.loads(line)
            key = (vocabt['word'], vocabt['pos'], vocabt['tokenizer'])

            if key not in vocabulary:
                vocabulary[key] = vocabt['id']


# Loading batch vocabulary
load_vocabulary()


for line in fileinput.input():
    fields = json.loads(line)
    id_field = fields['id']
    tokenized = fields['tokenized']
    grammar = fields['grammar']

    vocabs = [(w, p, tokenizer) for w, p in zip(tokenized.split(), grammar.split())]

    vocab2title = []

    for v in vocabs:
        if v in vocabulary:
            vocab2title.append((vocabulary[v], id_field))

    out = '\n'.join(['{vid}\t{tid}'.format(vid=v2t[0], tid=v2t[1]) for v2t in vocab2title])

    if len(out.strip()) > 0:
        print(out)
