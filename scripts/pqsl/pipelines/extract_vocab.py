import fileinput
import json

for line in fileinput.input():
    try:
        fields = json.loads(line)
        id_field = fields['id']
        tokenized = fields['tokenized']
        tokenized = tokenized.replace('\\', '\\\\')
        tokenized = tokenized.replace('"', '\\"').split()
        grammar = fields['grammar'].split()
        tokenizer = fields['tokenizer']
    
        out = '\n'.join(
                [
                    '"{word}"\t"{pos}"\t"{tokenizer}"\t"{titlefreq}"\t"{contentfreq}"\t"{commentfreq}"\t"{stopword}"\t"{quality}"'.format(
                        word=w, pos=p, tokenizer=tokenizer, titlefreq=0, contentfreq=0, commentfreq=0, stopword='f', quality=0.0
                    ) for w, p in zip(tokenized, grammar)
                ]
        )
        if len(out.strip()) > 0:
            print(out)
    except Exception as err:
        raise RuntimeError(line)
        

