import jieba
import jieba.posseg as pseg
import opencc
from .utils import Word


class TokenizerNotExistException(Exception):
    pass


class Tokenizer(object):

    def __init__(self):
        pass

    def __call__(self, sentence, pos=True):
        return self.cut(sentence, pos=pos)

    def cut(self, sentence, pos=True):
        raise NotImplementedError


class JiebaTokenizer(Tokenizer):

    def cut(self, sentence, pos=True):
        if pos:
            pairs = pseg.cut(sentence)
            tok = []

            for p in pairs:
                tok.append(Word(p.word, pos=p.flag))
            return tok
        else:
            return [Word(w) for w in jieba.cut(sentence)]


class OpenCCTokenizer(Tokenizer):

    def __init__(self, tokenizer_instance):
        self.tokenizer = tokenizer_instance

    def cut(self, sentence, pos=True):
        simplified = opencc.convert(sentence, config='tw2s.json')

        tokenized = self.tokenizer(simplified, pos=pos)

        recovered = []
        head = 0
        for tok in tokenized:
            l = len(tok.word)
            recovered.append(Word(sentence[head : head + l], pos=tok.pos))
            head += l
        return recovered


class SplitTokenizer(Tokenizer):

    def cut(self, sentence):
        words = []
        for mix in sentence.split():
            idx = mix.rfind(':')
            w, p = mix[:idx], mix[idx + 1:]
            words.append(Word(w, pos=p))
        return words


class JiebaPosWeight:

    weight = dict(
        n=4.0,
        nr=7.5,
        v=2.0,
        t=1.0,
        z=2.5,
        r=1.0,
        m=1.5,
        x=0.7,
        ns=5.0,
        w=1.0,
        i=5.5,
        l=1.8,
        vg=1.4,
        nz=5.5,
        eng=5.0,
        y=0.5,
        ng=3.0,
        zg=3.0,
    )
