import jieba
import jieba.posseg as pseg
from .utils import Word


class TokenizerNotExistException(Exception):
    pass


class Tokenizer(object):

    def __init__(self):
        pass

    def cut(self, sentence):
        raise NotImplementedError


class JiebaTokenizer(Tokenizer):

    def __call__(self, sentence):
        return self.cut(sentence)

    def cut(self, sentence, pos=True):
        if pos:
            pairs = pseg.cut(sentence)
            tok = []

            for p in pairs:
                w = p.word.strip()
                if len(w) > 0:
                    tok.append(Word(w, p.flag))
            return tok
        else:
            return [Word(w.strip()) for w in jieba.cut(sentence) if bool(w.strip())]
