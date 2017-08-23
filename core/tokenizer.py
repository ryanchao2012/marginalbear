import jieba
import jieba.posseg as pseg


class Word(dict):

    def __init__(self, word, pos='__unknown__'):
        dict.__init__(self, word=word, pos=pos)
        self.word = word
        self.pos = pos


class TokenizerNotExistException(Exception):
    pass


class Tokenizer(object):

    def __init__(self):
        pass

    def cut(self, sentence):
        raise NotImplementedError


class JiebaTokenizer(Tokenizer):

    def cut(self, sentence, pos=True):
        if pos:
            pairs = pseg.cut(sentence)
            tok = []

            for p in pairs:
                w = p.word.strip()
                if len(w) > 0:
                    tok.append(Word(w, p.flag))
            return tuple(tok)
        else:
            return tuple([Word(w.strip()) for w in jieba.cut(sentence) if bool(w.strip())])
