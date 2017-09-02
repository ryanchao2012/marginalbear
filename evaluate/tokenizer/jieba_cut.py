import jieba.posseg as pseg
import fileinput


for line in fileinput.input():
    print(' '.join([tok.word for tok in pseg.cut(line) if bool(tok.word.strip())]))

