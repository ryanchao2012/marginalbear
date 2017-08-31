import jieba
import fileinput


for line in fileinput.input():
    print(' '.join(list(jieba.cut(line))))

