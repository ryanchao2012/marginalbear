import jieba
from core.tokenizer import (
    OpenCCTokenizer,
    JiebaTokenizer
)
import fileinput


for line in fileinput.input():
    print(' '.join([w.word for w in OpenCCTokenizer(JiebaTokenizer()).cut(line)]))

