#!/usr/bin/env python
import edit_distance
import sys


def norm_distance(rel_ls, hyp_ls):
    num = len(rel_ls)
    score = distance(rel_ls, hyp_ls)

    return score / float(num) if num > 0 else 0.0

def distance(ref_ls, hyp_ls):
    sm = edit_distance.SequenceMatcher(a=ref_ls, b=hyp_ls)

    return sm.distance()

def score_from_file(target='query', groundtrue='groundtrue', tokenizer='jieba'):
    scores = []
    with open('data/{}.{}'.format(target, groundtrue, 'r')) as g, open('data/{}.{}'.format(target, tokenizer)) as f:
        for gin, fin in zip(g, f):
            scores.append(norm_distance(gin.split(), fin.split()))

    return float(sum(scores)) / float(len(scores))


def print_help_msg():
    print(
        '''
            Usage: python eval_distance <target> <groundtrue> <tokenizer1> <tokenizer2> ...
            Ex: python eval_distance query groundtrue jieba jseg
        '''
    )

# Usage: python eval_distance <target> <groundtrue> <tokenizer1> <tokenizer2> ...
# ex: python eval_distance query groundtrue jieba jseg
if __name__ == '__main__':
    num = len(sys.argv)
    if num < 4:
        print_help_msg()
        sys.exit()
    tokenizers = sys.argv[3:]
    target = sys.argv[1]
    groundtrue = sys.argv[2]
    try:
        for tok in tokenizers:
            print('{}: {:.3f}'.format(tok, score_from_file(target=target, groundtrue=groundtrue, tokenizer=tok)))
    except Exception as err:
        print(err)
        print_help_msg()


