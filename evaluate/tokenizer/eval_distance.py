import edit_distance
import sys


def distance(ref_ls, hyp_ls):
    sm = edit_distance.SequenceMatcher(a=ref_ls, b=hyp_ls)

    return sm.distance()

def score_from_file(target='query', groundtrue='groundtrue', tokenizer='jieba'):
    scores = []
    with open('tokenizer.{}.{}'.format(target, groundtrue, 'r')) as g, open('tokenizer.{}.{}'.format(target, tokenizer)) as f:
        for gin, fin in zip(g, f):
            scores.append(distance(gin.split(), fin.split()))

    return float(sum(scores)) / float(len(scores))


# Usage: python eval_distance <target> <groundtrue> <tokenizer1> <tokenizer2> ...
# ex: python eval_distance query groundtrue jieba jseg
if __name__ == '__main__':
    num = len(sys.argv)
    if num < 4:
        print('what? bye')
        sys.exit()
    tokenizers = sys.argv[3:]
    target = sys.argv[1]
    groundtrue = sys.argv[2]

    for tok in tokenizers:
        print('{}: {:.3f}'.format(tok, score_from_file(target=target, groundtrue=groundtrue, tokenizer=tok)))


