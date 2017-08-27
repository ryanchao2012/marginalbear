import numpy as np
import math


def doc2vec_ndcg(topic_words, predict_words_ls, model, k=30, ideal=0.8, ave=1, step=20):
    """Document-to-vector NDCG."""
    if len(predict_words_ls) <= 0:
        return 0.0
    rel = np.zeros((ave, len(predict_words_ls[:k])))
    for i in range(ave):
        rel[i, :] = np.asarray(
            [
                model.docvecs.similarity_unseen_docs(
                    model, topic_words, predict_words, steps=step
                )
                for predict_words in predict_words_ls[:k]
            ]
        )

    rel = rel.mean(axis=0)

    dcg = rel[0]
    for i, r in enumerate(rel[1:], 2):
        dcg += (r / math.log2(i))

    icdg = ideal
    for i in range(2, 2 + len(rel[1:])):
        icdg += (ideal / math.log2(i))

    return dcg / icdg
