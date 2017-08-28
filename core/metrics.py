import numpy as np
import requests
import math


def doc2vec_ndcg(topic_words, predict_words_ls, model, k=30, ideal=0.9, ave=1, step=100):
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


class MetricApiWrapper(object):

    def __init__(self, url):
        self.url = url

    def __call__(self, topic_words, predict_words_ls):

        return self.request(topic_words, predict_words_ls)

    def request(self, topic_words, predict_words_ls):
        topic_words_ = [w.word for w in topic_words]
        predict_words_ls_ = [[w.word for w in words] for words in predict_words_ls]

        payload = dict(topic_words=topic_words_, predict_words_ls=predict_words_ls_)

        response = requests.post(self.url, payload=payload)

        return response
