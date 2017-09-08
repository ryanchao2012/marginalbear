"""Ranking methods."""
import math
from .utils import (
    OkLogger
)


oklogger = OkLogger('ranking')


def jaccard_similarity(query_vocabs, doc_vocabs):
    """Jaccard similarity.

    query_vocabs: list of Vocab objects from query
    doc_vocabs: list of Vocab objects from document
    """
    qwords = [v.word for v in query_vocabs]
    docwords = [v.word for v in doc_vocabs]
    qset = set(qwords)
    dset = set(docwords)
    union = set(qwords + docwords)
    try:
        return len(qset.intersection(dset)) / float(len(union))
    except Exception as err:
        oklogger.logger.info(err)
        return -1


def pos_idf_jaccard_similarity(query_vocabs, doc_vocabs, doc_num=10000.0):
    union = {(v.word, v.pos): v for v in (query_vocabs + doc_vocabs)}
    inter = {k: v for k, v in union.items() if v in query_vocabs and v in doc_vocabs}

    w_inter = [v.weight * math.log(doc_num / min(1.0, v.titlefreq)) for k, v in inter.items()]
    w_union = [v.weight * math.log(doc_num / min(1.0, v.titlefreq)) for k, v in union.items()]

    return sum(w_inter) / sum(w_union)
