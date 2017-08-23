"""Ranking methods."""
from .utils import (
    OkLogger, Vocab
)


ranking_logger = OkLogger('ranking')


def jaccard_similarity(query_vocabs, doc_vocabs):
    """
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
        ranking_logger.info(err)
        return -1
