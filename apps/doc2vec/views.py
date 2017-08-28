# from django.shortcuts import render
from django.http import HttpResponse
from django.views.decorators.http import require_POST
from core.metrics import doc2vec_ndcg
from core.utils import OkLogger
from gensim.models.doc2vec import Doc2Vec

# Create your views here.
logger = OkLogger('doc2vec.api')

d2v_path = '/var/local/okbot/doc2vec/tok2push2.model'


logger.info('Loading doc2vec model...')
d2v_model = Doc2Vec.load(d2v_path)
logger.info('doc2vec model loaded.')


@require_POST
def catch(request):
    topic_words = request.POST.get('topic_words', None)
    predict_words_ls = request.POST.get('predict_words_ls', None)

    if topic_words is None or predict_words_ls is None:
        logger.warning('Void topic_words/predict_words_ls.')
        return HttpResponse('0')
    else:
        return HttpResponse(doc2vec_ndcg(topic_words, predict_words_ls, d2v_model))
