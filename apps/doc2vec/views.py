# import django.shortcuts import render
from django.http import HttpResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt

from core.metrics import doc2vec_ndcg
from core.utils import OkLogger
from gensim.models.doc2vec import Doc2Vec

# Create your views here.
oklogger = OkLogger('doc2vec.api')

d2v_path = '/var/local/marginalbear/doc2vec/title2comment.model'


oklogger.logger.info('Loading doc2vec model...')
d2v_model = Doc2Vec.load(d2v_path)
oklogger.logger.info('doc2vec model loaded.')

@csrf_exempt
@require_POST
def catch(request):
    topic_words = request.POST.get('topic_words', None)
    predict_words_ls = request.POST.get('predict_words_ls', None)

    if not(bool(topic_words) and bool(predict_words_ls)):
        oklogger.logger.warning('Void topic_words/predict_words_ls.')
        return HttpResponse('0')
    else:
        return HttpResponse(doc2vec_ndcg(topic_words, predict_words_ls, d2v_model))
