from json import dumps

from django.db import connection, DatabaseError
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes, utils as general_utils
from api.user.utils import get_user_by_id
from api.thread.utils import get_thread_by_id
from api.forum.utils import get_forum_by_id
from api.post.utils import get_post_by_id


def details(request):
    __cursor = connection.cursor()
    if request.method != 'GET':
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    post_id = general_utils.validate_id(request.GET.get('post'))
    if post_id is None:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id not found'})) 
    if post_id == False:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id should be int'})) 
    try:
        post, related_ids = get_post_by_id(__cursor, post_id) 
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    except TypeError:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'post not found'}))
    related = request.GET.getlist('related')
    related_functions_dict = {
                          'user': get_user_by_id,
                          'thread': get_thread_by_id,
                          'forum': get_forum_by_id
                          }
    for related_ in related:
        if related_ in ['user', 'forum', 'thread']:
            get_related_info_func = related_functions_dict[related_]
            post[related_], related_ids_ = get_related_info_func(__cursor, related_ids[related_]) 
        else:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect related parameter'})) 
    __cursor.close()        
    return HttpResponse(dumps({'code': codes.OK,
                               'response': post}))
