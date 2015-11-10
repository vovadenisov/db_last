from json import dumps, loads

from django.db import connection, DatabaseError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

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
    thread_id = general_utils.validate_id(request.GET.get('thread'))
    if thread_id is None:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id not found'})) 
    if thread_id == False:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'})) 
    try:
        thread, related_ids = get_thread_by_id(__cursor, thread_id) 
        #print related_ids
    except TypeError:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'thread doesn\'t exist'}))
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))


    related = request.GET.getlist('related')
    
    related_functions_dict = {'user': get_user_by_id,
                              'forum': get_forum_by_id
                             }
    for related_ in related:
        if related_ in ['user', 'forum']:
            get_related_info_func = related_functions_dict[related_]
            thread[related_], related_ids_ = get_related_info_func(__cursor, related_ids[related_])
        else:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect related parameter'}))            
    __cursor.close()        
    return HttpResponse(dumps({'code': codes.OK,
                               'response': thread}))

