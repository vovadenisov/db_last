from json import dumps, loads

from django.db import connection, DatabaseError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_THREAD_BY_ID
from api.queries.update import UPDATE_THREAD_VOTES
from api.thread.utils import get_thread_by_id

@csrf_exempt
def vote(request):
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body) 
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))   
    try:
        thread_id = json_request['thread']
        vote = json_request['vote']
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    thread_id = general_utils.validate_id(thread_id) 
    if thread_id == False:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id should be int'}))
    try:
        vote = int(vote)
        if abs(vote) != 1:
            raise ValueError
    except ValueError:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect vote value'})) 
    if vote < 0:
        column_name = 'dislikes'
    else:
        column_name = 'likes'
  
    try:
        __cursor.execute(SELECT_THREAD_BY_ID, [thread_id, ]) 
        if not __cursor.rowcount:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                       'response': 'thread not found'}))
        thread_id_qs = __cursor.execute(UPDATE_THREAD_VOTES.format(column_name, column_name), [thread_id, ])
        thread, related_obj = get_thread_by_id(__cursor, thread_id)
    except DatabaseError as db_err:
        __cursor.close() 
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))     
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': thread}))
