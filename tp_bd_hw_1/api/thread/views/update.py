from json import dumps, loads

from django.db import connection, DatabaseError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.thread.utils import get_thread_by_id
from api.queries.update import UPDATE_THREAD
from api.queries.select import SELECT_THREAD_BY_ID


@csrf_exempt
def update(request):
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body) 
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))   
    try:
        thread_id = json_request['thread']
        message = unicode(json_request['message'])
        slug = unicode(json_request['slug'])
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    thread_id = general_utils.validate_id(thread_id) 
    if thread_id == False:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'})) 
    try:
        __cursor.execute(SELECT_THREAD_BY_ID, [thread_id, ]) 
        if not __cursor.rowcount:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                       'response': 'thread not found'}))
        __cursor.execute(UPDATE_THREAD, [message, slug, thread_id, ]) 
        thread, related_obj = get_thread_by_id(__cursor, thread_id)
    except DatabaseError as db_err:
        __cursor.close() 
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)})) 
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': thread}))

