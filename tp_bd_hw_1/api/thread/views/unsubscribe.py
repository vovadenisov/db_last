from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.delete import DELETE_SUBSCRIPTION
from api.queries.select import SELECT_USER_BY_EMAIL, SELECT_THREAD_BY_ID


@csrf_exempt
def unsubscribe(request):
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body) 
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))
    
    try:
        email = unicode(json_request['user'])
        thread = json_request['thread']
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))  

    # validate user
    try:
        user_id_qs = __cursor.execute(SELECT_USER_BY_EMAIL, [email, ]) 
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
    if not __cursor.rowcount:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                    'response': 'user with not found'}))
    user_id = __cursor.fetchone()[0]

    #validate thread
    thread_id = general_utils.validate_id(thread)
    if thread_id == False:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'}))
    try:
       thread_id_qs = __cursor.execute(SELECT_THREAD_BY_ID, [thread_id,]) 
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    if not __cursor.rowcount:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread was not found'}))
    thread_id = __cursor.fetchone()[0]  
      
    try:
        __cursor.execute(DELETE_SUBSCRIPTION, [thread_id, user_id])  
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))  
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': {"thread": thread_id,
                                            "user": email}}))


