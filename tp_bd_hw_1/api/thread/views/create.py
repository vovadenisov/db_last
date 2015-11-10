from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_USER_BY_EMAIL, SELECT_FORUM_BY_SHORT_NAME, SELECT_LAST_INSERT_ID
from api.queries.insert import INSERT_THREAD
from api.queries.update import UPDATE_THREAD_SET_DELETE_FLAG


@csrf_exempt
def create(request):
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body) 
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))
    
    try:
        forum = json_request['forum']
        title = json_request['title']
        is_closed = json_request['isClosed']
        email = json_request['user']
        date = json_request['date']
        message = json_request['message']
        slug = json_request['slug']
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))    
   
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
                                   'response': 'user not found'}))
    user_id = __cursor.fetchone()[0]
    
    # validate forum
    try:
        __cursor.execute(SELECT_FORUM_BY_SHORT_NAME, [forum, ])  
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if not __cursor.rowcount:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'forum not found'}))
    forum_id = __cursor.fetchone()[0]
    #validate date
    date = general_utils.validate_date(date)
    if not date:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect date fromat'}))
    #validate message
    if not message:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'message should not be empty'}))

    #validate slug
    if not slug:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'slug should not be empty'}))
    #validate slug
    if not title:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'title should not be empty'}))

    #validate is_closed
    is_closed = bool(is_closed)
    try:
        __cursor.execute(INSERT_THREAD, [forum_id, title, is_closed, 
                                         user_id, date, message, slug])
        __cursor.execute(SELECT_LAST_INSERT_ID, [])
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)})) 
    thread_id = __cursor.fetchone()[0]

    is_deleted = json_request.get('isDeleted')
    if is_deleted is not None:
        is_deleted = bool(is_deleted)
        try:
            __cursor.execute(UPDATE_THREAD_SET_DELETE_FLAG, [is_deleted, thread_id])
        except DatabaseError as db_err: 
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)})) 
    else:
        is_deleted = False       
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': {
                                   "date": date,
                                   "forum": forum,
                                    "id": thread_id,
                                    "isClosed": is_closed,
                                    "isDeleted": is_deleted,
                                    "message": message,
                                    "slug": slug,
                                    "title": title,
                                    "user": email
                                }}))
                                
