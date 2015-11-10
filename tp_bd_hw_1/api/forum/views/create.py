from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes
from api.queries.select import SELECT_USER_BY_EMAIL, SELECT_LAST_INSERT_ID, SELECT_FORUM_BY_SHORT_NAME_FULL
from api.queries.insert import INSERT_FORUM

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
        name = json_request['name']
        short_name = json_request['short_name']
        email = json_request['user']
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))    
   
    try:
        __cursor.execute(SELECT_USER_BY_EMAIL, [email, ]) 
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if not __cursor.rowcount:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'user not found'}))

    user_id = __cursor.fetchone()[0]
    try:
        __cursor.execute(INSERT_FORUM, [name, short_name, user_id])
        __cursor.execute(SELECT_LAST_INSERT_ID, [])
        forum_id = __cursor.fetchone()[0]
        __cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': {
                                        'id': forum_id,
                                        'name': name,
                                        'short_name': short_name,
                                        'user': email
                                         }}))
    except IntegrityError:
        __cursor.execute(SELECT_FORUM_BY_SHORT_NAME_FULL, [short_name, ])
        existed_forum = __cursor.fetchone()
        __cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': {
                                        'id': existed_forum[0],
                                        'name': existed_forum[1],
                                        'short_name': existed_forum[2],
                                        'user': existed_forum[3]
                                         }}))
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

