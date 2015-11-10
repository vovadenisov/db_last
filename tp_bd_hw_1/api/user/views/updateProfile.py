from json import dumps, loads

from django.db import connection, DatabaseError
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes
from api.user.utils import get_user_by_id
from api.queries.select import SELECT_USER_BY_EMAIL
from api.queries.update import UPDATE_USER

@csrf_exempt
def updateProfile(request):
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body) 
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))
    
    try:
        about = json_request['about']
        name = json_request['name']
        email = json_request['user']
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))    
    try: 
        __cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
        if not __cursor.rowcount:
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                       'response': 'user does not exist'})) 
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))
     
    user_id = __cursor.fetchone()[0]
    try:
        __cursor.execute(UPDATE_USER, [about, name, user_id])
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))

    try:
        user, related_ids = get_user_by_id(__cursor, user_id)
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)})) 
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))
  

