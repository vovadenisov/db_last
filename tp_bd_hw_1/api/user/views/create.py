from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes
from api.queries.select import SELECT_LAST_INSERT_ID
from api.queries.insert import INSERT_USER
from api.queries.update import UPDATE_USER_ANONYMOUS_FLAG


@csrf_exempt                              
def create(request):
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body) 
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))
   
    try:
        username = json_request['username']
        about = json_request['about']
        name = json_request['name']
        email = json_request['email']
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))    

    try:
        __cursor.execute(INSERT_USER, [username, about, name, email])
        __cursor.execute(SELECT_LAST_INSERT_ID, [])
    except IntegrityError as i_err:
        __cursor.close()
        #print i_err
        return HttpResponse(dumps({'code': codes.USER_ALREADY_EXISTS,
                                   'response': 'user already exists'}))#'user already exists'})) 

    except DatabaseError as db_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))
    user_id = __cursor.fetchone()[0]
    user = {"about": about,
            "email": email,
            "id": user_id,
            "isAnonymous": False,
            "name": name,
            "username": username
             }     
    try:
        isAnonymous = json_request['isAnonymous'] 
        if not isinstance(isAnonymous, bool):
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'isAnonymous flag should be bool'}))
    except KeyError:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': user}))
    try:
        __cursor.execute(UPDATE_USER_ANONYMOUS_FLAG, [isAnonymous, user_id])
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))
    user["isAnonymous"] = isAnonymous 
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))


