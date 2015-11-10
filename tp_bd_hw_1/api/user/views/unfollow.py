from json import dumps, loads

from django.db import connection, DatabaseError
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes
from api.user.utils import get_user_by_id
from api.queries.delete import DELETE_FOLLOWER
from api.queries.select import SELECT_USER_BY_EMAIL

@csrf_exempt
def unfollow(request):
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body) 
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))
    
    try:
        follower = unicode(json_request['follower'])
        followee = unicode(json_request['followee'])
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))  

    # validate users
    users = [] 
    for email in [follower, followee]:
        try:
            user_id_qs = __cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
        except DatabaseError as db_err: 
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

        if not  __cursor.rowcount:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'user with not found'}))
        user_id = __cursor.fetchone()[0]
        users.append(user_id)

    try:
        __cursor.execute(DELETE_FOLLOWER, users)  
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))  
    
    try:
        user, related_ids = get_user_by_id(__cursor, users[0])
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)})) 
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))
