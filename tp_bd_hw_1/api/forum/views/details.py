from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.http import HttpResponse

from api.general import codes
from api.queries.select import SELECT_FORUM_BY_SHORT_NAME_FULL
from api.user.utils import get_user_by_id

def details(request):
    __cursor = connection.cursor()
    if request.method != 'GET':
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    short_name = request.GET.get('forum')
    if not short_name:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'forum name not found'})) 
    try:
        __cursor.execute(SELECT_FORUM_BY_SHORT_NAME_FULL, [short_name, ]) 
        if not __cursor.rowcount:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'forum not found'}))
    except DatabaseError as db_err:
        __cursor.close() 
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    forum = __cursor.fetchone()
    response = {"id": forum[0],
                "name": forum[1],
                "short_name": forum[2]
               }

    related = request.GET.get('related')
    if related:
        if related != 'user':
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect related parameter: {}'.format(related)}))
        user_id = forum[4]
        try:
            user, related_ids = get_user_by_id(__cursor, user_id)
        except DatabaseError as db_err: 
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
        response['user'] = user
        
    else:
        response["user"] = forum[3]  
        __cursor.close()       
    return HttpResponse(dumps({'code': codes.OK,
                               'response': response}))

