from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_FORUM_BY_SHORT_NAME, SELECT_USER_ID_BY_FORUM
from api.user.utils import get_user_by_id


def listUsers(request):
    __cursor = connection.cursor()
    if request.method != 'GET':
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    short_name = request.GET.get('forum')
    if not short_name:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'forum name not found'})) 
    try:
        forum_qs = __cursor.execute(SELECT_FORUM_BY_SHORT_NAME, [short_name, ])
        if not __cursor.rowcount:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'forum not found'}))
    except DatabaseError as db_err:
        __cursor.close() 
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    forum_id = __cursor.fetchone()[0]

    get_all_forum_users_specified_query = SELECT_USER_ID_BY_FORUM
    query_params = [forum_id, ]
    since_id = general_utils.validate_id(request.GET.get('since_id'))
    if since_id:
        get_all_forum_users_specified_query += '''AND user.id >= %s '''
        query_params.append(since_id)
    elif since_id == False and since_id is not None:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'since_id should be int'})) 
   
    order = request.GET.get('order', 'desc')
    if order.lower() not in ('asc', 'desc'):
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect order parameter: {}'.format(order)}))
    
    get_all_forum_users_specified_query += '''ORDER BY user.name ''' + order

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        get_all_forum_users_specified_query += ''' LIMIT %s'''
        query_params.append(limit)

    try:
        users_qs = __cursor.execute(get_all_forum_users_specified_query, query_params) 
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)})) 
    users = []
    for user in __cursor.fetchall():
        users.append(get_user_by_id(__cursor, user[0])[0])  
    __cursor.close()       
    return HttpResponse(dumps({'code': codes.OK,
                               'response': users
                               }))

