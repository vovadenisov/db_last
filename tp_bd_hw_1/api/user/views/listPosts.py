from json import dumps, loads

from django.db import connection, DatabaseError
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_POSTS_BY_USER, SELECT_USER_BY_EMAIL


def listPosts(request):
    __cursor = connection.cursor()
    if request.method != 'GET':
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    email = request.GET.get('user')
    # validate user
    try:
        user_id_qs = __cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
    except DatabaseError as db_err:
        __cursor.close() 
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if not  __cursor.rowcount:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'user not found'}))
    user_id = __cursor.fetchone()[0]
    query_params = [user_id, ]
    get_post_list_specified_query = SELECT_POSTS_BY_USER
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        get_post_list_specified_query += '''AND post.date >= %s '''
        query_params.append(since_date)
    elif since_date == False and since_date is not None:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect since_date fromat'}))

    order = request.GET.get('order', 'desc')
    if order.lower() not in ('asc', 'desc'):
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect order parameter: {}'.format(order)}))
    
    get_post_list_specified_query += '''ORDER BY post.date ''' + order

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        get_post_list_specified_query += ''' LIMIT %s'''
        query_params.append(limit)

    try:
        post_list_qs = __cursor.execute(get_post_list_specified_query, query_params)
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)})) 
    
    posts = []

    for post in __cursor.fetchall():
        posts.append({
            "date": post[0].strftime("%Y-%m-%d %H:%M:%S") ,
            "dislikes": post[1],
            "forum": post[2],
            "id": post[3],
            "isApproved": post[4],
            "isDeleted": post[5],
            "isEdited": post[6],
            "isHighlighted": post[7],
            "isSpam": post[8],
            "likes": post[9],
            "message": post[10],
            "parent": post[11],
            "points": post[12],
            "thread": post[13],
            "user": post[14]
        })
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': posts})) 
