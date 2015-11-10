from json import dumps

from django.db import connection, DatabaseError, IntegrityError, transaction
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_THREAD_BY_ID, SELECT_FORUM_BY_SHORT_NAME, SELECT_POSTS_BY_FORUM_OR_THREAD

                                
def list_posts(request):
    __cursor = connection.cursor()
    if request.method != 'GET':
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    thread_id = general_utils.validate_id(request.GET.get('thread'))
    forum = request.GET.get('forum')
    if thread_id is None and forum is None:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id or forum id not found'})) 
    if thread_id == False:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'}))
    if thread_id and forum:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'you should specify thread OR forum'}))
    if thread_id:
        related_table_name = 'thread'
        related_query = SELECT_THREAD_BY_ID 
        related_params = [thread_id, ]
    else:
        related_table_name = 'forum'
        related_query = SELECT_FORUM_BY_SHORT_NAME 
        related_params = [forum, ]     

    try:
        __cursor.execute(related_query, related_params) 
        if not __cursor.rowcount:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                       'response': '{} not found'.format(related_table_name)})) 
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    related_id = __cursor.fetchone()[0]
    query_params = [related_id, ]
    get_post_list_specified_query = SELECT_POSTS_BY_FORUM_OR_THREAD
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
        __cursor.execute(get_post_list_specified_query.format(related_table_name), 
                         query_params)
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
