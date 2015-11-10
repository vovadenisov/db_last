from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_ALL_POSTS_BY_THREAD, SELECT_TOP_POST_NUMBER, SELECT_THREAD_BY_ID


def listPosts(request):
    __cursor = connection.cursor()
    if request.method != 'GET':
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    thread = request.GET.get('thread')
    thread_id = general_utils.validate_id(thread)
    if thread_id is None:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id is required'}))
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

    get_all_posts_specified_query = SELECT_ALL_POSTS_BY_THREAD
    query_params = [thread_id, ]
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        get_all_posts_specified_query += '''AND post.date >= %s '''
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
    
    get_all_posts_query_postfix = ''' ORDER BY post.{} ''' + order

    sort = request.GET.get('sort', 'flat')
    if sort.lower() not in ('flat', 'tree', 'parent_tree'):
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect sort parameter: {}'.format(sort)}))

    if sort == 'flat':
        get_all_posts_query_postfix = get_all_posts_query_postfix.format('date')
    else:
        get_all_posts_query_postfix = get_all_posts_query_postfix.format('hierarchy_id')

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        if sort == 'flat' or sort == 'tree':
            get_all_posts_specified_query += get_all_posts_query_postfix + ''' LIMIT %s'''
            query_params.append(limit)
        else:
            if order == 'asc':
                operation = '<='
            else:
                operation = '>='
                try:
                    max_posts_number_qs = __cursor.execute(SELECT_TOP_POST_NUMBER, [thread_id,]) 
                except DatabaseError as db_err: 
                    __cursor.close()
                    return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                                'response': unicode(db_err)}))
                if __cursor.rowcount:
                    max_posts_number = __cursor.fetchone()[0]
                else:
                    max_posts_number = 0     
                limit = max_posts_number - limit + 1
                if limit < 1:
                    limit = 1
            get_all_posts_specified_query += "AND post.hierarchy_id {} '{}' ".format(operation, limit) + \
                                              get_all_posts_query_postfix
    else:
        get_all_posts_specified_query += get_all_posts_query_postfix

    try:
        posts_qs = __cursor.execute(get_all_posts_specified_query, query_params) 
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
            "isApproved": not not post[4],
            "isDeleted": not not post[5],
            "isEdited": not not post[6],
            "isHighlighted": not not post[7],
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
                               'response': posts
                               }))
