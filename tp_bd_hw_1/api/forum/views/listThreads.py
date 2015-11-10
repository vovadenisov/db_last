from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_ALL_THREADS_BY_FORUM, SELECT_FORUM_BY_SHORT_NAME
from api.user.utils import get_user_by_id
from api.thread.utils import get_thread_by_id
from api.forum.utils import get_forum_by_id


def listThreads(request):
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
        __cursor.execute(SELECT_FORUM_BY_SHORT_NAME, [short_name, ])
        if not __cursor.rowcount:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'forum not found'}))
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    forum_id = __cursor.fetchone()[0]

    get_all_forum_threads_specified_query = SELECT_ALL_THREADS_BY_FORUM
    query_params = [forum_id, ]
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        get_all_forum_threads_specified_query += '''AND date >= %s '''
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
    
    get_all_forum_threads_specified_query += '''ORDER BY thread.date ''' + order

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        get_all_forum_threads_specified_query += ''' LIMIT %s'''
        query_params.append(limit)

    try:
        threads_qs = __cursor.execute(get_all_forum_threads_specified_query, query_params) 
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)})) 
    
    related = set(request.GET.getlist('related'))
    threads = []
    related_functions_dict = {'user': get_user_by_id,
                              'forum': get_forum_by_id,
                              } 
    for thread in __cursor.fetchall():
        threads.append({
            "date": thread[0].strftime("%Y-%m-%d %H:%M:%S"),
            "dislikes": thread[1],
            "forum": thread[2],
            "id": thread[3],
            "isClosed": thread[4],
            "isDeleted": thread[5],
            "likes": thread[6],
            "message": thread[7],
            "points": thread[8],
            "posts": thread[9], 
            "slug": thread[10],
            "title": thread[11],
            "user": thread[12]
            })

        related_ids = {'forum': thread[13],
                       'user': thread[14]
                       }

        for related_ in  related:
            if related_ in ['user', 'forum']:
                get_related_info_func = related_functions_dict[related_]
                threads[-1][related_], related_ids_ = get_related_info_func(__cursor, related_ids[related_])  
            else:
                __cursor.close()
                return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                           'response': 'incorrect related parameter'}))     
    __cursor.close()        
    return HttpResponse(dumps({'code': codes.OK,
                               'response': threads
                               }))
