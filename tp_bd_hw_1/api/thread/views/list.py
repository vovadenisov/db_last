from json import dumps

from django.db import connection, DatabaseError, IntegrityError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_THREADS_BY_FORUM_OR_USER, SELECT_USER_BY_EMAIL, SELECT_FORUM_BY_SHORT_NAME


def list_threads(request):
    __cursor = connection.cursor()
    if request.method != 'GET':
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    short_name = request.GET.get('forum')
    email = request.GET.get('user')
    if short_name is None and email is None:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id or forum id not found'})) 
    if short_name and email:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'you should specify thread OR forum'}))
    if email:
        related_table_name = 'user'
        related_query = SELECT_USER_BY_EMAIL
        related_params = [email, ]
    else:
        related_table_name = 'forum'
        related_query = SELECT_FORUM_BY_SHORT_NAME 
        related_params = [short_name, ]     
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
    get_thread_list_specified_query = SELECT_THREADS_BY_FORUM_OR_USER
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        get_thread_list_specified_query += '''AND thread.date >= %s '''
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
    
    get_thread_list_specified_query += '''ORDER BY thread.date ''' + order

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'limit should be int'}))
        get_thread_list_specified_query += ''' LIMIT %s'''
        query_params.append(limit)

    try:
        threads_qs = __cursor.execute(get_thread_list_specified_query.format(related_table_name), 
                                          query_params)
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)})) 
    
    threads = []
    for thread in __cursor.fetchall():
        threads.append({
            "date": thread[0].strftime("%Y-%m-%d %H:%M:%S") ,
            "dislikes": thread[1],
            "forum": thread[2],
            "id": thread[3],
            "isClosed": not not thread[4],
            "isDeleted": not not thread[5],
            "likes": thread[6],
            "message": thread[7],
            "points": thread[8],
            "posts": thread[9], 
            "slug": thread[10],
            "title": thread[11],
            "user": thread[12]
            })
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': threads}))
