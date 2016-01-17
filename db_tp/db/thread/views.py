from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.thread.utils import update_thread_posts, get_thread_by_id
from api.user.utils import get_user_by_id
from api.forum.utils import get_forum_by_id

from api.queries.select import SELECT_THREAD_BY_ID, SELECT_ROW_COUNT, SELECT_USER_BY_EMAIL, SELECT_FORUM_BY_SHORT_NAME, \
    SELECT_LAST_INSERT_ID, SELECT_THREADS_BY_FORUM_OR_USER, SELECT_ALL_POSTS_BY_THREAD, SELECT_TOP_POST_NUMBER
from api.queries.update import UPDATE_THREAD_DELETED_FLAG, UPDATE_THREAD_POSTS_DELETED_FLAG, UPDATE_THREAD_SET_IS_CLOSED_FLAG,\
    UPDATE_THREAD_SET_IS_CLOSED_FLAG, UPDATE_THREAD_VOTES, UPDATE_THREAD
from api.queries.insert import INSERT_THREAD, INSERT_SUBSCRIPTION
from api.queries.delete import DELETE_SUBSCRIPTION

@csrf_exempt
def vote(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))
    try:
        threadId = json_request['thread']
        vote = json_request['vote']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    threadId = general_utils.validate_id(threadId)
    if threadId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id should be int'}))
    try:
        vote = int(vote)
        if abs(vote) != 1:
            raise ValueError
    except ValueError:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect vote value'}))
    if vote < 0:
        column_name = 'dislikes'
    else:
        column_name = 'likes'

    try:
        cursor.execute(SELECT_THREAD_BY_ID, [threadId, ])
        if cursor.rowcount == 0:
            cursor.close()
            return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                       'response': 'thread not found'}))
        cursor.execute(UPDATE_THREAD_VOTES.format(column_name, column_name), [threadId, ])
        thread, related_obj = get_thread_by_id(cursor, threadId)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': thread}))


@csrf_exempt
def update(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))
    try:
        message = json_request['message']
        slug = json_request['slug']
        threadId = json_request['thread']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    threadId = general_utils.validate_id(threadId)
    if threadId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'}))
    try:
        cursor.execute(SELECT_THREAD_BY_ID, [threadId, ])
        if cursor.rowcount == 0:
            cursor.close()
            return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                       'response': 'thread not found'}))
        cursor.execute(UPDATE_THREAD, [unicode(message), unicode(slug), threadId, ])
        thread, related_obj = get_thread_by_id(cursor, threadId)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': thread}))

@csrf_exempt
def unsubscribe(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))

    try:
        email = json_request['user']
        thread = json_request['thread']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))

    # validate user
    try:
        cursor.execute(SELECT_USER_BY_EMAIL, [unicode(email), ])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                    'response': 'user with not found'}))
    userId = cursor.fetchone()[0]

    #validate thread
    threadId = general_utils.validate_id(thread)
    if threadId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'}))
    try:
       cursor.execute(SELECT_THREAD_BY_ID, [threadId,])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread was not found'}))
    threadId = cursor.fetchone()[0]

    try:
        cursor.execute(DELETE_SUBSCRIPTION, [threadId, userId])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': {"thread": threadId,
                                            "user": unicode(email)}}))

@csrf_exempt
def subscribe(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))

    try:
        email = json_request['user']
        thread = json_request['thread']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))

    #user validate
    try:
        cursor.execute(SELECT_USER_BY_EMAIL, [unicode(email), ])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                    'response': 'user not found'}))
    userId = cursor.fetchone()[0]

    # thread validate
    threadId = general_utils.validate_id(thread)
    if threadId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'}))
    try:
        cursor.execute(SELECT_THREAD_BY_ID, [threadId,])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread was not found'}))
    threadId = cursor.fetchone()[0]

    try:
        cursor.execute(INSERT_SUBSCRIPTION, [userId, threadId])
    except IntegrityError:
        pass
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': {"thread": threadId,
                                            "user": unicode(email)}}))

def listPosts(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    thread = request.GET.get('thread')
    threadId = general_utils.validate_id(thread)
    if threadId is None:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id is required'}))
    if threadId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'}))
    try:
        cursor.execute(SELECT_THREAD_BY_ID, [threadId,])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread was not found'}))
    threadId = cursor.fetchone()[0]

    all_posts_query = SELECT_ALL_POSTS_BY_THREAD
    query_params = [threadId, ]
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        all_posts_query += '''AND post.date >= %s '''
        query_params.append(since_date)
    elif since_date == False and since_date is not None:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect since_date fromat'}))

    order = request.GET.get('order', 'desc')
    if order.lower() not in ('asc', 'desc'):
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect order parameter: {}'.format(order)}))

    posts_query_postfix = ''' ORDER BY post.{} ''' + order

    sort = request.GET.get('sort', 'flat')
    if sort.lower() not in ('flat', 'tree', 'parent_tree'):
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect sort parameter: {}'.format(sort)}))

    if sort == 'flat':
        posts_query_postfix = posts_query_postfix.format('date')
    else:
        posts_query_postfix = posts_query_postfix.format('hierarchy_id')

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             cursor.close()
             return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        if sort == 'flat' or sort == 'tree':
            all_posts_query += posts_query_postfix + ''' LIMIT %s'''
            query_params.append(limit)
        else:
            if order == 'asc':
                operation = '<='
            else:
                operation = '>='
                try:
                    cursor.execute(SELECT_TOP_POST_NUMBER, [threadId,])
                except DatabaseError as db_err:
                    cursor.close()
                    return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                                'response': unicode(db_err)}))
                if cursor.rowcount:
                    max_posts_number = cursor.fetchone()[0]
                else:
                    max_posts_number = 0
                limit = max_posts_number - limit + 1
                if limit < 1:
                    limit = 1
            all_posts_query += "AND post.hierarchy_id {} '{}' ".format(operation, limit) + \
                                              posts_query_postfix
    else:
        all_posts_query += posts_query_postfix

    try:
        cursor.execute(all_posts_query, query_params)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    posts = []
    for post in cursor.fetchall():
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
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': posts
                               }))

def list_threads(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    short_name = request.GET.get('forum')
    email = request.GET.get('user')
    if short_name is None and email is None:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'email or forum short_name not found'}))
    if short_name and email:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'you should specify email OR forum name'}))
    if email:
        related_table_name = 'user'
        related_query = SELECT_USER_BY_EMAIL
        related_params = [email, ]
    else:
        related_table_name = 'forum'
        related_query = SELECT_FORUM_BY_SHORT_NAME
        related_params = [short_name, ]
    try:
        cursor.execute(related_query, related_params)
        if cursor.rowcount == 0:
            cursor.close()
            return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                       'response': '{} not found'.format(related_table_name)}))
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    relatedId = cursor.fetchone()[0]
    query_params = [relatedId, ]
    thread_list_query = SELECT_THREADS_BY_FORUM_OR_USER
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        thread_list_query = '''{}AND thread.date >= %s '''.format(thread_list_query)
        query_params.append(since_date)
    elif since_date == False and since_date is not None:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect since_date fromat'}))

    order = request.GET.get('order', 'desc')
    if order.lower() not in ('asc', 'desc'):
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect order parameter: {}'.format(order)}))

    thread_list_query = '''{0}ORDER BY thread.date {1}'''.format(thread_list_query, order)

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'limit should be int'}))
        thread_list_query = '''{0} LIMIT %s'''.format(thread_list_query)
        query_params.append(limit)

    try:
        cursor.execute(thread_list_query.format(related_table_name),
                                          query_params)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    threads = []
    for thread in cursor.fetchall():
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
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': threads}))

def details(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    threadId = general_utils.validate_id(request.GET.get('thread'))
    if threadId is None:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id not found'}))
    if threadId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'}))
    try:
        thread, relatedIds = get_thread_by_id(cursor, threadId)
    except TypeError:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'thread doesn\'t exist'}))
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))


    related = request.GET.getlist('related')

    related_functions_dict = {'user': get_user_by_id,
                              'forum': get_forum_by_id
                             }
    for related_ in related:
        if related_ in ['user', 'forum']:
            get_related_info_func = related_functions_dict[related_]
            thread[related_], relatedIds_ = get_related_info_func(cursor, relatedIds[related_])
        else:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect related parameter'}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': thread}))

@csrf_exempt
def create(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))

    try:
        message = json_request['message']
        title = json_request['title']
        is_closed = json_request['isClosed']
        forum = json_request['forum']
        date = json_request['date']
        email = json_request['user']
        slug = json_request['slug']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    try:
        cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'user not found'}))
    userId = cursor.fetchone()[0]

    #forum validate
    try:
        cursor.execute(SELECT_FORUM_BY_SHORT_NAME, [forum, ])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'forum not found'}))
    forumId = cursor.fetchone()[0]

    #message validate
    if not message:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'message should not be empty'}))

    #date validate
    date = general_utils.validate_date(date)
    if not date:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect date fromat'}))

    #validate slug
    if not title:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'title should not be empty'}))

    #slug validate
    if not slug:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'slug should not be empty'}))

    #validate is_closed
    is_closed = bool(is_closed)
    try:
        cursor.execute(INSERT_THREAD, [forumId, title, is_closed,
                                         userId, date, message, slug])
        cursor.execute(SELECT_LAST_INSERT_ID, [])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    threadId = cursor.fetchone()[0]

    is_deleted = json_request.get('isDeleted')
    if is_deleted is not None:
        is_deleted = bool(is_deleted)
        try:
            cursor.execute(UPDATE_THREAD_SET_DELETE_FLAG, [is_deleted, threadId])
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
    else:
        is_deleted = False
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': {
                                   "date": date,
                                   "forum": forum,
                                    "id": threadId,
                                    "isClosed": is_closed,
                                    "isDeleted": is_deleted,
                                    "message": message,
                                    "slug": slug,
                                    "title": title,
                                    "user": email
                                }}))

def change_deleted_flag_wrapper(deleted_flag):
    @csrf_exempt
    def change_deleted_flag(request):
        cursor = connection.cursor()
        try:
            json_request = loads(request.body)
        except ValueError as value_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                       'response': unicode(value_err)}))

        try:
            threadId = json_request['thread']
        except KeyError as key_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'Not found: {}'.format(unicode(key_err))}))
        try:
            cursor.execute(SELECT_THREAD_BY_ID, [threadId, ])
            if cursor.rowcount == 0:
                cursor.close()
                return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                           'response': 'thread not found'}))
            cursor.execute(UPDATE_THREAD_DELETED_FLAG.format(deleted_flag), [threadId, ])
            cursor.execute(UPDATE_THREAD_POSTS_DELETED_FLAG.format(deleted_flag), [threadId,])
            cursor.execute(SELECT_ROW_COUNT)
            posts_diff = cursor.fetchone()
            if posts_diff:
                posts_diff = posts_diff[0]
                if deleted_flag.upper() == 'TRUE':
                    posts_diff = -posts_diff
                update_thread_posts(cursor, threadId, posts_diff)
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
        cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': {
                                       'thread': threadId
                                   }}))
    return change_deleted_flag

def change_closed_flag_wrapper(close_flag):
    @csrf_exempt
    def change_closed_flag(request):
        cursor = connection.cursor()
        try:
            json_request = loads(request.body)
        except ValueError as value_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))
        try:
            threadId = json_request['thread']
        except KeyError as key_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

        try:
            cursor.execute(SELECT_THREAD_BY_ID, [threadId, ])
            if cursor.rowcount == 0:
                cursor.close()
                return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                       'response': 'thread not found'}))
            cursor.execute(UPDATE_THREAD_SET_IS_CLOSED_FLAG.format(close_flag), [threadId, ])
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

        cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': {
                                       'thread': threadId
                                   }}))
    return change_closed_flag

## OPEN
open_thread = change_closed_flag_wrapper('FALSE')
## REMOVE
remove = change_deleted_flag_wrapper('TRUE')
## RESTORE
restore = change_deleted_flag_wrapper('FALSE')

