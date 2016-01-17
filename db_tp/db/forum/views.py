from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_USER_BY_EMAIL, SELECT_LAST_INSERT_ID, SELECT_FORUM_BY_SHORT_NAME_FULL,\
    SELECT_ALL_POSTS_BY_FORUM, SELECT_FORUM_BY_SHORT_NAME, SELECT_ALL_THREADS_BY_FORUM, SELECT_ALL_THREADS_BY_FORUM
from api.queries.insert import INSERT_FORUM

from api.user.utils import get_user_by_id
from api.thread.utils import get_thread_by_id
from api.forum.utils import get_forum_by_id




def listUsers(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    short_name = request.GET.get('forum')
    if not short_name:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'forum name not correct'}))
    try:
        cursor.execute(SELECT_FORUM_BY_SHORT_NAME, [short_name, ])
        if cursor.rowcount == 0:
             cursor.close()
             return HttpResponse(dumps({'code': general_utils.NOT_FOUND,
                                        'response': 'forum not found'}))
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    forumId = cursor.fetchone()[0]

    all_forum_users_query = SELECT_USER_ID_BY_FORUM
    params = [forumId, ]
    since_id = general_utils.validate_id(request.GET.get('since_id'))
    if since_id:
        all_forum_users_query += '''AND user.id >= %s '''
        params.append(since_id)
    elif since_id == False and since_id is not None:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'since_id should be int'}))

    order = request.GET.get('order', 'desc')
    if order.lower() not in ('asc', 'desc'):
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'incorrect order parameter: {}'.format(order)}))

    all_forum_users_query += '''ORDER BY user.name ''' + order

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             cursor.close()
             return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        all_forum_users_query += ''' LIMIT %s'''
        params.append(limit)

    try:
        users_qs = cursor.execute(all_forum_users_query, params)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    users = []
    for user in cursor.fetchall():
        users.append(get_user_by_id(cursor, user[0])[0])
    cursor.close()
    return HttpResponse(dumps({'code': general_utils.OK,
                               'response': users
                               }))

def listThreads(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    short_name = request.GET.get('forum')
    if not short_name:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'forum name not found'}))
    try:
        cursor.execute(SELECT_FORUM_BY_SHORT_NAME, [short_name, ])
        if cursor.rowcount == 0:
             cursor.close()
             return HttpResponse(dumps({'code': general_utils.NOT_FOUND,
                                        'response': 'forum not found'}))
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    forumId = cursor.fetchone()[0]

    all_forum_threads_query = SELECT_ALL_THREADS_BY_FORUM
    params = [forumId, ]
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        all_forum_threads_query += '''AND date >= %s '''
        params.append(since_date)
    elif since_date == False and since_date is not None:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'incorrect since_date fromat'}))

    order = request.GET.get('order', 'desc')
    if order.lower() not in ('asc', 'desc'):
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'incorrect order parameter: {}'.format(order)}))

    all_forum_threads_query += '''ORDER BY thread.date ''' + order

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             cursor.close()
             return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        all_forum_threads_query += ''' LIMIT %s'''
        params.append(limit)

    try:
        threads_qs = cursor.execute(all_forum_threads_query, params)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    related = set(request.GET.getlist('related'))
    threads = []
    related_itemfunctions_dict = {'user': get_user_by_id,
                              'forum': get_forum_by_id,
                              }
    for thread_item in cursor.fetchall():
        threads.append({
            "date": thread_item[0].strftime("%Y-%m-%d %H:%M:%S"),
            "dislikes": thread_item[1],
            "forum": thread_item[2],
            "id": thread_item[3],
            "isClosed": thread_item[4],
            "isDeleted": thread_item[5],
            "likes": thread_item[6],
            "message": thread_item[7],
            "points": thread_item[8],
            "posts": thread_item[9],
            "slug": thread_item[10],
            "title": thread_item[11],
            "user": thread_item[12]
            })

        related_itemids = {'forum': thread_item[13],
                       'user': thread_item[14]
                       }
        if 'user' in related:
            get_related_iteminfo_func = related_itemfunctions_dict['user']
            threads[-1]['user'], related_itemids_ = get_related_iteminfo_func(cursor, related_itemids['user'])
        else:
            cursor.close()
            return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                       'response': 'incorrect related parameter'}))
        if 'forum' in related:
            get_related_iteminfo_func = related_itemfunctions_dict['forum']
            threads[-1]['forum'], related_itemids_ = get_related_iteminfo_func(cursor, related_itemids['forum'])
        else:
            cursor.close()
            return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                       'response': 'incorrect related parameter'}))

    cursor.close()
    return HttpResponse(dumps({'code': general_utils.OK,
                               'response': threads
                               }))

def listPosts(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    short_name = request.GET.get('forum')
    if not short_name:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'forum name not found'}))
    try:
        cursor.execute(SELECT_FORUM_BY_SHORT_NAME, [short_name, ])#.fetchone()
        if cursor.rowcount == 0:
             cursor.close()
             return HttpResponse(dumps({'code': general_utils.NOT_FOUND,
                                        'response': 'forum not found'}))
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    forumId = cursor.fetchone()[0]

    get_all_forum_posts_specified_query = SELECT_ALL_POSTS_BY_FORUM
    params = [forumId, ]
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        get_all_forum_posts_specified_query += '''AND post.date >= %s '''
        params.append(since_date)
    elif since_date == False and since_date is not None:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'incorrect since_date fromat'}))

    order = request.GET.get('order', 'desc')
    if order.lower() not in ('asc', 'desc'):
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'incorrect order parameter: {}'.format(order)}))

    get_all_forum_posts_specified_query += '''ORDER BY post.date ''' + order

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             cursor.close()
             return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        get_all_forum_posts_specified_query += ''' LIMIT %s'''
        params.append(limit)

    try:
        cursor.execute(get_all_forum_posts_specified_query, params)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    related = set(request.GET.getlist('related'))
    related_itemfunctions_dict = {'user': get_user_by_id,
                              'thread': get_thread_by_id,
                              'forum': get_forum_by_id
                              }
    posts = []
    for post_item in cursor.fetchall():
        post_items.append({
            "date": post_item[0].strftime("%Y-%m-%d %H:%M:%S"),
            "dislikes": post_item[1],
            "forum": post_item[2],
            "id": post_item[3],
            "isApproved": post_item[4],
            "isDeleted": post_item[5],
            "isEdited": post_item[6],
            "isHighlighted": post_item[7],
            "isSpam": post_item[8],
            "likes": post_item[9],
            "message": post_item[10],
            "parent": post_item[11],
            "points": post_item[12],
            "thread": post_item[13],
            "user": post_item[14]
            })

        related_itemids = {'forum': post_item[15],
                       'thread': post_item[16],
                       'user': post_item[17]
                       }

        for related_item in related:
            if related_item in ['thread', 'forum', 'user']:
                get_related_iteminfo_func = related_itemfunctions_dict[related_item]
                posts[-1][related_item], related_itemids_ = get_related_iteminfo_func(cursor, related_itemids[related_item])
            else:
                cursor.close()
                return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                           'response': 'incorrect related parameter'}))
    cursor.close()
    return HttpResponse(dumps({'code': general_utils.OK,
                               'response': posts
                               }))


def details(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    short_name = request.GET.get('forum')
    if not short_name:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INVALID_QUERY,
                                   'response': 'forum name not found'}))
    try:
        cursor.execute(SELECT_FORUM_BY_SHORT_NAME_FULL, [short_name, ])
        if cursor.rowcount == 0:
             cursor.close()
             return HttpResponse(dumps({'code': general_utils.NOT_FOUND,
                                        'response': 'forum not found'}))
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    forum = cursor.fetchone()
    response = {"id": forum[0],
                "name": forum[1],
                "short_name": forum[2]
               }

    related = request.GET.get('related')
    if related:
        if related != 'user':
            cursor.close()
            return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                       'response': 'incorrect related parameter: {}'.format(related)}))
        user_id = forum[4]
        try:
            user, related_itemids = get_user_by_id(cursor, user_id)
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
        response['user'] = user

    else:
        response["user"] = forum[3]
        cursor.close()
    return HttpResponse(dumps({'code': general_utils.OK,
                               'response': response}))



@csrf_exempt
def create(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INVALID_QUERY,
                                   'response': unicode(value_err)}))

    try:
        name = json_request['name']
        short_name = json_request['short_name']
        email = json_request['user']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    try:
        cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.NOT_FOUND,
                                   'response': 'user not found'}))

    user_id = cursor.fetchone()[0]
    try:
        cursor.execute(INSERT_FORUM, [name, short_name, user_id])
        cursor.execute(SELECT_LAST_INSERT_ID, [])
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.OK,
                                   'response': {
                                        'id': cursor.fetchone()[0],
                                        'name': name,
                                        'short_name': short_name,
                                        'user': email
                                         }}))
    except IntegrityError:
        cursor.execute(SELECT_FORUM_BY_SHORT_NAME_FULL, [short_name, ])
        forum = cursor.fetchone()
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.OK,
                                   'response': {
                                        'id': forum[0],
                                        'name': forum[1],
                                        'short_name': forum[2],
                                        'user': forum[3]
                                         }}))
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': general_utils.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
