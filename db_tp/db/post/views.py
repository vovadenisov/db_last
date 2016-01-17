__author__ = 'usr'
from json import loads, dumps

from django.http import HttpResponse
from api.queries.select import SELECT_THREAD_BY_postId, SELECT_LAST_INSERT_ID, SELECT_USER_BY_EMAIL, SELECT_FORUM_BY_SHORT_NAME, \
SELECT_TOP_POST_NUMBER, SELECT_PARENT_POST_HIERARCHY, SELECT_THREAD_BY_ID, SELECT_POSTS_BY_FORUM_OR_THREAD, SELECT_POST_BY_ID
from api.queries.update import UPDATE_POST_SET_DELETE_FLAG, UPDATE_POST_PREFIX, UPDATE_POST_NUMBER, UPDATE_CHILD_POST_COUNT, \
    UPDATE_POST_VOTES, SELECT_POST_BY_ID
from api.queries.insert import INSERT_TOP_POST_NUMBER, INSERT_POST
from json import dumps, loads
from django.db import connection, DatabaseError, IntegrityError, transaction
from django.views.decorators.csrf import csrf_exempt
from api.general import codes, utils as codes_and_utils
from api.thread.utils import update_thread_posts, get_thread_by_id
from api.post.utils import get_post_by_id
from api.forum.utils import get_forum_by_id

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
        postId = json_request['post']
        vote = json_request['vote']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    postId = codes_and_utils.validate_id(postId)
    if postId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id should be int'}))
    vote = int(vote)
    if abs(vote) != 1:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY, 'response': 'incorrect vote value'}))

    if vote < 0:
        column_name = 'dislikes'
    else:
        column_name = 'likes'

    try:
        cursor.execute(SELECT_POST_BY_ID, [postId, ])
        if cursor.rowcount == 0:
             cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'post not found'}))
        cursor.execute(UPDATE_POST_VOTES.format(column_name, column_name), [postId, ])
        if cursor.rowcount == 0:
            cursor.close()
            return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'post not found'}))
        post, related_obj = get_post_by_id(cursor, postId)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': post}))

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
        postId = json_request['post']
        message = unicode(json_request['message'])
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    postId = codes_and_utils.validate_id(postId)
    if postId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id should be int'}))
    try:
        cursor.execute(SELECT_POST_BY_ID, [postId, ])
        if cursor.rowcount == 0:
             cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'post not found'}))
        postId_qs = cursor.execute(UPDATE_POST_MESSAGE, [message, postId])
        post, related_obj = get_post_by_id(cursor, postId)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': post}))


def list_posts(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    threadId = codes_and_utils.validate_id(request.GET.get('thread'))
    forum = request.GET.get('forum')
    if threadId is None and forum is None:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id or forum id not found'}))
    if threadId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'thread id should be int'}))
    if threadId and forum:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'you should specify thread OR forum'}))
    if not threadId:
        related_table_name = 'forum'
        related_query = SELECT_FORUM_BY_SHORT_NAME
        related_params = [forum, ]
    else:
        related_table_name = 'thread'
        related_query = SELECT_THREAD_BY_ID
        related_params = [threadId, ]

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
    related_id = cursor.fetchone()[0]
    query_params = [related_id, ]
    get_post_list_specified_query = SELECT_POSTS_BY_FORUM_OR_THREAD
    since_date = codes_and_utils.validate_date(request.GET.get('since'))
    if since_date:
        get_post_list_specified_query += '''AND post.date >= %s '''
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

    get_post_list_specified_query += '''ORDER BY post.date ''' + order

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             cursor.close()
             return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        get_post_list_specified_query += ''' LIMIT %s'''
        query_params.append(limit)

    try:
        cursor.execute(get_post_list_specified_query.format(related_table_name),
                         query_params)
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
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': posts}))

def details(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    postId = codes_and_utils.validate_id(request.GET.get('post'))
    if postId is None:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id not found'}))
    if postId == False:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id should be int'}))
    try:
        post, related_ids = get_post_by_id(cursor, postId)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    except TypeError:
        cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'post not found'}))
    related = request.GET.getlist('related')
    related_functions_dict = {
                          'user': get_user_by_id,
                          'thread': get_thread_by_id,
                          'forum': get_forum_by_id
                          }
    for related_ in related:
        if related_ in ['user', 'forum', 'thread']:
            get_related_info_func = related_functions_dict[related_]
            post[related_], related_ids_ = get_related_info_func(cursor, related_ids[related_])
        else:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect related parameter'}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': post}))


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
        date = json_request['date']
        threadId = json_request['thread']
        message = json_request['message']
        forum = json_request['forum']
        email = json_request['user']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))
    # validate user
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
    user_id = cursor.fetchone()[0]

    # validate forum
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
    forum_id = cursor.fetchone()[0]

    #validate thread
    try:
        cursor.execute(SELECT_THREAD_BY_ID, [threadId, ])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'thread not found'}))
    threadId = cursor.fetchone()[0]

    #validate date
    date = codes_and_utils.validate_date(date)
    if not date:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect date fromat'}))
    #validate message
    if not message:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'message should not be empty'}))

    #validate optional args
    query_params = []
    optional_args = ['isApproved', 'isDeleted', 'isEdited', 'isHighlighted', 'isSpam']
    for optional_arg_name in optional_args:
       optional_arg_value = json_request.get(optional_arg_name)
       if optional_arg_value is not None:
           #print optional_arg_name, optional_arg_value
           if not isinstance(optional_arg_value, bool):
               cursor.close()
               return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                          'response': 'optional flag should be bool'}))
           query_params.append([optional_arg_name, optional_arg_value])

    parentId = json_request.get('parent')

    with transaction.atomic():
        if parentId:
            try:
                cursor.execute(SELECT_PARENT_POST_HIERARCHY, [parentId, ])
                if cursor.rowcount == 0:
                     cursor.close()
                     return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                                'response': 'parent post not found'}))
                post = cursor.fetchone()
                cursor.execute(UPDATE_CHILD_POST_COUNT, [parentId, ])
            except DatabaseError as db_err:
                cursor.close()
                return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                           'response': unicode(db_err)}))

            hierarchyId = '{0}{1}/'.format(post[2], unicode(post[1] + 1))
        else:
            try:
                cursor.execute(SELECT_TOP_POST_NUMBER, [threadId, ])
                if cursor.rowcount == 0:
                     cursor.execute(INSERT_TOP_POST_NUMBER, [threadId,])
                     post_number = 1
                else:
                     post_number = cursor.fetchone()[0] + 1
                     cursor.execute(UPDATE_POST_NUMBER, [threadId,])
            except DatabaseError as db_err:
                cursor.close()
                return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                           'response': unicode(db_err)}))
            hierarchyId = '{}/'.format(unicode(post_number))

        try:
            post_qs = cursor.execute(INSERT_POST, [hierarchyId, date, message,
                                                     user_id, forum_id, threadId, parentId])
            cursor.execute(SELECT_LAST_INSERT_ID, [])
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
        postId = cursor.fetchone()[0]

    update_post_query = UPDATE_POST_PREFIX
    if query_params:
        update_post_query += ", ".join([query_param[0] + '= %s' for query_param in query_params]) + \
                             ''' WHERE id = %s'''
        try:
            cursor.execute(update_post_query, [query_param[1] for query_param in query_params] + [postId,])
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

    try:
         post, realted_ids = get_post_by_id(cursor, postId)
    except TypeError:
        cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': post}))
    if not post['isDeleted']:
        update_thread_posts(cursor, threadId, 1)
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': post}))


def change_delete_flag(delete_flag):
    @csrf_exempt
    def change_delete_flag(request):
        cursor = connection.cursor()
        try:
            json_request = loads(request.body)
        except ValueError as value_err:
           cursor.close()
           return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                      'response': unicode(value_err)}))
        try:
            postId = json_request['post']
        except KeyError as key_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'Not found: {}'.format(unicode(key_err))}))
        postId = codes_and_utils.validate_id(postId)
        if postId == False:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                       'response': 'post id should be int'}))
        try:
            cursor.execute(SELECT_THREAD_BY_postId, [postId, ])
            if cursor.rowcount == 0:
                 cursor.close()
                 return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                            'response': 'post not found'}))
            threadId = cursor.fetchone()[0]
            if delete_flag.upper() == 'TRUE':
                posts_diff = -1
            else:
                posts_diff = 1
            update_thread_posts(cursor, threadId, posts_diff)
            cursor.execute(UPDATE_POST_SET_DELETE_FLAG.format(delete_flag), [postId, ])
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
        cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': {"post": postId}}))

    return change_delete_flag

remove = change_delete_flag('TRUE')

restore = change_delete_flag('FALSE')