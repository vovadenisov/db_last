__author__ = 'usr'
from json import loads, dumps

from django.http import HttpResponse
from api.queries.select import SELECT_THREAD_BY_POST_ID, SELECT_LAST_INSERT_ID, SELECT_USER_BY_EMAIL, SELECT_FORUM_BY_SHORT_NAME, \
SELECT_TOP_POST_NUMBER, SELECT_PARENT_POST_HIERARCHY, SELECT_THREAD_BY_ID, SELECT_POSTS_BY_FORUM_OR_THREAD, SELECT_POST_BY_ID
from api.queries.update import UPDATE_POST_SET_DELETE_FLAG, UPDATE_POST_PREFIX, UPDATE_POST_NUMBER, UPDATE_CHILD_POST_COUNT, \
    UPDATE_POST_VOTES, SELECT_POST_BY_ID
from api.queries.insert import INSERT_TOP_POST_NUMBER, INSERT_POST
from json import dumps, loads
from django.db import connection, DatabaseError, IntegrityError, transaction
from django.views.decorators.csrf import csrf_exempt
from api.general import codes, utils as general_utils
from api.thread.utils import update_thread_posts, get_thread_by_id
from api.post.utils import get_post_by_id
from api.forum.utils import get_forum_by_id



@csrf_exempt
def vote(request):
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))
    try:
        post_id = json_request['post']
        vote_ = json_request['vote']
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    post_id = general_utils.validate_id(post_id)
    if post_id == False:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id should be int'}))
    try:
        vote_ = int(vote_)
        if abs(vote_) != 1:
            raise ValueError
    except ValueError:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect vote value'}))
    if vote_ < 0:
        column_name = 'dislikes'
    else:
        column_name = 'likes'

    try:
        __cursor.execute(SELECT_POST_BY_ID, [post_id, ])
        if not __cursor.rowcount:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'post not found'}))
        __cursor.execute(UPDATE_POST_VOTES.format(column_name, column_name), [post_id, ])
        if not __cursor.rowcount:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'post not found'}))
        post, related_obj = get_post_by_id(__cursor, post_id)
    except DatabaseError as db_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': post}))



@csrf_exempt
def update(request):
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))
    try:
        post_id = json_request['post']
        message = unicode(json_request['message'])
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

    post_id = general_utils.validate_id(post_id)
    if post_id == False:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id should be int'}))
    try:
        __cursor.execute(SELECT_POST_BY_ID, [post_id, ])
        if not __cursor.rowcount:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'post not found'}))
        post_id_qs = __cursor.execute(UPDATE_POST_MESSAGE, [message, post_id])
        post, related_obj = get_post_by_id(__cursor, post_id)
    except DatabaseError as db_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': post}))


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

def details(request):
    __cursor = connection.cursor()
    if request.method != 'GET':
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    post_id = general_utils.validate_id(request.GET.get('post'))
    if post_id is None:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id not found'}))
    if post_id == False:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'post id should be int'}))
    try:
        post, related_ids = get_post_by_id(__cursor, post_id)
    except DatabaseError as db_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    except TypeError:
        __cursor.close()
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
            post[related_], related_ids_ = get_related_info_func(__cursor, related_ids[related_])
        else:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect related parameter'}))
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': post}))


@csrf_exempt
def create(request):
  try:
    __cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))

    try:
        date = json_request['date']
        thread_id = json_request['thread']
        message = json_request['message']
        forum = json_request['forum']
        email = json_request['user']
    except KeyError as key_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))
    # validate user
    try:
        __cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
    except DatabaseError as db_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if not __cursor.rowcount:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'user not found'}))
    user_id = __cursor.fetchone()[0]

    # validate forum
    try:
        __cursor.execute(SELECT_FORUM_BY_SHORT_NAME, [forum, ])
    except DatabaseError as db_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    if not __cursor.rowcount:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'forum not found'}))
    forum_id = __cursor.fetchone()[0]

    #validate thread
    try:
        __cursor.execute(SELECT_THREAD_BY_ID, [thread_id, ])
    except DatabaseError as db_err:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    if not __cursor.rowcount:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'thread not found'}))
    thread_id = __cursor.fetchone()[0]

    #validate date
    date = general_utils.validate_date(date)
    if not date:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect date fromat'}))
    #validate message
    if not message:
        __cursor.close()
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
               __cursor.close()
               return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                          'response': 'optional flag should be bool'}))
           query_params.append([optional_arg_name, optional_arg_value])

    parent_id = json_request.get('parent')

    with transaction.atomic():
        if parent_id:
            try:
                __cursor.execute(SELECT_PARENT_POST_HIERARCHY, [parent_id, ])
                if not __cursor.rowcount:
                     __cursor.close()
                     return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                                'response': 'parent post not found'}))
                post = __cursor.fetchone()
                __cursor.execute(UPDATE_CHILD_POST_COUNT, [parent_id, ])
            except DatabaseError as db_err:
                __cursor.close()
                return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                           'response': unicode(db_err)}))

            hierarchy_id = post[2] + unicode(post[1] + 1) + '/'
        else:
            try:
                __cursor.execute(SELECT_TOP_POST_NUMBER, [thread_id, ])
                if not __cursor.rowcount:
                     __cursor.execute(INSERT_TOP_POST_NUMBER, [thread_id,])
                     post_number = 1
                else:
                     post_number = __cursor.fetchone()[0] + 1
                     __cursor.execute(UPDATE_POST_NUMBER, [thread_id,])
            except DatabaseError as db_err:
                __cursor.close()
                return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                           'response': unicode(db_err)}))
            hierarchy_id = unicode(post_number) + '/'

        try:
            post_qs = __cursor.execute(INSERT_POST, [hierarchy_id, date, message,
                                                     user_id, forum_id, thread_id, parent_id])
            __cursor.execute(SELECT_LAST_INSERT_ID, [])
        except DatabaseError as db_err:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
        post_id = __cursor.fetchone()[0]

    update_post_query = UPDATE_POST_PREFIX
    if query_params:
        update_post_query += ", ".join([query_param[0] + '= %s' for query_param in query_params]) + \
                             ''' WHERE id = %s'''
        try:
            __cursor.execute(update_post_query, [query_param[1] for query_param in query_params] + [post_id,])
        except DatabaseError as db_err:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

    try:
         post, realted_ids = get_post_by_id(__cursor, post_id)
    except TypeError:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': post}))
    if not post['isDeleted']:
        update_thread_posts(__cursor, thread_id, 1)
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': post}))
  except Exception as e:
    print e


def change_delete_flag_wrapper(delete_flag):
    @csrf_exempt
    def change_delete_flag(request):
      try:
        __cursor = connection.cursor()
        try:
            json_request = loads(request.body)
        except ValueError as value_err:
           __cursor.close()
           return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                      'response': unicode(value_err)}))
        try:
            post_id = json_request['post']
        except KeyError as key_err:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'Not found: {}'.format(unicode(key_err))}))
        post_id = general_utils.validate_id(post_id)
        if post_id == False:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                       'response': 'post id should be int'}))
        try:
            __cursor.execute(SELECT_THREAD_BY_POST_ID, [post_id, ])
            if not __cursor.rowcount:
                 __cursor.close()
                 return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                            'response': 'post not found'}))
            thread_id = __cursor.fetchone()[0]
            if delete_flag.upper() == 'TRUE':
                posts_diff = -1
            else:
                posts_diff = 1
            update_thread_posts(__cursor, thread_id, posts_diff)
            __cursor.execute(UPDATE_POST_SET_DELETE_FLAG.format(delete_flag), [post_id, ])
        except DatabaseError as db_err:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
        __cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': {"post": post_id}}))
      except Exception as e:
        print e
    return change_delete_flag

