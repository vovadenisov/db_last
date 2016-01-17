from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes, utils as general_utils

from api.queries.select import SELECT_LAST_INSERT_ID, SELECT_USER_BY_EMAIL, SELECT_POSTS_BY_USER, SELECT_FOLLOW_RELATIONS
from api.queries.insert import INSERT_FOLLOWER, INSERT_USER
from api.queries.update import UPDATE_USER_ANONYMOUS_FLAG, UPDATE_USER

from api.user.utils import get_user_by_id


@csrf_exempt
def updateProfile(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))

    try:
        name = json_request['name']
        email = json_request['user']
        about = json_request['about']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))
    try:
        cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
        if cursor.rowcount == 0:
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                       'response': 'user does not exist'}))
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))

    userId = cursor.fetchone()[0]
    try:
        cursor.execute(UPDATE_USER, [about, name, userId])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))

    try:
        user, related_ids = get_user_by_id(cursor, userId)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))

def list_follow_relationship_wrapper(relationship):
    def list_follow_relationship(request):
        cursor = connection.cursor()
        if request.method != 'GET':
            cursor.close()
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
        email = request.GET.get('user')
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
        userId = cursor.fetchone()[0]
        if relationship == 'follower':
             partner_relationship = 'following'
        else:
             partner_relationship = 'follower'
        query = SELECT_FOLLOW_RELATIONS.format(relationship, partner_relationship)
        query_params = [userId, ]
        since_id = general_utils.validate_id(request.GET.get('id'))
        if since_id:
            query = '''{}AND {}_id >= %s '''.format(query, relationship)
            query_params.append(since_id)
        elif since_id == False and since_id is not None:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect since_id fromat'}))

        order = request.GET.get('order', 'desc')
        if order.lower() not in ('asc', 'desc'):
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect order parameter: {}'.format(order)}))

        query = '''{}ORDER BY user.name {}'''.format(query, order)

        limit = request.GET.get('limit')
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                 cursor.close()
                 return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
            query = '''{} LIMIT %s'''.format(query)
            query_params.append(limit)

        try:
            cursor.execute(query, query_params)
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

        followers = []
        for userId in cursor.fetchall():
            try:
                user, related_ids = get_user_by_id(cursor, userId[0])
            except DatabaseError as db_err:
                cursor.close()
                return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                           'response': unicode(db_err)}))
            followers.append(user)
        cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': followers}))
    return list_follow_relationship

def listPosts(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    email = request.GET.get('user')
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
    userId = cursor.fetchone()[0]
    query_params = [userId, ]
    post_list_query = SELECT_POSTS_BY_USER
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        post_list_query = '''{}AND post.date >= %s '''.format(post_list_query)
        query_params.append(since_date)
    elif since_date == False and since_date is not None:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect since_date fromat'}))

    order = request.GET.get('order', 'desc')
    if order not in ('asc', 'desc', 'ASC', 'DESC'):
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'incorrect order parameter: {}'.format(order)}))

    post_list_query = '''ORDER BY post.date '''.format(post_list_query, order)

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             cursor.close()
             return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        post_list_query = '''{} LIMIT %s'''.format(post_list_query)
        query_params.append(limit)

    try:
        cursor.execute(post_list_query, query_params)
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

@csrf_exempt
def follow(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))

    try:
        followee = json_request['followee']
        follower = json_request['follower']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))

    # validate users
    users = []
    for email in [unicode(follower), unicode(followee)]:
        try:
            cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

        if cursor.rowcount == 0:
             cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'user with not found'}))
        userId = cursor.fetchone()[0]
        users.append(userId)

    try:
        cursor.execute(INSERT_FOLLOWER, users)
    except IntegrityError:
        pass
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    try:
        user, related_ids = get_user_by_id(cursor, users[0])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))

def details(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    email = request.GET.get('user')
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
    userId = cursor.fetchone()[0]

    try:
        user, related_ids = get_user_by_id(cursor, userId)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))

@csrf_exempt
def create(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))

    try:
        name = json_request['name']
        about = json_request['about']
        email = json_request['email']
        username = json_request['username']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))

    try:
        cursor.execute(INSERT_USER, [username, about, name, email])
        cursor.execute(SELECT_LAST_INSERT_ID, [])
    except IntegrityError:
        cursor.close()
        return HttpResponse(dumps({'code': codes.USER_ALREADY_EXISTS,
                                   'response': 'user already exists'}))#'user already exists'}))

    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))
    userId = cursor.fetchone()[0]
    user = {"about": about,
            "email": email,
            "id": userId,
            "isAnonymous": False,
            "name": name,
            "username": username
             }
    try:
        isAnonymous = json_request['isAnonymous']
        if not isinstance(isAnonymous, bool):
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'isAnonymous flag should be bool'}))
    except KeyError:
        cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': user}))
    try:
        cursor.execute(UPDATE_USER_ANONYMOUS_FLAG, [isAnonymous, userId])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))
    user["isAnonymous"] = isAnonymous
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))

## LIST FOLLOWERS ##
listFollowers = list_follow_relationship_wrapper('follower')

## LIST FOLLOWINGS ##
listFollowings = list_follow_relationship_wrapper('following')

def list_follow_relationship_wrapper(relationship):
    def list_follow_relationship(request):
        cursor = connection.cursor()
        if request.method != 'GET':
            cursor.close()
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
        email = request.GET.get('user')
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
        userId = cursor.fetchone()[0]
        if relationship == 'follower':
             partner_relationship = 'following'
        else:
             partner_relationship = 'follower'
        query = SELECT_FOLLOW_RELATIONS.format(relationship, partner_relationship)
        query_params = [userId, ]
        since_id = general_utils.validate_id(request.GET.get('id'))
        if since_id:
            query += '''AND {}_id >= %s '''.format(relationship)
            query_params.append(since_id)
        elif since_id == False and since_id is not None:
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect since_id fromat'}))

        order = request.GET.get('order', 'desc')
        if order.lower() not in ('asc', 'desc'):
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect order parameter: {}'.format(order)}))

        query += '''ORDER BY user.name ''' + order

        limit = request.GET.get('limit')
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                 cursor.close()
                 return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
            query += ''' LIMIT %s'''
            query_params.append(limit)

        try:
            cursor.execute(query, query_params)
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

        followers = []
        for userId in cursor.fetchall():
            try:
                user, related_ids = get_user_by_id(cursor, userId[0])
            except DatabaseError as db_err:
                cursor.close()
                return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                           'response': unicode(db_err)}))
            followers.append(user)
        cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': followers}))
    return list_follow_relationship

def listPosts(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    email = request.GET.get('user')
    # validate user
    try:
        userId_qs = cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'user not found'}))
    userId = cursor.fetchone()[0]
    query_params = [userId, ]
    post_list_query = SELECT_POSTS_BY_USER
    since_date = general_utils.validate_date(request.GET.get('since'))
    if since_date:
        post_list_query += '''AND post.date >= %s '''
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

    post_list_query += '''ORDER BY post.date ''' + order

    limit = request.GET.get('limit')
    if limit:
        try:
            limit = int(limit)
        except ValueError:
             cursor.close()
             return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
        post_list_query += ''' LIMIT %s'''
        query_params.append(limit)

    try:
        post_list_qs = cursor.execute(post_list_query, query_params)
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

@csrf_exempt
def follow(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))

    try:
        follower = unicode(json_request['follower'])
        followee = unicode(json_request['followee'])
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))

    # validate users
    users = []
    for email in [follower, followee]:
        try:
            userId_qs = cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
        except DatabaseError as db_err:
            cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

        if cursor.rowcount == 0:
             cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                        'response': 'user with not found'}))
        userId = cursor.fetchone()[0]
        users.append(userId)

    try:
        cursor.execute(INSERT_FOLLOWER, users)
    except IntegrityError:
        pass
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    try:
        user, related_ids = get_user_by_id(cursor, users[0])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))

def details(request):
    cursor = connection.cursor()
    if request.method != 'GET':
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    email = request.GET.get('user')
    # validate user
    try:
        userId_qs = cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if cursor.rowcount == 0:
        cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'user not found'}))
    userId = cursor.fetchone()[0]

    try:
        user, related_ids = get_user_by_id(cursor, userId)
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))

@csrf_exempt
def create(request):
    cursor = connection.cursor()
    try:
        json_request = loads(request.body)
    except ValueError as value_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': str(value_err)}))

    try:
        username = json_request['username']
        about = json_request['about']
        name = json_request['name']
        email = json_request['email']
    except KeyError as key_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(str(key_err))}))

    try:
        cursor.execute(INSERT_USER, [username, about, name, email])
        cursor.execute(SELECT_LAST_INSERT_ID, [])
    except IntegrityError as i_err:
        cursor.close()
        #print i_err
        return HttpResponse(dumps({'code': codes.USER_ALREADY_EXISTS,
                                   'response': 'user already exists'}))#'user already exists'}))

    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))
    userId = cursor.fetchone()[0]
    user = {"about": about,
            "email": email,
            "id": userId,
            "isAnonymous": False,
            "name": name,
            "username": username
             }
    try:
        isAnonymous = json_request['isAnonymous']
        if not isinstance(isAnonymous, bool):
            cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'isAnonymous flag should be bool'}))
    except KeyError:
        cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': user}))
    try:
        cursor.execute(UPDATE_USER_ANONYMOUS_FLAG, [isAnonymous, userId])
    except DatabaseError as db_err:
        cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': str(db_err)}))
    user["isAnonymous"] = isAnonymous
    cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))

## LIST FOLLOWERS ##
listFollowers = list_follow_relationship_wrapper('follower')

## LIST FOLLOWINGS ##
listFollowings = list_follow_relationship_wrapper('following')