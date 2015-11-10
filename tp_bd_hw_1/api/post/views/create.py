from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError, transaction
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes, utils as general_utils
from api.thread.utils import update_thread_posts
from api.post.utils import get_post_by_id
from api.queries.insert import INSERT_TOP_POST_NUMBER, INSERT_POST
from api.queries.select import SELECT_LAST_INSERT_ID, SELECT_USER_BY_EMAIL, SELECT_FORUM_BY_SHORT_NAME, \
                               SELECT_TOP_POST_NUMBER, SELECT_PARENT_POST_HIERARCHY, SELECT_THREAD_BY_ID
from api.queries.update import UPDATE_POST_PREFIX, UPDATE_POST_NUMBER, UPDATE_CHILD_POST_COUNT


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
    #print 'PARENT ID: ', parent_id
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
