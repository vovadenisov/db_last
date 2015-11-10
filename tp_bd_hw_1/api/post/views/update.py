from json import loads, dumps

from django.db import connection, DatabaseError
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes, utils as general_utils
from api.post.utils import get_post_by_id
from api.queries.select import SELECT_POST_BY_ID
from api.queries.update import UPDATE_POST_MESSAGE


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
