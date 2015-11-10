from json import dumps, loads

from django.db import connection, DatabaseError
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.user.utils import get_user_by_id
from api.queries.select import SELECT_USER_BY_EMAIL


def details(request):
    __cursor = connection.cursor()
    if request.method != 'GET':
        __cursor.close()
        return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
    email = request.GET.get('user')
    # validate user
    try:
        user_id_qs = __cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))

    if not __cursor.rowcount:
        __cursor.close()
        return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'user not found'}))
    user_id = __cursor.fetchone()[0]

    try:
        user, related_ids = get_user_by_id(__cursor, user_id)
    except DatabaseError as db_err: 
        __cursor.close()
        return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                   'response': unicode(db_err)}))
    __cursor.close()
    return HttpResponse(dumps({'code': codes.OK,
                               'response': user}))    

