from json import dumps, loads

from django.db import connection, DatabaseError, IntegrityError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.queries.select import SELECT_THREAD_BY_ID
from api.queries.update import UPDATE_THREAD_SET_IS_CLOSED_FLAG

def change_closed_flag_wrapper(close_flag):
    @csrf_exempt
    def change_closed_flag(request):
        __cursor = connection.cursor()
        try:
            json_request = loads(request.body)
        except ValueError as value_err:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': unicode(value_err)}))
        try:
            thread_id = json_request['thread']
        except KeyError as key_err:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                   'response': 'Not found: {}'.format(unicode(key_err))}))

        try:
            __cursor.execute(SELECT_THREAD_BY_ID, [thread_id, ])
            if not __cursor.rowcount:
                __cursor.close()
                return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                       'response': 'thread not found'}))
            __cursor.execute(UPDATE_THREAD_SET_IS_CLOSED_FLAG.format(close_flag), [thread_id, ])
        except DatabaseError as db_err:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

        __cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': {
                                       'thread': thread_id
                                   }}))
    return change_closed_flag


## OPEN
open_thread = change_closed_flag_wrapper('FALSE')
## REMOVE
remove = change_deleted_flag_wrapper('TRUE')
## RESTORE
restore = change_deleted_flag_wrapper('FALSE')

