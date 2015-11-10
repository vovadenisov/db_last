from json import dumps, loads

from django.db import connection, DatabaseError
from django.views.decorators.csrf import csrf_exempt
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.thread.utils import update_thread_posts
from api.queries.update import UPDATE_THREAD_DELETED_FLAG, UPDATE_THREAD_POSTS_DELETED_FLAG
from api.queries.select import SELECT_THREAD_BY_ID, SELECT_ROW_COUNT

def change_deleted_flag_wrapper(deleted_flag):
    @csrf_exempt
    def change_deleted_flag(request):
      try:
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
            __cursor.execute(UPDATE_THREAD_DELETED_FLAG.format(deleted_flag), [thread_id, ]) 
            __cursor.execute(UPDATE_THREAD_POSTS_DELETED_FLAG.format(deleted_flag), [thread_id,]) 
            __cursor.execute(SELECT_ROW_COUNT)
            posts_diff = __cursor.fetchone()
            if posts_diff:
                print posts_diff
                posts_diff = posts_diff[0]
                if deleted_flag.upper() == 'TRUE':
                    posts_diff = -posts_diff
                update_thread_posts(__cursor, thread_id, posts_diff)
        except DatabaseError as db_err: 
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))
        __cursor.close() 
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': {
                                       'thread': thread_id
                                    }})) 
      except Exception as e:
        print e
    return change_deleted_flag

 
