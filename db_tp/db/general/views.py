from json import dumps

from django.db import connection
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes
from api.queries.delete import CLEAR_TABLE
from api.queries.alter import SET_AUTO_INCREMENT_BY_ZERO
from api.queries.select import SELECT_TABLE_STATUSES
from tp_bd_hw_1.settings import DATABASES


@csrf_exempt
def status(request):
    cursor = connection.cursor()
    base = DATABASES['default']['NAME']
    cursor.execute(SELECT_TABLE_STATUSES.format(base))
    statuses = {}
    for status_ in cursor.fetchall():
        statuses[status_[0]] = status_[1]
    cursor.close()
    return HttpResponse(dumps({"code": codes.OK, "response": statuses}))

@csrf_exempt
def clear(request):
    cursor = connection.cursor()
    
    for table in ['post_hierarchy_utils', 'followers', 'subscriptions',
                  'post', 'thread', 'forum', 'user']:
        cursor.execute(CLEAR_TABLE.format(table))
        cursor.execute(SET_AUTO_INCREMENT_BY_ZERO.format(table))
    result = {"code": codes.OK, "response": "OK"}
    cursor.close()
    return HttpResponse(dumps(result))
