from json import dumps

from django.db import connection
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes
from api.queries.delete import CLEAR_TABLE
from api.queries.alter import RESET_AUTO_INCREMENT

@csrf_exempt
def clear(request):
    cursor = connection.cursor()
    for table in ['post_hierarchy_utils', 'followers', 'subscriptions',
                  'post', 'thread', 'forum', 'user']:
        cursor.execute(CLEAR_TABLE.format(table))
        cursor.execute(RESET_AUTO_INCREMENT.format(table))
    result = {"code": codes.OK,
              "response": "OK"
             }
    cursor.close()
    return HttpResponse(dumps(result))
