from json import dumps

from django.db import connection
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from api.general import codes
from api.queries.select import SELECT_TABLE_STATUSES
from tp_bd_hw_1.settings import DATABASES


@csrf_exempt
def status(request):
    __cursor = connection.cursor()
    db_name = DATABASES['default']['NAME']
    __cursor.execute(SELECT_TABLE_STATUSES.format(db_name))
    statuses = {}
    for status_ in __cursor.fetchall():
        statuses[status_[0]] = status_[1]
    __cursor.close()
    return HttpResponse(dumps({
                          "code": codes.OK,
                          "response": statuses
                           }))
