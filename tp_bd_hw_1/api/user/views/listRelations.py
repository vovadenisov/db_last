from json import dumps

from django.db import connection, DatabaseError
from django.http import HttpResponse

from api.general import codes, utils as general_utils
from api.user.utils import get_user_by_id
from api.queries.select import SELECT_FOLLOW_RELATIONS, SELECT_USER_BY_EMAIL


def list_follow_relationship_wrapper(relationship):
    def list_follow_relationship(request):
        __cursor = connection.cursor()
        if request.method != 'GET':
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INVALID_QUERY,
                                   'response': 'request method should be GET'}))
        email = request.GET.get('user')
        # validate user
        try:
            __cursor.execute(SELECT_USER_BY_EMAIL, [email, ])
        except DatabaseError as db_err: 
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)}))

        if not  __cursor.rowcount:
             __cursor.close()
             return HttpResponse(dumps({'code': codes.NOT_FOUND,
                                   'response': 'user not found'}))
        user_id = __cursor.fetchone()[0]
        if relationship == 'follower':
             partner_relationship = 'following'
        else:
             partner_relationship = 'follower'    
        query = SELECT_FOLLOW_RELATIONS.format(relationship, partner_relationship)
        query_params = [user_id, ]
        since_id = general_utils.validate_id(request.GET.get('id'))
        if since_id:
            query += '''AND {}_id >= %s '''.format(relationship)
            query_params.append(since_id)
        elif since_id == False and since_id is not None:
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect since_id fromat'}))

        order = request.GET.get('order', 'desc')
        if order.lower() not in ('asc', 'desc'):
            __cursor.close()
            return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                       'response': 'incorrect order parameter: {}'.format(order)}))
    
        query += '''ORDER BY user.name ''' + order

        limit = request.GET.get('limit')
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                 __cursor.close()
                 return HttpResponse(dumps({'code': codes.INCORRECT_QUERY,
                                        'response': 'limit should be int'}))
            query += ''' LIMIT %s'''
            query_params.append(limit)

        try:
            __cursor.execute(query, query_params)
        except DatabaseError as db_err: 
            __cursor.close()
            return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                       'response': unicode(db_err)})) 
    
        followers = []
        for user_id in __cursor.fetchall():
            try:
                user, related_ids = get_user_by_id(__cursor, user_id[0])
            except DatabaseError as db_err: 
                __cursor.close()
                return HttpResponse(dumps({'code': codes.UNKNOWN_ERR,
                                           'response': unicode(db_err)}))  
            followers.append(user)
        __cursor.close()
        return HttpResponse(dumps({'code': codes.OK,
                                   'response': followers})) 
    return list_follow_relationship

