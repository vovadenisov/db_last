from api.queries.select import SELECT_USER_BY_ID_FULL, SELECT_FOLLOWERS, SELECT_FOLLOWINGS, SELECT_SUBSCRIPTIONS

def get_user_by_id(cursor, user_id):
    cursor.execute(SELECT_USER_BY_ID_FULL, [user_id, ])
    user = cursor.fetchone()
    result_user = {"about": user[0],
                   "email": user[1], 
                   "followers": [],
                   "following": [],
                   "id": user[2],
                   "isAnonymous": not not user[3],
                   "name": user[4],
                   "subscriptions": [],
                   "username": user[5]
                   }
    cursor.execute(SELECT_FOLLOWERS, [user_id, ])
    if cursor.rowcount:
        result_user["followers"].extend([follower[0] for follower in cursor.fetchall()])
    cursor.execute(SELECT_FOLLOWINGS, [user_id, ])
    if cursor.rowcount:
        result_user["following"].extend([following[0] for following in cursor.fetchall()])
    cursor.execute(SELECT_SUBSCRIPTIONS, [user_id, ])
    if cursor.rowcount:
        result_user["subscriptions"].extend([subscription[0] for subscription in  cursor.fetchall()])
    return result_user, {}


