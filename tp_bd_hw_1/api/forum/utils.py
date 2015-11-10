from api.queries.select import SELECT_FORUM_BY_ID_FULL

def get_forum_by_id(cursor, forum_id):
    cursor.execute(SELECT_FORUM_BY_ID_FULL, [forum_id, ])
    forum = cursor.fetchone()
    return {"id": forum[0],
            "name": forum[1],
            "short_name": forum[2],
            "user": forum[3]
           }, \
           {
            "user": forum[4]
           }
