from api.queries.select import SELECT_POST_BY_ID_FULL

def get_post_by_id(cursor, post_id):
    post_qs = cursor.execute(SELECT_POST_BY_ID_FULL, [post_id,])
    post = cursor.fetchone()
    return {
        "date": post[0].strftime("%Y-%m-%d %H:%M:%S") ,
        "dislikes": post[1],
        "forum": post[2],
        "id": post[3],
        "isApproved": not not post[4],
        "isDeleted": not not post[5],
        "isEdited": not not post[6],
        "isHighlighted": not not post[7],
        "isSpam": not not post[8],
        "likes": post[9],
        "message": post[10],
        "parent": post[11],
        "points": post[12],
        "thread": post[13],
        "user": post[14]
        }, \
        {
         "forum": post[15],
         "thread": post[16],
         "user": post[17]
         }
