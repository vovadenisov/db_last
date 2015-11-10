from api.queries.select import SELECT_THREAD_BY_ID_FULL 
from api.queries.update import UPDATE_THREAD_POSTS

def get_thread_by_id(cursor, thread_id):
    cursor.execute(SELECT_THREAD_BY_ID_FULL, [thread_id, ])#fetchone()
    thread = cursor.fetchone()
    return {"date": thread[0].strftime("%Y-%m-%d %H:%M:%S"),
            "dislikes": thread[1],
            "forum": thread[2],
            "id": thread[3],
            "isClosed": not not thread[4],
            "isDeleted": not not thread[5],
            "likes": thread[6],
            "message": thread[7],
            "points": thread[8],
            "posts": thread[9],
            "slug": thread[10],
            "title": thread[11],
            "user": thread[12]
           }, \
           {
           "forum": thread[13],
           "user": thread[14]
           }
           
  
def update_thread_posts(cursor, thread_id, posts_diff):
    try:
        cursor.execute(UPDATE_THREAD_POSTS, [posts_diff, thread_id])
    except Exception as e:
        print e
