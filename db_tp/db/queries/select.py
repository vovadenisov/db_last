SELECT_LAST_INSERT_ID = '''
                        SELECT LAST_INSERT_ID();
                        '''

SELECT_FORUM_BY_SHORT_NAME_FULL = u'''SELECT forum.id, forum.name, forum.short_name, user.email, user.id
                                      FROM forum INNER JOIN user
                                      ON forum.user_id = user.id
                                      WHERE forum.short_name = %s;
                                   '''
SELECT_FORUM_BY_ID_FULL = u'''SELECT forum.id, forum.name, forum.short_name, user.email, user.id
                                      FROM forum INNER JOIN user
                                      ON forum.user_id = user.id
                                      WHERE forum.id = %s;
                                   '''
                                   
SELECT_FORUM_BY_SHORT_NAME = u'''SELECT forum.id
                                 FROM forum 
                                 WHERE forum.short_name = %s;
                              '''                                  

SELECT_ALL_POSTS_BY_FORUM = '''SELECT post.date, post.dislikes, forum.short_name,
                                      post.id, post.isApproved, post.isDeleted, post.isEdited,
                                      post.isHighlighted, post.isSpam, post.likes, post.message, post.parent_id,
                                      post.likes - post.dislikes as points, post.thread_id, user.email,
                                      forum.id, thread_id, user.id
                                FROM post INNER JOIN forum ON post.forum_id = forum.id
                                INNER JOIN user ON user.id = post.user_id
                                WHERE post.forum_id = %s
                            '''

SELECT_ROW_COUNT = '''SELECT ROW_COUNT()
                   '''

SELECT_USER_BY_EMAIL = '''SELECT id FROM user
                          WHERE email = %s;
                       '''

SELECT_USER_ID_BY_FORUM = '''SELECT DISTINCT user_id
                             FROM post JOIN user ON user.id = post.user_id
                             WHERE post.forum_id = %s
                          '''


SELECT_ALL_THREADS_BY_FORUM = '''SELECT thread.date, thread.dislikes, forum.short_name,
                                        thread.id, thread.isClosed, thread.isDeleted, 
                                        thread.likes, thread.message,
                                        thread.likes - thread.dislikes as points, posts, 
                                        thread.slug, thread.title, user.email,
                                        forum.id,  user.id
                                 FROM thread INNER JOIN forum ON thread.forum_id = forum.id
                                 INNER JOIN user ON user.id = thread.user_id
                                 WHERE thread.forum_id = %s
                              '''

SELECT_TABLE_STATUSES = '''SELECT table_name, table_rows
                           FROM INFORMATION_SCHEMA.TABLES 
                           WHERE TABLE_SCHEMA = '{}'
                           AND table_name IN ('user', 'thread', 'forum', 'post')
                        '''
                        
SELECT_THREAD_BY_ID = '''SELECT id FROM thread
                         WHERE id = %s;
                      '''
                      
SELECT_PARENT_POST_HIERARCHY = '''SELECT id, child_posts_count, hierarchy_id FROM post
                                  WHERE id = %s;
                               '''
                               
SELECT_TOP_POST_NUMBER = '''
                            SELECT head_posts_number
                            FROM post_hierarchy_utils
                            WHERE thread_id = %s; 
                        '''

SELECT_POST_BY_ID =  '''
                        SELECT id
                        FROM post
                        WHERE id = %s;
                     '''

SELECT_POSTS_BY_FORUM_OR_THREAD = '''SELECT post.date, post.dislikes, forum.short_name, post.id,
                                            post.isApproved, post.isDeleted, post.isEdited, post.isHighlighted, 
                                            post.isSpam, post.likes, post.message, post.parent_id, 
                                            post.likes - post.dislikes as points, post.thread_id, user.email
                                     FROM post INNER JOIN user
                                               ON post.user_id = user.id
                                           INNER JOIN forum on forum.id = post.forum_id
                                     WHERE post.{}_id = %s 
                                  '''
                     
SELECT_THREADS_BY_FORUM_OR_USER = '''SELECT thread.date, thread.dislikes, forum.short_name,
                                            thread.id, thread.isClosed, thread.isDeleted, 
                                            thread.likes, thread.message,
                                            thread.likes - thread.dislikes as points, posts, 
                                            thread.slug, thread.title, user.email,
                                            forum.id,  user.id
                                     FROM thread INNER JOIN forum ON thread.forum_id = forum.id
                                     INNER JOIN user ON user.id = thread.user_id
                                     WHERE thread.{}_id = %s
                                  '''

SELECT_ALL_POSTS_BY_THREAD = '''SELECT post.date, post.dislikes, forum.short_name,
                                      post.id, post.isApproved, post.isDeleted, post.isEdited,
                                      post.isHighlighted, post.isSpam, post.likes, post.message, post.parent_id,
                                      post.likes - post.dislikes as points, post.thread_id, user.email,
                                      forum.id, post.thread_id, user.id,
                                      post.hierarchy_id
                                FROM post INNER JOIN forum ON post.forum_id = forum.id
                                INNER JOIN user ON user.id = post.user_id
                                WHERE post.thread_id = %s
                            '''

SELECT_FOLLOWINGS = '''SELECT email
                       FROM followers INNER JOIN user
                       ON followers.following_id = user.id
                       WHERE follower_id = %s
                    '''


SELECT_FOLLOW_RELATIONS = '''SELECT user.id
                             FROM followers JOIN user ON user.id = {}_id
                             WHERE {}_id = %s 
                           '''
                           
SELECT_POSTS_BY_USER = '''SELECT post.date, post.dislikes, forum.short_name,
                                 post.id, post.isApproved, post.isDeleted,
                                 post.isEdited, post.isHighlighted, post.isSpam,
                                 post.likes, post.message, post.parent_id,
                                 post.likes - post.dislikes as points, post.thread_id, 
                                 user.email
                          FROM post JOIN user ON post.user_id = user.id
                               JOIN forum ON forum.id = post.forum_id
                          WHERE post.user_id = %s '''
                          
SELECT_POST_BY_ID_FULL = ''' SELECT post.date, post.dislikes, forum.short_name, post.id,
                                    post.isApproved, post.isDeleted, post.isEdited, post.isHighlighted, 
                                    post.isSpam, post.likes, post.message, post.parent_id, 
                                    post.likes - post.dislikes as points, post.thread_id, user.email,
                                    post.forum_id, post.thread_id, post.user_id
                             FROM post INNER JOIN user
                             ON post.user_id = user.id
                             INNER JOIN forum on forum.id = post.forum_id
                             WHERE post.id = %s;
                        '''
                        
SELECT_THREAD_BY_POST_ID = '''SELECT thread_id
                                   FROM post
                                   WHERE id = %s
                                '''

                        
SELECT_USER_BY_ID_FULL = '''SELECT about, email, id, isAnonymous, name, username
                            FROM user
                            WHERE id = %s
                         '''

SELECT_FOLLOWERS = '''SELECT email
                      FROM followers INNER JOIN user
                      ON followers.follower_id = user.id
                      WHERE following_id = %s
                   '''

SELECT_THREAD_BY_ID_FULL = '''SELECT thread.date, thread.dislikes, forum.short_name, thread.id,
                                   thread.isClosed, thread.isDeleted, thread.likes, thread.message,
                                   thread.likes - thread.dislikes as points, posts,
                                   thread.slug, thread.title, user.email,
                                   forum.id, user.id

                            FROM thread INNER JOIN user
                            ON thread.user_id = user.id
                            INNER JOIN forum ON forum.id = thread.forum_id
                            WHERE thread.id = %s;
                        '''

SELECT_SUBSCRIPTIONS = '''SELECT thread_id
                          FROM subscriptions
                          WHERE user_id = %s
                       '''