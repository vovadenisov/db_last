UPDATE_THREAD = u'''UPDATE thread
                    SET message = %s,
                        slug = %s
                    WHERE id = %s;
                 '''

UPDATE_POST_SET_DELETE_FLAG = '''UPDATE post
                                 SET isDeleted = {}
                                 WHERE id = %s;
                               '''

UPDATE_POST_PREFIX = '''UPDATE post SET '''

UPDATE_CHILD_POST_COUNT = '''UPDATE post
                             SET child_posts_count = child_posts_count + 1
                             WHERE id = %s;
                          '''

UPDATE_POST_VOTES = '''UPDATE post
                       SET {} = {} + 1
                       WHERE id= %s
                    '''
UPDATE_THREAD_SET_IS_CLOSED_FLAG = u'''UPDATE thread
                                       SET isClosed = {}
                                       WHERE id = %s;
                                    '''

UPDATE_POST_NUMBER = '''UPDATE post_hierarchy_utils
                        SET head_posts_number = head_posts_number + 1
                        WHERE thread_id = %s;
                     '''

UPDATE_THREAD_DELETED_FLAG = '''UPDATE thread
                                SET isDeleted = {}
                                WHERE id = %s;
                             ''' 
UPDATE_POST_MESSAGE = u'''UPDATE post
                          SET message = %s
                          WHERE id = %s;
                       '''

UPDATE_USER = '''UPDATE user
                 SET about = %s,
                     name = %s
                 WHERE id = %s;
              '''

UPDATE_THREAD_POSTS_DELETED_FLAG = '''UPDATE post
                                      SET isDeleted = {}
                                      WHERE thread_id = %s
                                   '''

UPDATE_THREAD_SET_DELETE_FLAG = '''UPDATE thread
                                   SET isDeleted = %s
                                   WHERE id = %s
                                 '''

UPDATE_THREAD_POSTS = '''UPDATE thread
                         SET posts = posts + %s
                         WHERE id = %s
                      '''

UPDATE_THREAD_VOTES = '''UPDATE thread
                         SET {} = {} + 1
                         WHERE id = %s;
                      '''
                      
UPDATE_USER_ANONYMOUS_FLAG = '''UPDATE user SET isAnonymous = %s 
                                WHERE id = %s '''