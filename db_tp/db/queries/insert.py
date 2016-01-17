INSERT_FORUM = u'''INSERT INTO forum
                   (name, short_name, user_id)
                   VALUES
                   (%s, %s, %s);
               '''

INSERT_SUBSCRIPTION = '''INSERT INTO subscriptions
                         (user_id, thread_id)
                         VALUES
                         (%s, %s)
                      '''

INSERT_USER = u'''INSERT INTO user
                  (username, about, name, email)
                  VALUES
                  (%s, %s, %s, %s);
               '''


INSERT_POST = u'''INSERT INTO post 
                  (hierarchy_id, date, message, user_id, forum_id, thread_id, parent_id)
                  VALUES
                  (%s, %s, %s, %s, %s, %s, %s);
               '''

INSERT_TOP_POST_NUMBER = u'''INSERT INTO post_hierarchy_utils
                             (thread_id, head_posts_number)
                             VALUES
                             (%s, 1);
                          '''

INSERT_THREAD = '''INSERT INTO thread
                   (forum_id, title, isClosed, user_id, date, message, slug)
                   VALUES
                   (%s, %s, %s, %s, %s, %s, %s)
                '''


INSERT_FOLLOWER = '''INSERT INTO followers
                     (follower_id, following_id)
                     VALUES
                     (%s, %s)
                  '''
