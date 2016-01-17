CLEAR_TABLE = '''TRUNCATE TABLE {}'''

DELETE_FOLLOWER = '''DELETE FROM followers
                     WHERE follower_id = %s
                     AND following_id = %s
                  '''

DELETE_SUBSCRIPTION = '''DELETE FROM subscriptions
                         WHERE thread_id = %s
                         AND user_id = %s
                      '''
                      

