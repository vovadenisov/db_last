CLEAR_TABLE = '''DELETE FROM {}'''

DELETE_SUBSCRIPTION = '''DELETE FROM subscriptions
                         WHERE thread_id = %s
                         AND user_id = %s
                      '''
                      
DELETE_FOLLOWER = '''DELETE FROM followers
                     WHERE follower_id = %s
                     AND following_id = %s    
                  '''
