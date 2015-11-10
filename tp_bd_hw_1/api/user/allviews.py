## CREATE ##
from api.user.views.create import create

## DETAILS ##
from api.user.views.details import details

## FOLLOW ##
from api.user.views.follow import follow

from api.user.views.listRelations import list_follow_relationship_wrapper
## LIST FOLLOWERS ##
listFollowers = list_follow_relationship_wrapper('follower')

## LIST FOLLOWINGS ##
listFollowings = list_follow_relationship_wrapper('following')

## LIST POSTS ##
from api.user.views.listPosts import listPosts

## UNFOLLOW ##
from api.user.views.unfollow import unfollow

## UPDATE PROFILE ##
from api.user.views.updateProfile import updateProfile
