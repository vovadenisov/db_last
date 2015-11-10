## CREATE ##
from api.post.views.create import create

## DETAILS ##
from api.post.views.details import details

## LIST ##
from api.post.views.list import list_posts

from api.post.views.change_delete_flag import change_delete_flag_wrapper
## REMOVE ##
remove = change_delete_flag_wrapper('TRUE')
## RESTORE ##
restore = change_delete_flag_wrapper('FALSE')

## UPDATE ##
from api.post.views.update import update

## VOTE ##
from api.post.views.vote import vote
