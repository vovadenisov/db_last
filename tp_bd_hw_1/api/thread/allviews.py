from api.thread.views.change_closed_flag import change_closed_flag_wrapper
## CLOSE ##
close_thread = change_closed_flag_wrapper('TRUE')

## CREATE 
from api.thread.views.create import create

## DETAILS
from api.thread.views.details import details

## List threads
from api.thread.views.list import list_threads

 
## LIST POSTS ##
from api.thread.views.listPosts import listPosts

## OPEN
open_thread = change_closed_flag_wrapper('FALSE')

from api.thread.views.change_deleted_flag import change_deleted_flag_wrapper
## REMOVE
remove = change_deleted_flag_wrapper('TRUE')
## RESTORE
restore = change_deleted_flag_wrapper('FALSE')

## SUBSCRIBE
from api.thread.views.subscribe import subscribe

## UNSUBSCRIBE
from api.thread.views.unsubscribe import unsubscribe

## UPDATE ##
from api.thread.views.update import update

## VOTE ##
from api.thread.views.vote import vote
