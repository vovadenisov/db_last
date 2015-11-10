from django.conf.urls import include, url
from allviews import close_thread, open_thread, create, details, list_threads, remove, restore, update, vote, listPosts, subscribe, unsubscribe

urlpatterns = [
    url(r'^close/', close_thread),
    url(r'^create/', create),
    url(r'^details/', details),
    url(r'^list/', list_threads),
    url(r'^listPosts/', listPosts),
    url(r'^open/', open_thread),
    url(r'^remove/', remove),
    url(r'^restore/', restore),
    url(r'^subscribe/', subscribe),
    url(r'^unsubscribe/', unsubscribe),
    url(r'^update/', update),
    url(r'^vote/', vote),
]
