from django.conf.urls import include, url
from allviews import create, details, listPosts, listThreads, listUsers

urlpatterns = [
    url(r'^create/', create),
    url(r'^details/', details),
    url(r'^listPosts/', listPosts),
    url(r'^listThreads/', listThreads),
    url(r'^listUsers/', listUsers),
]
