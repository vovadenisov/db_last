from django.conf.urls import url
from views import create, details, follow, listFollowers, listFollowings, listPosts, unfollow, updateProfile

urlpatterns = [
    url(r'^create/', create),
    url(r'^details/', details),
    url(r'^follow/', follow),
    url(r'^listFollowers/', listFollowers),
    url(r'^listFollowing/', listFollowings),
    url(r'^listPosts/', listPosts),
    url(r'^unfollow/', unfollow),
    url(r'^updateProfile/', updateProfile),
]
