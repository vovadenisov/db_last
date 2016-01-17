"""tp_bd_hw_1 URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.8/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Add an import:  from blog import urls as blog_urls
    2. Add a URL to urlpatterns:  url(r'^blog/', include(blog_urls))
"""
from django.conf.urls import include, url
from api.forum import urls as forum_urls
from api.user import urls as user_urls
from api.post import urls as post_urls
from api.thread import urls as thread_urls
from api.general import urls as general_urls
#forum, user, post, thread, general

urlpatterns = [
    url(r'^db/api/forum/', include(forum_urls)),
    url(r'^db/api/user/', include(user_urls)),
    url(r'^db/api/post/', include(post_urls)),
    url(r'^db/api/thread/', include(thread_urls)),
    url(r'^db/api/', include(general_urls)),
]
