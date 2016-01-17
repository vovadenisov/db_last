from django.conf.urls import url
from views import create, details, list_posts, remove, restore, update, vote

urlpatterns = [
    url(r'^create/', create),
    url(r'^details/', details),
    url(r'^list/', list_posts),
    url(r'^remove/', remove),
    url(r'^restore/', restore),
    url(r'^update/', update),
    url(r'^vote/', vote),
]
