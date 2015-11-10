from django.conf.urls import url
from allviews import clear, status

urlpatterns = [
    url(r'^clear/', clear),
    url(r'^status/', status),
]
