from django.conf.urls import url
from views import clear, status

urlpatterns = [
    url(r'^clear/', clear),
    url(r'^status/', status),
]
