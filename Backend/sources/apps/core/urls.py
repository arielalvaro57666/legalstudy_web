from django.urls import path, include
from rest_framework import routers

from apps.app import app_urls
from apps.core.auth_views import AuthViewSet
router = routers.DefaultRouter()

router.register("auth", AuthViewSet, basename='auth')

urlpatterns = [
    path('', include('apps.app.app_urls'))
]

urlpatterns += router.urls