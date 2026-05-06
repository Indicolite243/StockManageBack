"""本地账号认证路由。"""

from django.urls import path

from .views import current_user, local_login, logout, register_user

urlpatterns = [
    path("login/", local_login, name="local_login"),
    path("register/", register_user, name="register_user"),
    path("profile/", current_user, name="current_user"),
    path("logout/", logout, name="logout"),
]
