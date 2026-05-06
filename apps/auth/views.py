"""
本地账号认证接口。

当前系统使用本地 MongoDB 保存账号信息，提供基础的用户名/密码登录和注册能力。
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime

from django.contrib.auth.hashers import check_password, make_password
from rest_framework.decorators import api_view
from rest_framework.response import Response

from apps.utils.db import get_mongodb_db

logger = logging.getLogger(__name__)

USERS_COLLECTION = "local_users"
DEFAULT_TEST_USERNAME = "Indicolite"
DEFAULT_TEST_PASSWORD = "123456"


def get_users_collection():
    db = get_mongodb_db()
    collection = db[USERS_COLLECTION]
    collection.create_index("username", unique=True)
    return collection


def ensure_default_user():
    """确保测试账号存在。"""
    collection = get_users_collection()
    existing_user = collection.find_one({"username": DEFAULT_TEST_USERNAME})
    if existing_user:
        return existing_user

    now = datetime.utcnow().isoformat()
    document = {
        "username": DEFAULT_TEST_USERNAME,
        "password_hash": make_password(DEFAULT_TEST_PASSWORD),
        "display_name": DEFAULT_TEST_USERNAME,
        "created_at": now,
        "updated_at": now,
        "last_login": None,
        "status": "active",
    }
    collection.insert_one(document)
    logger.info("Initialized default local auth user: %s", DEFAULT_TEST_USERNAME)
    return collection.find_one({"username": DEFAULT_TEST_USERNAME})


def build_auth_response(user_document: dict, access_token: str):
    return {
        "success": True,
        "message": "登录成功",
        "data": {
            "user": {
                "username": user_document["username"],
                "display_name": user_document.get("display_name") or user_document["username"],
                "last_login": user_document.get("last_login"),
                "status": user_document.get("status", "active"),
            },
            "token": {
                "access_token": access_token,
                "token_type": "Bearer",
                "expires_in": 86400,
            },
        },
    }


def validate_username_and_password(username: str, password: str):
    if not username or not password:
        return "账号和密码不能为空"
    if len(username) < 3:
        return "账号长度至少 3 位"
    if len(password) < 6:
        return "密码长度至少 6 位"
    return None


@api_view(["POST"])
def register_user(request):
    """本地账号注册。"""
    try:
        ensure_default_user()
        username = str(request.data.get("username", "")).strip()
        password = str(request.data.get("password", "")).strip()
        confirm_password = str(request.data.get("confirm_password", "")).strip()

        validation_error = validate_username_and_password(username, password)
        if validation_error:
            return Response({"success": False, "message": validation_error}, status=400)

        if password != confirm_password:
            return Response({"success": False, "message": "两次输入的密码不一致"}, status=400)

        collection = get_users_collection()
        if collection.find_one({"username": username}):
            return Response({"success": False, "message": "该账号已存在"}, status=409)

        now = datetime.utcnow().isoformat()
        collection.insert_one(
            {
                "username": username,
                "password_hash": make_password(password),
                "display_name": username,
                "created_at": now,
                "updated_at": now,
                "last_login": None,
                "status": "active",
            }
        )

        logger.info("Registered local auth user: %s", username)
        return Response({"success": True, "message": "注册成功，请使用新账号登录"})
    except Exception as exc:
        logger.error("Local register failed: %s", exc, exc_info=True)
        return Response({"success": False, "message": f"注册失败: {str(exc)}"}, status=500)


@api_view(["POST"])
def local_login(request):
    """本地账号密码登录。"""
    try:
        ensure_default_user()
        username = str(request.data.get("username", "")).strip()
        password = str(request.data.get("password", "")).strip()

        validation_error = validate_username_and_password(username, password)
        if validation_error:
            return Response({"success": False, "message": validation_error}, status=400)

        collection = get_users_collection()
        user_document = collection.find_one({"username": username})
        if not user_document:
            return Response({"success": False, "message": "账号或密码错误"}, status=401)

        password_hash = user_document.get("password_hash", "")
        if not password_hash or not check_password(password, password_hash):
            return Response({"success": False, "message": "账号或密码错误"}, status=401)

        access_token = secrets.token_urlsafe(32)
        last_login = datetime.utcnow().isoformat()

        collection.update_one(
            {"_id": user_document["_id"]},
            {
                "$set": {
                    "last_login": last_login,
                    "updated_at": last_login,
                    "auth_token": access_token,
                }
            },
        )
        user_document["last_login"] = last_login

        return Response(build_auth_response(user_document, access_token))
    except Exception as exc:
        logger.error("Local login failed: %s", exc, exc_info=True)
        return Response({"success": False, "message": f"登录失败: {str(exc)}"}, status=500)


@api_view(["GET"])
def current_user(request):
    """根据 Bearer Token 返回当前用户信息。"""
    try:
        ensure_default_user()
        auth_header = request.headers.get("Authorization", "")
        token = auth_header.replace("Bearer ", "").strip()
        if not token:
            return Response({"success": False, "message": "未提供认证信息"}, status=401)

        collection = get_users_collection()
        user_document = collection.find_one({"auth_token": token})
        if not user_document:
            return Response({"success": False, "message": "认证已失效"}, status=401)

        return Response(
            {
                "success": True,
                "data": {
                    "username": user_document["username"],
                    "display_name": user_document.get("display_name") or user_document["username"],
                    "last_login": user_document.get("last_login"),
                    "status": user_document.get("status", "active"),
                },
            }
        )
    except Exception as exc:
        logger.error("Current user lookup failed: %s", exc, exc_info=True)
        return Response({"success": False, "message": "获取当前用户失败"}, status=500)


@api_view(["POST"])
def logout(request):
    """本地账号退出登录。"""
    try:
        auth_header = request.headers.get("Authorization", "")
        token = auth_header.replace("Bearer ", "").strip()
        if token:
            collection = get_users_collection()
            collection.update_one({"auth_token": token}, {"$unset": {"auth_token": ""}})
        return Response({"success": True, "message": "已退出登录"})
    except Exception as exc:
        logger.error("Logout failed: %s", exc, exc_info=True)
        return Response({"success": False, "message": "退出登录失败"}, status=500)
