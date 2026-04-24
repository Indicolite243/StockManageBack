"""
数据库连接工具模块
提供统一的MongoDB连接管理
"""

import logging
from django.conf import settings

logger = logging.getLogger(__name__)

# MongoDB连接配置
try:
    MONGODB_URI = getattr(settings, 'MONGODB_URI', 'mongodb://81.68.81.245:27017/mydatabase?authSource=admin')
    MONGODB_DB_NAME = getattr(settings, 'MONGODB_DB_NAME', 'admin')
except:
    MONGODB_URI = 'mongodb://81.68.81.245:27017/mydatabase?authSource=admin'
    MONGODB_DB_NAME = 'admin'

_client = None

def get_mongodb_client():
    global _client
    if _client is None:
        try:
            from pymongo import MongoClient
            _client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
            _client.admin.command('ping')
            logger.info('MongoDB客户端连接创建成功')
        except Exception as e:
            _client = None
            raise Exception(f"MongoDB不可用: {str(e)}")
    return _client

def get_mongodb_db(db_name=None):
    if db_name is None:
        db_name = MONGODB_DB_NAME
    try:
        client = get_mongodb_client()
        return client[db_name]
    except Exception as e:
        # 这里不记录error级别日志，避免干扰用户
        raise

def close_mongodb_connection():
    global _client
    if _client is not None:
        try:
            _client.close()
            logger.info('MongoDB连接已关闭')
        except:
            pass
        finally:
            _client = None
