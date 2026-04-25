"""
MongoDB connection helpers.

This module centralizes MongoDB client creation for the backend.
It also includes a fallback import path for `pymongo` because the
user's Django environment may not have the package installed while
the user-level Python site-packages already does.
"""

import logging
import os
import sys
from pathlib import Path

from django.conf import settings

logger = logging.getLogger(__name__)

try:
    MONGODB_URI = getattr(settings, 'MONGODB_URI', 'mongodb://127.0.0.1:27017/')
    MONGODB_DB_NAME = getattr(settings, 'MONGODB_DB_NAME', 'stock_manager_local')
except Exception:
    MONGODB_URI = 'mongodb://127.0.0.1:27017/'
    MONGODB_DB_NAME = 'stock_manager_local'

_client = None


def _import_mongo_client():
    try:
        from pymongo import MongoClient
        return MongoClient
    except ModuleNotFoundError:
        appdata = os.environ.get('APPDATA', '')
        python_root = Path(appdata) / 'Python'
        fallback_sites = []
        if python_root.exists():
            fallback_sites.extend(sorted(python_root.glob('Python*/site-packages')))

        preferred_site = Path(appdata) / 'Python' / f'Python{sys.version_info.major}{sys.version_info.minor}' / 'site-packages'
        ordered_sites = []
        if preferred_site.exists():
            ordered_sites.append(preferred_site)
        ordered_sites.extend(path for path in fallback_sites if path != preferred_site)

        for site_path in ordered_sites:
            pymongo_init = site_path / 'pymongo' / '__init__.py'
            if pymongo_init.exists() and str(site_path) not in sys.path:
                sys.path.append(str(site_path))
                break

        from pymongo import MongoClient
        return MongoClient


def get_mongodb_client():
    global _client
    if _client is None:
        try:
            MongoClient = _import_mongo_client()
            _client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
            _client.admin.command('ping')
            logger.info('MongoDB client connected successfully')
        except Exception as e:
            _client = None
            raise Exception(f'MongoDB unavailable: {str(e)}')
    return _client


def get_mongodb_db(db_name=None):
    if db_name is None:
        db_name = MONGODB_DB_NAME
    client = get_mongodb_client()
    return client[db_name]


def close_mongodb_connection():
    global _client
    if _client is not None:
        try:
            _client.close()
            logger.info('MongoDB connection closed')
        except Exception:
            pass
        finally:
            _client = None
