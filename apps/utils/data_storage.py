"""
MongoDB-backed account snapshot storage.

Roles:
1. `latest_account_state` stores the freshest snapshot for each account.
2. `account_snapshots` stores throttled historical snapshots for analytics.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from django.conf import settings

from apps.utils.db import get_mongodb_db

logger = logging.getLogger(__name__)

SNAPSHOT_INTERVAL_SECONDS = int(getattr(settings, 'ACCOUNT_SNAPSHOT_INTERVAL_SECONDS', 60))
SNAPSHOT_RETENTION_DAYS = int(getattr(settings, 'ACCOUNT_SNAPSHOT_RETENTION_DAYS', 90))
HIGH_FREQ_INTERVAL_SECONDS = int(getattr(settings, 'ACCOUNT_SNAPSHOT_HIGH_FREQ_INTERVAL_SECONDS', 30))
HIGH_FREQ_RETENTION_DAYS = int(getattr(settings, 'ACCOUNT_SNAPSHOT_HIGH_FREQ_RETENTION_DAYS', 7))
MEDIUM_FREQ_INTERVAL_SECONDS = int(getattr(settings, 'ACCOUNT_SNAPSHOT_MEDIUM_FREQ_INTERVAL_SECONDS', 300))
MEDIUM_FREQ_RETENTION_DAYS = int(getattr(settings, 'ACCOUNT_SNAPSHOT_MEDIUM_FREQ_RETENTION_DAYS', 30))
LOW_FREQ_INTERVAL_SECONDS = int(getattr(settings, 'ACCOUNT_SNAPSHOT_LOW_FREQ_INTERVAL_SECONDS', 43200))
LOW_FREQ_RETENTION_DAYS = int(getattr(settings, 'ACCOUNT_SNAPSHOT_LOW_FREQ_RETENTION_DAYS', 3650))

_indexes_initialized = False
_last_prune_at = None


def _now() -> datetime:
    return datetime.now()


def _snapshot_collection():
    db = get_mongodb_db()
    return db.account_snapshots


def _high_freq_collection():
    db = get_mongodb_db()
    return db.account_snapshots_highfreq


def _daily_collection():
    db = get_mongodb_db()
    return db.account_snapshots_daily


def _latest_collection():
    db = get_mongodb_db()
    return db.latest_account_state


def _base_snapshot_query(include_debug: bool = False) -> Dict[str, Any]:
    if include_debug:
        return {}
    return {'data_source': {'$ne': 'debug_test'}}


def ensure_snapshot_storage_ready() -> None:
    global _indexes_initialized
    if _indexes_initialized:
        return

    latest = _latest_collection()
    high_freq = _high_freq_collection()
    snapshots = _snapshot_collection()
    daily = _daily_collection()

    latest.create_index('account_id', unique=True, name='latest_account_id_unique')
    latest.create_index([('timestamp', -1)], name='latest_timestamp_desc')

    high_freq.create_index([('account_id', 1), ('timestamp', -1)], name='highfreq_account_timestamp_desc')
    high_freq.create_index([('account_id', 1), ('date', 1)], name='highfreq_account_date')
    high_freq.create_index([('timestamp', -1)], name='highfreq_timestamp_desc')

    snapshots.create_index([('account_id', 1), ('timestamp', -1)], name='history_account_timestamp_desc')
    snapshots.create_index([('account_id', 1), ('date', 1)], name='history_account_date')
    snapshots.create_index([('timestamp', -1)], name='history_timestamp_desc')

    daily.create_index([('account_id', 1), ('timestamp', -1)], name='daily_account_timestamp_desc')
    daily.create_index([('account_id', 1), ('date', 1)], name='daily_account_date')
    daily.create_index([('timestamp', -1)], name='daily_timestamp_desc')

    _indexes_initialized = True


def prune_old_snapshots() -> None:
    global _last_prune_at
    now = _now()
    if _last_prune_at and (now - _last_prune_at).total_seconds() < 600:
        return

    retention_plan = [
        (_high_freq_collection(), HIGH_FREQ_RETENTION_DAYS),
        (_snapshot_collection(), MEDIUM_FREQ_RETENTION_DAYS),
        (_daily_collection(), LOW_FREQ_RETENTION_DAYS),
    ]
    for collection, retention_days in retention_plan:
        cutoff = now - timedelta(days=retention_days)
        collection.delete_many({'timestamp': {'$lt': cutoff}})
    _last_prune_at = now


def _normalize_positions(positions: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    normalized = []
    for position in positions or []:
        normalized.append({
            'stock_code': str(position.get('stock_code', '')),
            'stock_name': str(position.get('stock_name', '')),
            'volume': int(position.get('volume', 0) or 0),
            'can_use_volume': int(position.get('can_use_volume', 0) or 0),
            'current_price': float(position.get('current_price', position.get('open_price', 0)) or 0),
            'open_price': float(position.get('open_price', position.get('current_price', 0)) or 0),
            'avg_price': float(position.get('avg_price', position.get('cost_price', 0)) or 0),
            'cost_price': float(position.get('cost_price', position.get('avg_price', 0)) or 0),
            'market_value': float(position.get('market_value', 0) or 0),
            'frozen_volume': int(position.get('frozen_volume', 0) or 0),
            'on_road_volume': int(position.get('on_road_volume', 0) or 0),
            'yesterday_volume': int(position.get('yesterday_volume', 0) or 0),
        })
    return normalized


def _build_snapshot(account_id: str, account_data: Dict[str, Any], source: str = 'qmt_live') -> Dict[str, Any]:
    now = _now()
    positions = _normalize_positions(account_data.get('positions'))
    return {
        'account_id': str(account_id),
        'date': now.date().isoformat(),
        'timestamp': now,
        'snapshot_time': now.isoformat(timespec='seconds'),
        'data_source': source,
        'total_asset': float(account_data.get('total_asset', 0) or 0),
        'market_value': float(account_data.get('market_value', 0) or 0),
        'cash': float(account_data.get('cash', 0) or 0),
        'frozen_cash': float(account_data.get('frozen_cash', 0) or 0),
        'positions': positions,
    }


def _build_high_freq_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    positions = sorted(
        _normalize_positions(snapshot.get('positions')),
        key=lambda item: float(item.get('market_value', 0) or 0),
        reverse=True,
    )[:10]
    return {
        'account_id': snapshot['account_id'],
        'date': snapshot['date'],
        'timestamp': snapshot['timestamp'],
        'snapshot_time': snapshot['snapshot_time'],
        'data_source': snapshot['data_source'],
        'total_asset': snapshot['total_asset'],
        'market_value': snapshot['market_value'],
        'cash': snapshot['cash'],
        'frozen_cash': snapshot['frozen_cash'],
        'major_positions': positions,
    }


def _build_daily_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    positions = _normalize_positions(snapshot.get('positions'))
    return {
        'account_id': snapshot['account_id'],
        'date': snapshot['date'],
        'timestamp': snapshot['timestamp'],
        'snapshot_time': snapshot['snapshot_time'],
        'data_source': snapshot['data_source'],
        'total_asset': snapshot['total_asset'],
        'market_value': snapshot['market_value'],
        'cash': snapshot['cash'],
        'frozen_cash': snapshot['frozen_cash'],
        'positions': positions,
        'position_count': len(positions),
    }


def _should_insert_snapshot(collection, account_id: str, min_interval_seconds: int) -> bool:
    if min_interval_seconds <= 0:
        return True
    last_snapshot = collection.find_one({'account_id': str(account_id)}, sort=[('timestamp', -1)])
    if not last_snapshot or not last_snapshot.get('timestamp'):
        return True

    last_ts = last_snapshot['timestamp']
    if not isinstance(last_ts, datetime):
        return True

    return (_now() - last_ts).total_seconds() >= min_interval_seconds


def save_account_snapshot(account_id: str, account_data: Dict[str, Any], source: str = 'qmt_live') -> bool:
    try:
        ensure_snapshot_storage_ready()
        prune_old_snapshots()
        snapshot = _build_snapshot(account_id, account_data, source=source)
        latest_collection = _latest_collection()
        latest_collection.update_one(
            {'account_id': snapshot['account_id']},
            {'$set': snapshot},
            upsert=True,
        )

        high_freq_collection = _high_freq_collection()
        medium_freq_collection = _snapshot_collection()
        low_freq_collection = _daily_collection()

        if _should_insert_snapshot(high_freq_collection, snapshot['account_id'], HIGH_FREQ_INTERVAL_SECONDS):
            high_freq_collection.insert_one(_build_high_freq_snapshot(snapshot))

        if _should_insert_snapshot(medium_freq_collection, snapshot['account_id'], MEDIUM_FREQ_INTERVAL_SECONDS):
            medium_freq_collection.insert_one(snapshot)
            logger.info('账户 %s 中频历史快照已写入 MongoDB', account_id)
        else:
            logger.debug('账户 %s 距离上次中频快照不足 %s 秒，本次仅更新 latest_account_state', account_id, MEDIUM_FREQ_INTERVAL_SECONDS)

        if _should_insert_snapshot(low_freq_collection, snapshot['account_id'], LOW_FREQ_INTERVAL_SECONDS):
            low_freq_collection.insert_one(_build_daily_snapshot(snapshot))

        return True
    except Exception as e:
        logger.error('保存账户快照失败: %s', str(e), exc_info=True)
        return False


def _serialize_snapshot(snapshot: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not snapshot:
        return None
    return {
        'account_id': str(snapshot.get('account_id', '')),
        'date': snapshot.get('date'),
        'snapshot_time': snapshot.get('snapshot_time') or (
            snapshot.get('timestamp').isoformat(timespec='seconds') if isinstance(snapshot.get('timestamp'), datetime) else None
        ),
        'data_source': snapshot.get('data_source', 'mongodb_cache'),
        'total_asset': float(snapshot.get('total_asset', 0) or 0),
        'market_value': float(snapshot.get('market_value', 0) or 0),
        'cash': float(snapshot.get('cash', 0) or 0),
        'frozen_cash': float(snapshot.get('frozen_cash', 0) or 0),
        'positions': _normalize_positions(snapshot.get('positions')),
    }


def get_latest_account_state(account_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    try:
        ensure_snapshot_storage_ready()
        query = _base_snapshot_query()
        if account_id:
            query['account_id'] = str(account_id)
        snapshot = _latest_collection().find_one(query, sort=[('timestamp', -1)])
        return _serialize_snapshot(snapshot)
    except Exception as e:
        logger.error('读取最新账户快照失败: %s', str(e), exc_info=True)
        return None


def get_all_latest_account_states() -> List[Dict[str, Any]]:
    try:
        ensure_snapshot_storage_ready()
        snapshots = _latest_collection().find(_base_snapshot_query()).sort('account_id', 1)
        return [_serialize_snapshot(snapshot) for snapshot in snapshots]
    except Exception as e:
        logger.error('读取全部最新账户快照失败: %s', str(e), exc_info=True)
        return []


def get_account_history(account_id, days=30, start_date=None, end_date=None):
    try:
        ensure_snapshot_storage_ready()
        query: Dict[str, Any] = {'account_id': str(account_id)}

        if start_date or end_date:
            date_query: Dict[str, str] = {}
            if start_date:
                if isinstance(start_date, str):
                    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                date_query['$gte'] = start_date.isoformat()
            if end_date:
                if isinstance(end_date, str):
                    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                date_query['$lte'] = end_date.isoformat()
            if date_query:
                query['date'] = date_query
        else:
            end_date = _now().date()
            start_date = end_date - timedelta(days=days)
            query['date'] = {'$gte': start_date.isoformat(), '$lte': end_date.isoformat()}

        snapshots = _snapshot_collection().find(query).sort('date', 1)
        history = []
        for snapshot in snapshots:
            history.append({
                'date': snapshot['date'],
                'total_assets': float(snapshot.get('total_asset', 0) or 0),
                'market_value': float(snapshot.get('market_value', 0) or 0),
                'cash': float(snapshot.get('cash', 0) or 0),
                'snapshot_time': snapshot.get('snapshot_time') or (
                    snapshot.get('timestamp').isoformat(timespec='seconds') if isinstance(snapshot.get('timestamp'), datetime) else None
                ),
            })
        return history
    except Exception as e:
        logger.error('获取账户历史数据失败: %s', str(e), exc_info=True)
        return []


def get_account_snapshot_by_date(account_id, target_date):
    try:
        ensure_snapshot_storage_ready()
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date, '%Y-%m-%d').date()

        snapshot = _snapshot_collection().find_one({
            'account_id': str(account_id),
            'date': target_date.isoformat()
        })
        return _serialize_snapshot(snapshot)
    except Exception as e:
        logger.error('获取账户日期快照失败: %s', str(e), exc_info=True)
        return None


def get_yearly_data(account_id, start_year=None, end_year=None):
    try:
        query: Dict[str, Any] = {'account_id': str(account_id)}
        if start_year or end_year:
            date_query: Dict[str, str] = {}
            if start_year:
                date_query['$gte'] = f'{start_year}-01-01'
            if end_year:
                date_query['$lte'] = f'{end_year}-12-31'
            if date_query:
                query['date'] = date_query

        snapshots = _snapshot_collection().find(query).sort('date', 1)
        yearly_data: Dict[str, Dict[str, List[float]]] = {}
        for snapshot in snapshots:
            year = snapshot['date'][:4]
            yearly_data.setdefault(year, {'totalAssets': [], 'marketValues': []})
            yearly_data[year]['totalAssets'].append(float(snapshot.get('total_asset', 0) or 0))
            yearly_data[year]['marketValues'].append(float(snapshot.get('market_value', 0) or 0))

        result = {}
        for year, data in yearly_data.items():
            if not data['totalAssets']:
                continue
            end_assets = data['totalAssets'][-1]
            start_assets = data['totalAssets'][0]
            avg_assets = sum(data['totalAssets']) / len(data['totalAssets'])
            avg_market_value = sum(data['marketValues']) / len(data['marketValues']) if data['marketValues'] else 0
            return_rate = ((end_assets - start_assets) / start_assets * 100) if start_assets > 0 else 0
            investment_rate = (avg_market_value / avg_assets * 100) if avg_assets > 0 else 0
            result[year] = {
                'totalAssets': round(end_assets, 2),
                'returnRate': round(return_rate, 2),
                'investmentRate': round(investment_rate, 2)
            }
        return result
    except Exception as e:
        logger.error('获取年度数据失败: %s', str(e), exc_info=True)
        return {}


def get_weekly_data(account_id, weeks=4):
    try:
        end_date = _now().date()
        start_date = end_date - timedelta(weeks=weeks)
        history = get_account_history(account_id, start_date=start_date, end_date=end_date)
        if not history:
            return {}

        weekly_data: Dict[str, Dict[str, List[float]]] = {}
        for record in history:
            date_obj = datetime.strptime(record['date'], '%Y-%m-%d').date()
            year, week, _ = date_obj.isocalendar()
            week_key = f'{year}-W{week:02d}'
            weekly_data.setdefault(week_key, {'totalAssets': [], 'marketValues': []})
            weekly_data[week_key]['totalAssets'].append(record['total_assets'])
            weekly_data[week_key]['marketValues'].append(record['market_value'])

        result = {}
        for week_key, data in weekly_data.items():
            if not data['totalAssets']:
                continue
            end_assets = data['totalAssets'][-1]
            start_assets = data['totalAssets'][0]
            return_rate = ((end_assets - start_assets) / start_assets * 100) if start_assets > 0 else 0
            avg_market_value = sum(data['marketValues']) / len(data['marketValues']) if data['marketValues'] else 0
            avg_assets = sum(data['totalAssets']) / len(data['totalAssets'])
            investment_rate = (avg_market_value / avg_assets * 100) if avg_assets > 0 else 0
            result[week_key] = {
                'totalAssets': round(end_assets, 2),
                'returnRate': round(return_rate, 2),
                'investmentRate': round(investment_rate, 2)
            }
        return result
    except Exception as e:
        logger.error('获取周度数据失败: %s', str(e), exc_info=True)
        return {}
