from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from django.http import JsonResponse
from rest_framework.decorators import api_view

from apps.utils.data_storage import get_latest_account_state
from apps.utils.db import get_mongodb_db
from apps.utils.stock_info import INDUSTRY_UNKNOWN, get_instrument_metadata
from apps.utils.xt_trader import create_stock_account, get_xt_trader_connection

logger = logging.getLogger(__name__)


def _history_collection():
    return get_mongodb_db().account_snapshots


def _to_date(value: Optional[str]) -> date:
    if not value:
        return datetime.now().date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), '%Y-%m-%d').date()


def _serialize_snapshot(snapshot: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not snapshot:
        return None

    snapshot_time = snapshot.get('snapshot_time')
    timestamp = snapshot.get('timestamp')
    if not snapshot_time and isinstance(timestamp, datetime):
        snapshot_time = timestamp.isoformat(timespec='seconds')

    positions = []
    for position in snapshot.get('positions', []) or []:
        stock_code = str(position.get('stock_code', '')).strip().upper()
        stock_name = str(position.get('stock_name', stock_code))
        metadata = get_instrument_metadata(stock_code, stock_name=stock_name, allow_remote=False) or {}
        positions.append({
            'stock_code': stock_code,
            'stock_name': stock_name,
            'industry': position.get('industry') or metadata.get('industry') or INDUSTRY_UNKNOWN,
            'instrument_type': position.get('instrument_type') or metadata.get('instrument_type') or 'STOCK',
            'volume': int(position.get('volume', 0) or 0),
            'current_price': float(position.get('current_price', position.get('open_price', 0)) or 0),
            'open_price': float(position.get('open_price', position.get('current_price', 0)) or 0),
            'avg_price': float(position.get('avg_price', position.get('cost_price', 0)) or 0),
            'cost_price': float(position.get('cost_price', position.get('avg_price', 0)) or 0),
            'market_value': float(position.get('market_value', 0) or 0),
        })

    return {
        'account_id': str(snapshot.get('account_id', '')),
        'date': snapshot.get('date'),
        'timestamp': timestamp,
        'snapshot_time': snapshot_time,
        'data_source': snapshot.get('data_source', 'mongodb_history'),
        'total_asset': float(snapshot.get('total_asset', 0) or 0),
        'market_value': float(snapshot.get('market_value', 0) or 0),
        'cash': float(snapshot.get('cash', 0) or 0),
        'positions': positions,
    }


def _find_snapshot_on_or_after(account_id: str, target_date: date) -> Optional[Dict[str, Any]]:
    raw = _history_collection().find_one(
        {'account_id': str(account_id), 'date': {'$gte': target_date.isoformat()}},
        sort=[('date', 1), ('timestamp', 1)],
    )
    return _serialize_snapshot(raw)


def _find_snapshot_on_or_before(account_id: str, target_date: date) -> Optional[Dict[str, Any]]:
    raw = _history_collection().find_one(
        {'account_id': str(account_id), 'date': {'$lte': target_date.isoformat()}},
        sort=[('date', -1), ('timestamp', -1)],
    )
    return _serialize_snapshot(raw)


def _build_live_snapshot(account_id: str) -> Optional[Dict[str, Any]]:
    xt_trader, connected = get_xt_trader_connection()
    if not connected or not xt_trader:
        raise RuntimeError('QMT 交易接口未连接')

    acc = create_stock_account(account_id)
    xt_trader.subscribe(acc)
    asset = xt_trader.query_stock_asset(acc)
    if not asset:
        raise RuntimeError('未查询到账户资产信息')

    positions = xt_trader.query_stock_positions(acc) or []
    now = datetime.now()
    snapshot_positions = []
    total_market_value = float(asset.market_value or 0)

    for pos in positions:
        stock_code = str(getattr(pos, 'stock_code', '') or '').strip().upper()
        stock_name = stock_code
        try:
            from apps.Comparison.views import resolve_stock_name  # reuse existing helper
            stock_name = resolve_stock_name(stock_code) or stock_code
        except Exception:
            stock_name = stock_code

        volume = int(getattr(pos, 'volume', 0) or 0)
        market_value = float(getattr(pos, 'market_value', 0) or 0)
        cost_price = float(getattr(pos, 'open_price', 0) or 0)
        current_price = market_value / volume if volume > 0 else cost_price
        metadata = get_instrument_metadata(stock_code, stock_name=stock_name, allow_remote=False) or {}
        snapshot_positions.append({
            'stock_code': stock_code,
            'stock_name': stock_name,
            'industry': metadata.get('industry') or INDUSTRY_UNKNOWN,
            'instrument_type': metadata.get('instrument_type') or 'STOCK',
            'volume': volume,
            'current_price': current_price,
            'open_price': current_price,
            'avg_price': cost_price,
            'cost_price': cost_price,
            'market_value': market_value,
        })

    return {
        'account_id': str(account_id),
        'date': now.date().isoformat(),
        'timestamp': now,
        'snapshot_time': now.isoformat(timespec='seconds'),
        'data_source': 'qmt_live',
        'total_asset': float(asset.total_asset or 0),
        'market_value': total_market_value,
        'cash': float(asset.cash or 0),
        'positions': snapshot_positions,
    }


def _pick_start_snapshot(account_id: str, start_date: date) -> Optional[Dict[str, Any]]:
    return _find_snapshot_on_or_after(account_id, start_date) or _find_snapshot_on_or_before(account_id, start_date)


def _pick_end_snapshot(account_id: str, source: str, end_date: date) -> Optional[Dict[str, Any]]:
    today = datetime.now().date()
    if source == 'qmt' and end_date >= today:
        try:
            return _build_live_snapshot(account_id)
        except Exception as exc:
            logger.warning('QMT 实时归因快照获取失败，回退到 MongoDB: %s', exc)
            latest_snapshot = get_latest_account_state(account_id)
            if latest_snapshot:
                latest_snapshot['data_source'] = 'mongodb_cache'
                return latest_snapshot
    return _find_snapshot_on_or_before(account_id, end_date) or get_latest_account_state(account_id)


def _count_samples(account_id: str, start_date: date, end_date: date) -> int:
    return _history_collection().count_documents({
        'account_id': str(account_id),
        'date': {'$gte': start_date.isoformat(), '$lte': end_date.isoformat()},
    })


def _compute_attribution(start_snapshot: Dict[str, Any], end_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    end_positions = end_snapshot.get('positions', []) or []
    start_positions = {item.get('stock_code'): item for item in (start_snapshot.get('positions', []) or [])}
    total_market_value = float(end_snapshot.get('market_value', 0) or 0)
    if total_market_value <= 0:
        total_market_value = sum(float(item.get('market_value', 0) or 0) for item in end_positions)

    attribution_rows: List[Dict[str, Any]] = []
    industry_buckets: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        'name': INDUSTRY_UNKNOWN,
        'contribution_pct': 0.0,
        'pnl_amount': 0.0,
        'market_value': 0.0,
        'count': 0,
    })

    total_pnl_amount = 0.0
    positive_count = 0
    negative_count = 0

    for position in end_positions:
        stock_code = position.get('stock_code', '')
        stock_name = position.get('stock_name') or stock_code
        metadata = get_instrument_metadata(stock_code, stock_name=stock_name, allow_remote=False) or {}
        industry = position.get('industry') or metadata.get('industry') or INDUSTRY_UNKNOWN
        instrument_type = position.get('instrument_type') or metadata.get('instrument_type') or 'STOCK'
        volume = int(position.get('volume', 0) or 0)
        current_price = float(position.get('current_price', position.get('open_price', 0)) or 0)
        cost_price = float(position.get('cost_price', position.get('avg_price', 0)) or 0)
        market_value = float(position.get('market_value', 0) or 0)

        start_position = start_positions.get(stock_code, {})
        start_price = float(
            start_position.get('current_price')
            or start_position.get('open_price')
            or start_position.get('cost_price')
            or cost_price
            or current_price
            or 0
        )
        if start_price <= 0:
            start_price = current_price

        return_rate = ((current_price - start_price) / start_price * 100) if start_price > 0 else 0.0
        pnl_amount = (current_price - start_price) * volume if volume > 0 else market_value - float(start_position.get('market_value', 0) or 0)
        weight_pct = (market_value / total_market_value * 100) if total_market_value > 0 else 0.0
        contribution_pct = (pnl_amount / total_market_value * 100) if total_market_value > 0 else 0.0

        total_pnl_amount += pnl_amount
        if contribution_pct > 0:
            positive_count += 1
        elif contribution_pct < 0:
            negative_count += 1

        attribution_rows.append({
            'stockCode': stock_code,
            'stockName': stock_name,
            'industry': industry,
            'instrumentType': instrument_type,
            'weightPct': round(weight_pct, 4),
            'contributionPct': round(contribution_pct, 4),
            'returnRate': round(return_rate, 4),
            'pnlAmount': round(pnl_amount, 2),
            'marketValue': round(market_value, 2),
            'currentPrice': round(current_price, 4),
            'startPrice': round(start_price, 4),
            'costPrice': round(cost_price, 4),
            'volume': volume,
        })

        bucket = industry_buckets[industry]
        bucket['name'] = industry
        bucket['contribution_pct'] += contribution_pct
        bucket['pnl_amount'] += pnl_amount
        bucket['market_value'] += market_value
        bucket['count'] += 1

    attribution_rows.sort(key=lambda item: abs(item['contributionPct']), reverse=True)
    industry_rows = sorted(
        [
            {
                'name': bucket['name'],
                'contributionPct': round(bucket['contribution_pct'], 4),
                'pnlAmount': round(bucket['pnl_amount'], 2),
                'marketValue': round(bucket['market_value'], 2),
                'count': bucket['count'],
            }
            for bucket in industry_buckets.values()
        ],
        key=lambda item: abs(item['contributionPct']),
        reverse=True,
    )

    leading_industry = industry_rows[0]['name'] if industry_rows else INDUSTRY_UNKNOWN
    return {
        'summary': {
            'totalMarketValue': round(total_market_value, 2),
            'totalPnlAmount': round(total_pnl_amount, 2),
            'positiveCount': positive_count,
            'negativeCount': negative_count,
            'leadingIndustry': leading_industry,
        },
        'attributionRows': attribution_rows,
        'industryRows': industry_rows,
    }


@api_view(['GET'])
def asset_attribution(request):
    account_id = request.GET.get('account_id', '62283925')
    source = (request.GET.get('source') or 'qmt').strip().lower()
    if source not in {'qmt', 'mongodb'}:
        source = 'qmt'

    start_date = _to_date(request.GET.get('start_date'))
    end_date = _to_date(request.GET.get('end_date'))
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    try:
        start_snapshot = _pick_start_snapshot(account_id, start_date)
        end_snapshot = _pick_end_snapshot(account_id, source, end_date)
        if not end_snapshot:
            return JsonResponse({'success': False, 'error': '未找到可用于业绩归因的结束快照'}, status=404)
        if not start_snapshot:
            start_snapshot = end_snapshot

        result = _compute_attribution(start_snapshot, end_snapshot)
        result.update({
            'success': True,
            'account_id': account_id,
            'data_source': end_snapshot.get('data_source') or ('qmt_live' if source == 'qmt' else 'mongodb_history'),
            'snapshot_time': end_snapshot.get('snapshot_time'),
            'start_snapshot_time': start_snapshot.get('snapshot_time'),
            'range_start': start_date.isoformat(),
            'range_end': end_date.isoformat(),
            'sample_count': _count_samples(account_id, start_date, end_date),
        })
        return JsonResponse(result)
    except Exception as exc:
        logger.error('获取业绩归因数据失败: %s', exc, exc_info=True)
        return JsonResponse({'success': False, 'error': f'获取业绩归因数据失败: {exc}'}, status=500)
