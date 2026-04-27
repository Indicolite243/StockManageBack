import datetime
import logging
from collections import defaultdict

from django.http import JsonResponse
from rest_framework.decorators import api_view
from xtquant import xtdata

from apps.utils.data_storage import (
    get_account_history,
    get_all_latest_account_states,
    save_account_snapshot,
)
from apps.utils.xt_trader import create_stock_account, get_xt_trader_connection

logger = logging.getLogger(__name__)


def _parse_snapshot_time(snapshot_time):
    if not snapshot_time:
        return None
    if isinstance(snapshot_time, datetime.datetime):
        return snapshot_time
    try:
        return datetime.datetime.fromisoformat(str(snapshot_time))
    except Exception:
        return None


def resolve_stock_name(stock_code):
    if not stock_code:
        return ''
    try:
        detail = xtdata.get_instrument_detail(stock_code)
        if isinstance(detail, dict):
            return detail.get('InstrumentName') or detail.get('instrument_name') or str(stock_code)
        if detail:
            return getattr(detail, 'InstrumentName', None) or getattr(detail, 'instrument_name', None) or str(stock_code)
    except Exception as exc:
        logger.warning('获取股票 %s 名称失败: %s', stock_code, str(exc))
    return str(stock_code)


def normalize_data_source(requested_source: str, default: str = 'qmt') -> str:
    source = (requested_source or default).strip().lower()
    if source not in {'qmt', 'mongodb', 'auto'}:
        source = default
    return source


def _format_accounts(snapshot_accounts, source='mongodb_cache', is_realtime=False, fallback_reason=''):
    formatted_accounts = []
    for account in snapshot_accounts:
        positions = account.get('positions', []) or []
        total_asset = float(account.get('total_asset', 0) or 0)
        initial_total_asset = 10000000
        total_return_rate = ((total_asset - initial_total_asset) / initial_total_asset * 100) if initial_total_asset else 0
        total_positions = sum(int(position.get('volume', 0) or 0) for position in positions)
        formatted_accounts.append({
            'account_id': str(account.get('account_id', '')),
            'account_type': str(account.get('account_type', 'STOCK')),
            'total_asset': total_asset,
            'cash': float(account.get('cash', 0) or 0),
            'frozen_cash': float(account.get('frozen_cash', 0) or 0),
            'market_value': float(account.get('market_value', 0) or 0),
            'positions': positions,
            'total_return_rate': round(total_return_rate, 2),
            'total_positions': total_positions,
            'snapshot_time': account.get('snapshot_time', ''),
        })
    snapshot_time = next((item.get('snapshot_time') for item in formatted_accounts if item.get('snapshot_time')), '')
    return {
        'success': True,
        'accounts': formatted_accounts,
        'data_source': source,
        'snapshot_time': snapshot_time,
        'is_realtime': is_realtime,
        'is_stale': not is_realtime,
        'fallback_reason': fallback_reason,
    }


def convert_positions(positions, account_id):
    if not positions:
        return []

    pos_list = []
    stock_codes = [pos.stock_code for pos in positions]
    current_prices = {}

    try:
        for code in stock_codes:
            xtdata.subscribe_quote(code, period='1d', start_time='', end_time='', count=0, callback=None)
        ticks = xtdata.get_full_tick(stock_codes)
        if ticks:
            for code, tick in ticks.items():
                if tick and 'lastPrice' in tick:
                    price = tick['lastPrice']
                    if price and price > 0:
                        current_prices[code] = price
            logger.info('成功获取实时行情: %s 只股票', len(current_prices))
    except Exception as exc:
        logger.warning('获取实时行情失败: %s', str(exc))

    for pos in positions:
        try:
            current_price = 0.0
            if pos.stock_code in current_prices:
                current_price = current_prices[pos.stock_code]
            elif getattr(pos, 'volume', 0) and getattr(pos, 'market_value', 0):
                current_price = pos.market_value / pos.volume
            elif hasattr(pos, 'open_price'):
                current_price = pos.open_price

            open_price = round(float(current_price), 2)
            avg_price = round(float(getattr(pos, 'open_price', 0) or 0), 2)
            pos_data = {
                'account_id': str(account_id),
                'account_type': str(getattr(pos, 'account_type', 'STOCK')),
                'stock_code': str(pos.stock_code),
                'stock_name': resolve_stock_name(pos.stock_code),
                'volume': int(getattr(pos, 'volume', 0) or 0),
                'can_use_volume': int(getattr(pos, 'can_use_volume', 0) or 0),
                'current_price': open_price,
                'open_price': open_price,
                'market_value': float(getattr(pos, 'market_value', 0) or 0),
                'frozen_volume': int(getattr(pos, 'frozen_volume', 0) or 0),
                'on_road_volume': int(getattr(pos, 'on_road_volume', 0) or 0),
                'yesterday_volume': int(getattr(pos, 'yesterday_volume', 0) or 0),
                'avg_price': avg_price,
                'cost_price': avg_price,
            }
            pos_list.append(pos_data)
        except Exception as exc:
            logger.error('转换持仓数据失败 %s: %s', getattr(pos, 'stock_code', 'unknown'), str(exc))

    pos_list.sort(key=lambda x: x['market_value'], reverse=True)
    return pos_list


def fetch_live_accounts_from_qmt():
    xt_trader, connected = get_xt_trader_connection()
    if not connected:
        raise RuntimeError('连接交易接口失败')

    accounts = xt_trader.query_account_infos() or []
    account_list = []
    for acc in accounts:
        try:
            raw_account_type = getattr(acc, 'account_type', 'STOCK')
            if isinstance(raw_account_type, int):
                account_type = 'STOCK' if raw_account_type == 2 else ('FUTURE' if raw_account_type == 3 else 'STOCK')
            else:
                account_type = str(raw_account_type)
            account_obj = create_stock_account(acc.account_id, account_type)
            xt_trader.subscribe(account_obj)
            asset = xt_trader.query_stock_asset(account_obj)
            if asset is None:
                continue
            positions = xt_trader.query_stock_positions(account_obj) or []
            pos_list = convert_positions(positions, asset.account_id)
            account_data = {
                'account_id': str(asset.account_id),
                'account_type': account_type,
                'total_asset': float(asset.total_asset),
                'cash': float(asset.cash),
                'frozen_cash': float(asset.frozen_cash),
                'market_value': float(asset.market_value),
                'positions': pos_list,
                'snapshot_time': datetime.datetime.now().isoformat(timespec='seconds'),
            }
            save_account_snapshot(asset.account_id, account_data, source='qmt_live')
            account_list.append(account_data)
        except Exception as exc:
            logger.error('处理账户 %s 时出错: %s', getattr(acc, 'account_id', 'unknown'), str(exc), exc_info=True)

    if not account_list:
        raise RuntimeError('未查询到任何账户数据')

    return _format_accounts(account_list, source='qmt_live', is_realtime=True)


@api_view(['GET'])
def get_account_info(request):
    use_mock = request.GET.get('mock', 'false').lower() == 'true'
    if use_mock:
        return JsonResponse({'success': True, 'accounts': [], 'data_source': 'mock'})

    source = normalize_data_source(request.GET.get('source'), default='qmt')
    if source == 'mongodb':
        snapshot_accounts = get_all_latest_account_states()
        if snapshot_accounts:
            return JsonResponse(_format_accounts(snapshot_accounts, source='mongodb_cache', is_realtime=False))
        return JsonResponse({'success': True, 'accounts': [], 'data_source': 'mongodb_cache', 'snapshot_time': '', 'is_realtime': False, 'is_stale': True, 'fallback_reason': ''})

    try:
        return JsonResponse(fetch_live_accounts_from_qmt())
    except Exception as live_error:
        logger.warning('QMT 实时账户获取失败: %s', str(live_error))
        snapshot_accounts = get_all_latest_account_states()
        if snapshot_accounts:
            return JsonResponse(_format_accounts(snapshot_accounts, source='mongodb_cache', is_realtime=False, fallback_reason=str(live_error)))
        return JsonResponse({'success': False, 'error': str(live_error), 'accounts': []}, status=503)


@api_view(['GET'])
def get_asset_category(request):
    source = normalize_data_source(request.GET.get('source'), default='qmt')
    response = get_account_info(request)
    data = response.data if hasattr(response, 'data') else None
    if data is None:
        import json
        data = json.loads(response.content.decode('utf-8'))
    accounts = data.get('accounts', [])
    total_market_value = sum(float(acc.get('market_value', 0) or 0) for acc in accounts)
    total_cash = sum(float(acc.get('cash', 0) or 0) for acc in accounts)
    total_assets = total_market_value + total_cash
    stock_percentage = (total_market_value / total_assets * 100) if total_assets > 0 else 0
    cash_percentage = (total_cash / total_assets * 100) if total_assets > 0 else 0
    return JsonResponse({
        'categories': [
            {'category': '股票', 'totalAssets': round(total_market_value, 2), 'percentage': round(stock_percentage, 2)},
            {'category': '现金', 'totalAssets': round(total_cash, 2), 'percentage': round(cash_percentage, 2)},
        ],
        'data_source': data.get('data_source', source),
        'snapshot_time': data.get('snapshot_time', ''),
    })


def _infer_region_from_code(stock_code):
    code = str(stock_code or '')
    if code.endswith('.SH'):
        return '上海'
    if code.endswith('.SZ'):
        return '深圳'
    if code.endswith('.BJ'):
        return '北京'
    return '其他'


@api_view(['GET'])
def get_region_data(request):
    response = get_account_info(request)
    data = response.data if hasattr(response, 'data') else None
    if data is None:
        import json
        data = json.loads(response.content.decode('utf-8'))
    region_totals = defaultdict(float)
    total_assets = 0.0
    for account in data.get('accounts', []):
        for pos in account.get('positions', []) or []:
            market_value = float(pos.get('market_value', 0) or 0)
            region_totals[_infer_region_from_code(pos.get('stock_code'))] += market_value
            total_assets += market_value
    regions = []
    for region, market_value in sorted(region_totals.items(), key=lambda item: item[1], reverse=True):
        percentage = (market_value / total_assets * 100) if total_assets > 0 else 0
        regions.append({'region': region, 'totalAssets': round(market_value, 2), 'percentage': round(percentage, 2)})
    return JsonResponse({'regions': regions, 'data_source': data.get('data_source', ''), 'snapshot_time': data.get('snapshot_time', '')})


@api_view(['GET'])
def get_time_data(request):
    account_id = request.GET.get('account_id', '62283925')
    duration = int(request.GET.get('duration', 30) or 30)
    history = get_account_history(account_id, days=duration)
    if history:
        return JsonResponse({
            'account_id': account_id,
            'time_data': history,
            'is_mock': False,
            'data_source': 'mongodb_history',
            'snapshot_time': history[-1].get('snapshot_time', ''),
        })
    return JsonResponse({
        'account_id': account_id,
        'time_data': [],
        'is_mock': False,
        'data_source': 'mongodb_history',
        'snapshot_time': '',
    })
