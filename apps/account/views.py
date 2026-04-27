import time
import datetime
import logging
from django.http import JsonResponse
from rest_framework.decorators import api_view
from xtquant import xtdata
from django.conf import settings
from apps.utils.xt_trader import get_xt_trader_connection, create_stock_account
from apps.utils.data_storage import (
    get_account_history,
    get_all_latest_account_states,
    get_latest_account_state,
    save_account_snapshot,
)

# 閰嶇疆鏃ュ織
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


def _is_snapshot_stale(snapshot_accounts, max_age_seconds=90):
    latest_time = None
    for account in snapshot_accounts or []:
        parsed_time = _parse_snapshot_time(account.get('snapshot_time'))
        if parsed_time and (latest_time is None or parsed_time > latest_time):
            latest_time = parsed_time
    if latest_time is None:
        return True
    return (datetime.datetime.now() - latest_time).total_seconds() > max_age_seconds


def resolve_stock_name(stock_code):
    """Resolve instrument name from xtdata with safe fallbacks."""
    if not stock_code:
        return ''

    try:
        detail = xtdata.get_instrument_detail(stock_code)
        if isinstance(detail, dict):
            return detail.get('InstrumentName') or detail.get('instrument_name') or str(stock_code)
        if detail:
            return getattr(detail, 'InstrumentName', None) or getattr(detail, 'instrument_name', None) or str(stock_code)
    except Exception as e:
        logger.warning(f'鑾峰彇鑲＄エ {stock_code} 鍚嶇О澶辫触: {str(e)}')

    return str(stock_code)


def normalize_data_source(requested_source: str, default: str = 'qmt') -> str:
    source = (requested_source or default).strip().lower()
    if source not in {'qmt', 'mongodb', 'auto'}:
        source = default
    return source


def format_snapshot_accounts(snapshot_accounts):
    account_list = []
    for account in snapshot_accounts:
        positions = account.get('positions', [])
        total_asset = float(account.get('total_asset', 0) or 0)
        initial_total_asset = 10000000
        total_return_rate = ((total_asset - initial_total_asset) / initial_total_asset * 100) if initial_total_asset else 0
        total_positions = sum(int(position.get('volume', 0) or 0) for position in positions)

        account_list.append({
            'account_id': str(account.get('account_id', '')),
            'account_type': str(account.get('account_type', 'STOCK')),
            'total_asset': total_asset,
            'cash': float(account.get('cash', 0) or 0),
            'frozen_cash': float(account.get('frozen_cash', 0) or 0),
            'market_value': float(account.get('market_value', 0) or 0),
            'positions': positions,
            'total_return_rate': round(total_return_rate, 2),
            'total_positions': total_positions,
            'snapshot_time': account.get('snapshot_time'),
        })

    return account_list


def build_snapshot_response(snapshot_accounts, source='mongodb_cache', fallback_reason=''):
    formatted_accounts = format_snapshot_accounts(snapshot_accounts)
    snapshot_time = next((item.get('snapshot_time') for item in formatted_accounts if item.get('snapshot_time')), None)
    return {
        'accounts': formatted_accounts,
        'data_source': source,
        'snapshot_time': snapshot_time,
        'is_realtime': False,
        'is_stale': True,
        'fallback_reason': fallback_reason,
    }


def fetch_live_accounts_from_qmt():
    xt_trader, connected = get_xt_trader_connection()
    if not connected:
        raise RuntimeError('杩炴帴浜ゆ槗鎺ュ彛澶辫触')

    accounts = xt_trader.query_account_infos()
    account_list = []
    for acc in accounts:
        try:
            raw_account_type = getattr(acc, 'account_type', 'STOCK')
            if isinstance(raw_account_type, int):
                if raw_account_type == 2:
                    account_type = 'STOCK'
                elif raw_account_type == 3:
                    account_type = 'FUTURE'
                else:
                    account_type = 'STOCK'
            else:
                account_type = str(raw_account_type)

            account_obj = create_stock_account(acc.account_id, account_type)
            xt_trader.subscribe(account_obj)
            asset = xt_trader.query_stock_asset(account_obj)
            if asset is None:
                continue

            positions = xt_trader.query_stock_positions(account_obj)
            pos_list = convert_positions(positions, asset.account_id)
            account_data = {
                'account_id': str(asset.account_id),
                'account_type': str(asset.account_type) if hasattr(asset, 'account_type') else 'STOCK',
                'total_asset': float(asset.total_asset),
                'cash': float(asset.cash),
                'frozen_cash': float(asset.frozen_cash),
                'market_value': float(asset.market_value),
                'positions': pos_list,
            }
            save_account_snapshot(asset.account_id, account_data, source='qmt_live')
            account_list.append(account_data)
        except Exception as e:
            logger.error('澶勭悊璐︽埛 %s 鏃跺嚭閿? %s', getattr(acc, 'account_id', 'unknown'), str(e), exc_info=True)

    if not account_list:
        raise RuntimeError('鏈煡璇㈠埌浠讳綍璐︽埛鏁版嵁')

    return {
        'accounts': format_snapshot_accounts(account_list),
        'data_source': 'qmt_live',
        'snapshot_time': datetime.datetime.now().isoformat(timespec='seconds'),
        'is_realtime': True,
        'is_stale': False,
    }


@api_view(['GET'])
def get_account_info(request):
    """
    ????????????
    API: /api/account-info/
    ?? source=qmt|mongodb|auto
    """
    use_mock = request.GET.get('mock', 'false').lower() == 'true'
    if use_mock:
        logger.info('????????')
        return get_mock_account_info()

    source = normalize_data_source(request.GET.get('source'), default='qmt')
    logger.info('??????, source=%s', source)

    if source == 'mongodb':
        snapshot_accounts = get_all_latest_account_states()
        if snapshot_accounts:
            return JsonResponse(build_snapshot_response(snapshot_accounts, source='mongodb_cache'))
        return JsonResponse({
            'success': True,
            'accounts': [],
            'data_source': 'mongodb_cache',
            'snapshot_time': '',
            'is_realtime': False,
            'is_stale': True,
            'fallback_reason': '',
        })

    try:
        live_response = fetch_live_accounts_from_qmt()
        return JsonResponse(live_response)
    except Exception as live_error:
        logger.warning('QMT ????????: %s', str(live_error))
        snapshot_accounts = get_all_latest_account_states()
        if snapshot_accounts:
            return JsonResponse(build_snapshot_response(snapshot_accounts, source='mongodb_cache', fallback_reason=str(live_error)))

        if source == 'auto':
            return JsonResponse({'success': False, 'error': 'QMT ? MongoDB ????', 'accounts': []}, status=503)
        return JsonResponse({'success': False, 'error': str(live_error), 'accounts': []}, status=503)

def convert_positions(positions, account_id):
    """
    杞崲鎸佷粨鏁版嵁涓哄墠绔渶瑕佺殑鏍煎紡
    - 鏁版嵁绫诲瀷杞崲锛堥伩鍏嶅簭鍒楀寲閿欒锛?
    - 鎸夊競鍊奸檷搴忔帓搴?
    - 杩斿洖鍓?0鏉¤褰?
    """
    if not positions:
        return []
    
    pos_list = []
    
    # 鑾峰彇鎵€鏈夋寔浠撶殑鑲＄エ浠ｇ爜鍒楄〃
    stock_codes = [pos.stock_code for pos in positions]
    
    # 鎵归噺鑾峰彇瀹炴椂琛屾儏鏁版嵁浠ユ樉绀哄綋鍓嶄环鏍?
    # 娉ㄦ剰锛歺tdata 闇€瑕佸湪 MiniQMT 杩愯鏃舵墠鑳借幏鍙栨暟鎹?
    current_prices = {}
    try:
        logger.info(f'灏濊瘯鑾峰彇瀹炴椂琛屾儏锛岃偂绁ㄥ垪琛? {stock_codes}')
        # 璁㈤槄杩欎簺鑲＄エ鐨勮鎯咃紙纭繚鏁版嵁鏄渶鏂扮殑锛?
        # 浣跨敤 'tick' 鍛ㄦ湡璁㈤槄瀹炴椂琛屾儏
        for code in stock_codes:
            xtdata.subscribe_quote(code, period='1d', start_time='', end_time='', count=0, callback=None)
        
        # 鑾峰彇鍏ㄦ帹鏁版嵁
        ticks = xtdata.get_full_tick(stock_codes)
        if ticks:
            for code, tick in ticks.items():
                if tick and 'lastPrice' in tick:
                    price = tick['lastPrice']
                    # 杩囨护鎺変环鏍间负0鐨勬棤鏁堟暟鎹紙鍋滅墝鎴栨湭寮€鐩樺彲鑳藉鑷?锛?
                    if price > 0:
                        current_prices[code] = price
            logger.info(f'成功获取实时行情: {len(current_prices)} 只股票')
        else:
            logger.warning('鑾峰彇鍒扮殑 tick 鏁版嵁涓虹┖')
            
    except Exception as e:
        logger.warning(f'鑾峰彇瀹炴椂琛屾儏澶辫触: {str(e)}')

    for pos in positions:
        try:
            # 纭畾褰撳墠浠锋牸锛氫紭鍏堜娇鐢ㄥ疄鏃惰鎯咃紝鍚﹀垯閫氳繃甯傚€?鏁伴噺璁＄畻锛屾渶鍚庝娇鐢ㄥ紑浠撲环鍏滃簳
            current_price = 0.0
            if pos.stock_code in current_prices:
                current_price = current_prices[pos.stock_code]
            elif pos.volume > 0 and hasattr(pos, 'market_value'):
                # 濡傛灉娌℃湁瀹炴椂琛屾儏锛屽皾璇曠敤 甯傚€?鏁伴噺 璁＄畻闅愬惈浠锋牸
                current_price = pos.market_value / pos.volume
            elif hasattr(pos, 'open_price'):
                current_price = pos.open_price

            # 纭繚鎵€鏈夋暟鍊肩被鍨嬫纭浆鎹?
            pos_data = {
                'account_id': str(account_id),
                'account_type': str(pos.account_type) if hasattr(pos, 'account_type') else 'STOCK',
                'stock_code': str(pos.stock_code),  # 鑲＄エ浠ｇ爜锛屽 "600000.SH"
                'stock_name': resolve_stock_name(pos.stock_code),  # 鑲＄エ鍚嶇О
                'volume': int(pos.volume),  # 鎸佷粨鏁伴噺
                'can_use_volume': int(pos.can_use_volume),  # 鍙敤鏁伴噺
                'open_price': round(float(current_price), 2),  # 褰撳墠浠锋牸锛堜慨姝ｄ负瀹炴椂琛屾儏浠锋牸锛?
                'market_value': float(pos.market_value),  # 甯傚€?
                'frozen_volume': int(pos.frozen_volume) if hasattr(pos, 'frozen_volume') and pos.frozen_volume else 0,  # 鍐荤粨鏁伴噺
                'on_road_volume': int(pos.on_road_volume) if hasattr(pos, 'on_road_volume') and pos.on_road_volume else 0,  # 鍦ㄩ€旇偂浠?
                'yesterday_volume': int(pos.yesterday_volume) if hasattr(pos, 'yesterday_volume') else 0,  # 鏄ㄦ棩鎸佷粨
                'avg_price': round(float(pos.open_price), 2) if hasattr(pos, 'open_price') else 0.0,  # 鎴愭湰浠凤紙淇涓轰娇鐢╫pen_price浣滀负鎴愭湰浠凤級
            }
            pos_list.append(pos_data)
        except Exception as e:
            logger.error(f'杞崲鎸佷粨鏁版嵁澶辫触 {getattr(pos, "stock_code", "unknown")}: {str(e)}')
            continue
    
    # 鎸夊競鍊奸檷搴忔帓搴?
    pos_list.sort(key=lambda x: x['market_value'], reverse=True)
    
    # 杩斿洖鍓?0鏉★紙鍓嶇闇€姹傦級
    return pos_list


def get_mock_account_info():
    """
    杩斿洖妯℃嫙璐︽埛鏁版嵁
    鐢ㄤ簬娴嬭瘯鍜屾紨绀猴紝褰撹繀鎶曡繛鎺ヤ笉鍙敤鏃惰嚜鍔ㄤ娇鐢?
    """
    logger.info('杩斿洖妯℃嫙璐︽埛鏁版嵁')
    
    mock_data = {
        'accounts': [
            {
                'account_id': 'DEMO000001',
                'account_type': 'STOCK',
                'cash': 1250000.00,
                'frozen_cash': 75000.00,
                'market_value': 2850000.00,
                'total_asset': 4100000.00,
                'positions': [
                    {
                        'account_id': 'DEMO000001',
                        'account_type': 'STOCK',
                        'stock_code': '600519.SH',
                        'stock_name': '璐靛窞鑼呭彴',
                        'volume': 500,
                        'can_use_volume': 500,
                        'open_price': 1680.50,
                        'market_value': 840250.00,
                        'frozen_volume': 0,
                        'on_road_volume': 0,
                        'yesterday_volume': 500,
                        'avg_price': 1620.00
                    },
                    {
                        'account_id': 'DEMO000001',
                        'account_type': 'STOCK',
                        'stock_code': '600036.SH',
                        'stock_name': '鎷涘晢閾惰',
                        'volume': 20000,
                        'can_use_volume': 20000,
                        'open_price': 35.80,
                        'market_value': 716000.00,
                        'frozen_volume': 0,
                        'on_road_volume': 0,
                        'yesterday_volume': 20000,
                        'avg_price': 34.20
                    },
                    {
                        'account_id': 'DEMO000001',
                        'account_type': 'STOCK',
                        'stock_code': '601318.SH',
                        'stock_name': '涓浗骞冲畨',
                        'volume': 15000,
                        'can_use_volume': 15000,
                        'open_price': 42.50,
                        'market_value': 637500.00,
                        'frozen_volume': 0,
                        'on_road_volume': 0,
                        'yesterday_volume': 15000,
                        'avg_price': 41.00
                    },
                    {
                        'account_id': 'DEMO000001',
                        'account_type': 'STOCK',
                        'stock_code': '000858.SZ',
                        'stock_name': '五粮液',
                        'volume': 3000,
                        'can_use_volume': 3000,
                        'open_price': 155.60,
                        'market_value': 466800.00,
                        'frozen_volume': 0,
                        'on_road_volume': 0,
                        'yesterday_volume': 3000,
                        'avg_price': 150.00
                    },
                    {
                        'account_id': 'DEMO000001',
                        'account_type': 'STOCK',
                        'stock_code': '000001.SZ',
                        'stock_name': '骞冲畨閾惰',
                        'volume': 8000,
                        'can_use_volume': 8000,
                        'open_price': 12.50,
                        'market_value': 100000.00,
                        'frozen_volume': 0,
                        'on_road_volume': 0,
                        'yesterday_volume': 8000,
                        'avg_price': 11.80
                    },
                    {
                        'account_id': 'DEMO000001',
                        'account_type': 'STOCK',
                        'stock_code': '600887.SH',
                        'stock_name': '浼婂埄鑲′唤',
                        'volume': 2500,
                        'can_use_volume': 2500,
                        'open_price': 28.90,
                        'market_value': 72250.00,
                        'frozen_volume': 0,
                        'on_road_volume': 0,
                        'yesterday_volume': 2500,
                        'avg_price': 27.50
                    },
                    {
                        'account_id': 'DEMO000001',
                        'account_type': 'STOCK',
                        'stock_code': '601012.SH',
                        'stock_name': '闅嗗熀缁胯兘',
                        'volume': 1200,
                        'can_use_volume': 1200,
                        'open_price': 18.30,
                        'market_value': 21960.00,
                        'frozen_volume': 0,
                        'on_road_volume': 0,
                        'yesterday_volume': 1200,
                        'avg_price': 20.00
                    },
                    {
                        'account_id': 'DEMO000001',
                        'account_type': 'STOCK',
                        'stock_code': '300750.SZ',
                        'stock_name': '瀹佸痉鏃朵唬',
                        'volume': 100,
                        'can_use_volume': 100,
                        'open_price': 165.80,
                        'market_value': 16580.00,
                        'frozen_volume': 0,
                        'on_road_volume': 0,
                        'yesterday_volume': 100,
                        'avg_price': 180.00
                    }
                ]
            }
        ]
    }
    
    return JsonResponse(mock_data)


@api_view(['GET'])
def get_asset_category(request):
    """
    鑾峰彇璧勪骇鍒嗙被鏁版嵁
    API鏂囨。: /api/asset-category/
    鏍规嵁鑲＄エ鎵€灞炶涓?鏉垮潡杩涜鍒嗙被缁熻
    绗﹀悎鍓嶇鏁版嵁鏍煎紡瑕佹眰锛氫娇鐢╟ategories瀛楁锛宑ategory鍜宼otalAssets瀛楁鍚?
    """
    logger.info('鑾峰彇璧勪骇鍒嗙被鏁版嵁')
    
    # 妫€鏌ユ槸鍚︿娇鐢ㄦā鎷熸暟鎹?
    use_mock = request.GET.get('mock', 'true').lower() == 'true'
    
    if use_mock:
        # 妯℃嫙鏁版嵁 - 绗﹀悎鍓嶇鏍煎紡瑕佹眰
        category_data = {
            'categories': [  # 鍓嶇瑕佹眰浣跨敤categories瀛楁
                {
                    'category': '鑲＄エ',  # 鍓嶇瑕佹眰浣跨敤category瀛楁
                    'totalAssets': 2850000.00,  # 鍓嶇瑕佹眰浣跨敤totalAssets瀛楁
                    'percentage': 69.51
                },
                {
                    'category': '鐜伴噾',
                    'totalAssets': 1250000.00,
                    'percentage': 30.49
                }
            ]
        }
        return JsonResponse(category_data)
    
    try:
        logger.info('寮€濮嬭幏鍙栬祫浜у垎绫绘暟鎹紙鐪熷疄鏁版嵁锛?)
        
        # 浣跨敤缁熶竴鐨勪氦鏄撴帴鍙ｈ繛鎺ュ伐鍏?
        xt_trader, connected = get_xt_trader_connection()
        if not connected:
            logger.error('杩炴帴浜ゆ槗鎺ュ彛澶辫触')
            logger.info('鑷姩鍒囨崲鍒版ā鎷熸暟鎹ā寮?)
            return JsonResponse({
                'categories': [
                    {'category': '鑲＄エ', 'totalAssets': 2850000.00, 'percentage': 69.51},
                    {'category': '鐜伴噾', 'totalAssets': 1250000.00, 'percentage': 30.49}
                ]
            })

        # 鏌ヨ鎵€鏈夎处鎴蜂俊鎭?
        accounts = xt_trader.query_account_infos()
        if not accounts:
            logger.warning('鏈煡璇㈠埌璐︽埛淇℃伅')
            return JsonResponse({
                'categories': [
                    {'category': '鑲＄エ', 'totalAssets': 0.00, 'percentage': 0.00},
                    {'category': '鐜伴噾', 'totalAssets': 0.00, 'percentage': 0.00}
                ]
            })

        # 姹囨€绘墍鏈夎处鎴风殑鏁版嵁
        total_market_value = 0.0
        total_cash = 0.0
        
        for acc in accounts:
            try:
                xt_trader.subscribe(acc)
                asset = xt_trader.query_stock_asset(acc)
                if asset:
                    total_market_value += float(asset.market_value)
                    total_cash += float(asset.cash)
            except Exception as e:
                logger.warning(f'澶勭悊璐︽埛 {acc} 鏃跺嚭閿? {str(e)}')
                continue
        
        total_assets = total_market_value + total_cash
        
        # 璁＄畻鍗犳瘮
        stock_percentage = (total_market_value / total_assets * 100) if total_assets > 0 else 0
        cash_percentage = (total_cash / total_assets * 100) if total_assets > 0 else 0
        
        logger.info(f'鎴愬姛鑾峰彇璧勪骇鍒嗙被鏁版嵁锛氳偂绁?{total_market_value:.2f}锛岀幇閲?{total_cash:.2f}')
        return JsonResponse({
            'categories': [
                {
                    'category': '鑲＄エ',
                    'totalAssets': round(total_market_value, 2),
                    'percentage': round(stock_percentage, 2)
                },
                {
                    'category': '鐜伴噾',
                    'totalAssets': round(total_cash, 2),
                    'percentage': round(cash_percentage, 2)
                }
            ]
        })
        
    except Exception as e:
        logger.error(f'鑾峰彇璧勪骇鍒嗙被鏁版嵁澶辫触: {str(e)}', exc_info=True)
        # 鍙戠敓閿欒鏃惰繑鍥炴ā鎷熸暟鎹?
        return JsonResponse({
            'categories': [
                {'category': '鑲＄エ', 'totalAssets': 2850000.00, 'percentage': 69.51},
                {'category': '鐜伴噾', 'totalAssets': 1250000.00, 'percentage': 30.49}
            ]
        })


@api_view(['GET'])
def get_region_data(request):
    """
    鑾峰彇鍦板尯鍒嗗竷鏁版嵁
    API鏂囨。: /api/region-data/
    鏍规嵁鑲＄エ涓婂競鍦板尯杩涜缁熻
    绗﹀悎鍓嶇鏁版嵁鏍煎紡瑕佹眰锛氫娇鐢╮egions瀛楁锛宺egion鍜宼otalAssets瀛楁鍚?
    """
    logger.info('鑾峰彇鍦板尯鍒嗗竷鏁版嵁')
    
    # 妫€鏌ユ槸鍚︿娇鐢ㄦā鎷熸暟鎹?
    use_mock = request.GET.get('mock', 'true').lower() == 'true'
    
    if use_mock:
        # 妯℃嫙鏁版嵁 - 绗﹀悎鍓嶇鏍煎紡瑕佹眰
        region_data = {
            'regions': [  # 鍓嶇瑕佹眰浣跨敤regions瀛楁
                {
                    'region': '涓婃捣',  # 鍓嶇瑕佹眰浣跨敤region瀛楁
                    'totalAssets': 1353500.00,  # 鍓嶇瑕佹眰浣跨敤totalAssets瀛楁
                    'percentage': 28.77
                },
                {
                    'region': '娣卞湷',
                    'totalAssets': 712500.00,
                    'percentage': 25.00
                },
                {
                    'region': '鍖椾含',
                    'totalAssets': 570000.00,
                    'percentage': 20.00
                },
                {
                    'region': '骞垮窞',
                    'totalAssets': 342000.00,
                    'percentage': 12.00
                },
                {
                    'region': '鏉窞',
                    'totalAssets': 228000.00,
                    'percentage': 8.00
                },
                {
                    'region': '鍏朵粬',
                    'totalAssets': 177500.00,
                    'percentage': 6.23
                }
            ]
        }
        return JsonResponse(region_data)
    
    try:
        logger.info('寮€濮嬭幏鍙栧湴鍖哄垎甯冩暟鎹紙鐪熷疄鏁版嵁锛?)
        
        # 浣跨敤缁熶竴鐨勪氦鏄撴帴鍙ｈ繛鎺ュ伐鍏?
        xt_trader, connected = get_xt_trader_connection()
        if not connected:
            logger.error('杩炴帴浜ゆ槗鎺ュ彛澶辫触')
            logger.info('鑷姩鍒囨崲鍒版ā鎷熸暟鎹ā寮?)
            return JsonResponse({
                'regions': [
                    {'region': '涓婃捣', 'totalAssets': 1353500.00, 'percentage': 28.77}
                ]
            })

        # 鏌ヨ鎵€鏈夎处鎴蜂俊鎭?
        accounts = xt_trader.query_account_infos()
        if not accounts:
            logger.warning('鏈煡璇㈠埌璐︽埛淇℃伅')
            return JsonResponse({'regions': []})

        # 鑾峰彇鑲＄エ鍦板尯淇℃伅
        from apps.utils.stock_info import get_stock_region
        
        # 鎸夊湴鍖烘眹鎬?
        region_data_dict = {}
        total_market_value = 0.0
        
        for acc in accounts:
            try:
                xt_trader.subscribe(acc)
                positions = xt_trader.query_stock_positions(acc)
                if positions:
                    for pos in positions:
                        stock_code = pos.stock_code
                        market_value = float(pos.market_value)
                        region = get_stock_region(stock_code)
                        
                        if region not in region_data_dict:
                            region_data_dict[region] = 0.0
                        
                        region_data_dict[region] += market_value
                        total_market_value += market_value
            except Exception as e:
                logger.warning(f'澶勭悊璐︽埛 {acc} 鏃跺嚭閿? {str(e)}')
                continue
        
        # 璁＄畻鍗犳瘮骞惰浆鎹负鍒楄〃
        region_list = []
        for region, assets in region_data_dict.items():
            percentage = (assets / total_market_value * 100) if total_market_value > 0 else 0
            region_list.append({
                'region': region,
                'totalAssets': round(assets, 2),
                'percentage': round(percentage, 2)
            })
        
        # 鎸夋€昏祫浜ч檷搴忔帓搴?
        region_list.sort(key=lambda x: x['totalAssets'], reverse=True)
        
        logger.info(f'鎴愬姛鑾峰彇 {len(region_list)} 涓湴鍖虹殑鏁版嵁')
        return JsonResponse({
            'regions': region_list
        })
        
    except Exception as e:
        logger.error(f'鑾峰彇鍦板尯鍒嗗竷鏁版嵁澶辫触: {str(e)}', exc_info=True)
        # 鍙戠敓閿欒鏃惰繑鍥炴ā鎷熸暟鎹?
        return JsonResponse({
            'regions': [
                {'region': '涓婃捣', 'totalAssets': 1353500.00, 'percentage': 28.77}
            ]
        })


@api_view(['GET'])
def get_time_data(request):
    """
    ?????????
    ???? MongoDB ??????????????????
    """
    logger.info('????????')

    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    account_id = request.GET.get('account_id')
    use_mock = request.GET.get('mock', 'false').lower() == 'true'

    if use_mock:
        import random
        from datetime import timedelta

        if not end_date:
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

        time_series = []
        current_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        base_value = 3800000.00

        while current_date <= end_dt:
            daily_change = random.uniform(-0.02, 0.02)
            base_value = base_value * (1 + daily_change)
            return_rate = (base_value - 3800000.00) / 3800000.00 * 100
            time_series.append({
                'date': current_date.strftime('%Y-%m-%d'),
                'totalAssets': round(base_value, 2),
                'returnRate': round(return_rate, 2)
            })
            current_date += timedelta(days=1)

        return JsonResponse({'time_series': time_series, 'data_source': 'mock', 'is_mock': True})

    try:
        if not account_id:
            latest_snapshot = get_latest_account_state()
            if not latest_snapshot:
                return JsonResponse({'time_series': [], 'data_source': 'mongodb_history', 'is_mock': False})
            account_id = latest_snapshot.get('account_id')

        history = get_account_history(account_id, start_date=start_date, end_date=end_date, days=30)
        if not history:
            return JsonResponse({'time_series': [], 'data_source': 'mongodb_history', 'is_mock': False, 'account_id': account_id})

        initial_assets = history[0]['total_assets'] if history else 0
        time_series = []
        for record in history:
            current_assets = record['total_assets']
            return_rate = ((current_assets - initial_assets) / initial_assets * 100) if initial_assets > 0 else 0
            time_series.append({
                'date': record['date'],
                'totalAssets': round(current_assets, 2),
                'returnRate': round(return_rate, 2)
            })

        return JsonResponse({
            'time_series': time_series,
            'data_source': 'mongodb_history',
            'is_mock': False,
            'account_id': account_id,
            'snapshot_time': history[-1].get('snapshot_time') if history else None,
            'period_days_available': len(history),
        })
    except Exception as e:
        logger.error('??????????: %s', str(e), exc_info=True)
        return JsonResponse({'success': False, 'error': str(e), 'time_series': []}, status=500)



