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

# 配置日志
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
        logger.warning(f'获取股票 {stock_code} 名称失败: {str(e)}')

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
        raise RuntimeError('连接交易接口失败')

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
            logger.error('处理账户 %s 时出错: %s', getattr(acc, 'account_id', 'unknown'), str(e), exc_info=True)

    if not account_list:
        raise RuntimeError('未查询到任何账户数据')

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
    转换持仓数据为前端需要的格式
    - 数据类型转换（避免序列化错误）
    - 按市值降序排序
    - 返回前10条记录
    """
    if not positions:
        return []
    
    pos_list = []
    
    # 获取所有持仓的股票代码列表
    stock_codes = [pos.stock_code for pos in positions]
    
    # 批量获取实时行情数据以显示当前价格
    # 注意：xtdata 需要在 MiniQMT 运行时才能获取数据
    current_prices = {}
    try:
        logger.info(f'尝试获取实时行情，股票列表: {stock_codes}')
        # 订阅这些股票的行情（确保数据是最新的）
        # 使用 'tick' 周期订阅实时行情
        for code in stock_codes:
            xtdata.subscribe_quote(code, period='1d', start_time='', end_time='', count=0, callback=None)
        
        # 获取全推数据
        ticks = xtdata.get_full_tick(stock_codes)
        if ticks:
            for code, tick in ticks.items():
                if tick and 'lastPrice' in tick:
                    price = tick['lastPrice']
                    # 过滤掉价格为0的无效数据（停牌或未开盘可能导致0）
                    if price > 0:
                        current_prices[code] = price
            logger.info(f'成功获取实时行情: {len(current_prices)} 只股票')
        else:
            logger.warning('获取到的 tick 数据为空')
            
    except Exception as e:
        logger.warning(f'获取实时行情失败: {str(e)}')

    for pos in positions:
        try:
            # 确定当前价格：优先使用实时行情，否则通过市值/数量计算，最后使用开仓价兜底
            current_price = 0.0
            if pos.stock_code in current_prices:
                current_price = current_prices[pos.stock_code]
            elif pos.volume > 0 and hasattr(pos, 'market_value'):
                # 如果没有实时行情，尝试用 市值/数量 计算隐含价格
                current_price = pos.market_value / pos.volume
            elif hasattr(pos, 'open_price'):
                current_price = pos.open_price

            # 确保所有数值类型正确转换
            pos_data = {
                'account_id': str(account_id),
                'account_type': str(pos.account_type) if hasattr(pos, 'account_type') else 'STOCK',
                'stock_code': str(pos.stock_code),  # 股票代码，如 "600000.SH"
                'stock_name': resolve_stock_name(pos.stock_code),  # 股票名称
                'volume': int(pos.volume),  # 持仓数量
                'can_use_volume': int(pos.can_use_volume),  # 可用数量
                'open_price': round(float(current_price), 2),  # 当前价格（修正为实时行情价格）
                'market_value': float(pos.market_value),  # 市值
                'frozen_volume': int(pos.frozen_volume) if hasattr(pos, 'frozen_volume') and pos.frozen_volume else 0,  # 冻结数量
                'on_road_volume': int(pos.on_road_volume) if hasattr(pos, 'on_road_volume') and pos.on_road_volume else 0,  # 在途股份
                'yesterday_volume': int(pos.yesterday_volume) if hasattr(pos, 'yesterday_volume') else 0,  # 昨日持仓
                'avg_price': round(float(pos.open_price), 2) if hasattr(pos, 'open_price') else 0.0,  # 成本价（修正为使用open_price作为成本价）
            }
            pos_list.append(pos_data)
        except Exception as e:
            logger.error(f'转换持仓数据失败 {getattr(pos, "stock_code", "unknown")}: {str(e)}')
            continue
    
    # 按市值降序排序
    pos_list.sort(key=lambda x: x['market_value'], reverse=True)
    
    # 返回前10条（前端需求）
    return pos_list[:10]


def get_mock_account_info():
    """
    返回模拟账户数据
    用于测试和演示，当迅投连接不可用时自动使用
    """
    logger.info('返回模拟账户数据')
    
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
                        'stock_name': '贵州茅台',
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
                        'stock_name': '招商银行',
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
                        'stock_name': '中国平安',
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
                        'stock_name': '平安银行',
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
                        'stock_name': '伊利股份',
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
                        'stock_name': '隆基绿能',
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
                        'stock_name': '宁德时代',
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
    获取资产分类数据
    API文档: /api/asset-category/
    根据股票所属行业/板块进行分类统计
    符合前端数据格式要求：使用categories字段，category和totalAssets字段名
    """
    logger.info('获取资产分类数据')
    
    # 检查是否使用模拟数据
    use_mock = request.GET.get('mock', 'true').lower() == 'true'
    
    if use_mock:
        # 模拟数据 - 符合前端格式要求
        category_data = {
            'categories': [  # 前端要求使用categories字段
                {
                    'category': '股票',  # 前端要求使用category字段
                    'totalAssets': 2850000.00,  # 前端要求使用totalAssets字段
                    'percentage': 69.51
                },
                {
                    'category': '现金',
                    'totalAssets': 1250000.00,
                    'percentage': 30.49
                }
            ]
        }
        return JsonResponse(category_data)
    
    try:
        logger.info('开始获取资产分类数据（真实数据）')
        
        # 使用统一的交易接口连接工具
        xt_trader, connected = get_xt_trader_connection()
        if not connected:
            logger.error('连接交易接口失败')
            logger.info('自动切换到模拟数据模式')
            return JsonResponse({
                'categories': [
                    {'category': '股票', 'totalAssets': 2850000.00, 'percentage': 69.51},
                    {'category': '现金', 'totalAssets': 1250000.00, 'percentage': 30.49}
                ]
            })

        # 查询所有账户信息
        accounts = xt_trader.query_account_infos()
        if not accounts:
            logger.warning('未查询到账户信息')
            return JsonResponse({
                'categories': [
                    {'category': '股票', 'totalAssets': 0.00, 'percentage': 0.00},
                    {'category': '现金', 'totalAssets': 0.00, 'percentage': 0.00}
                ]
            })

        # 汇总所有账户的数据
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
                logger.warning(f'处理账户 {acc} 时出错: {str(e)}')
                continue
        
        total_assets = total_market_value + total_cash
        
        # 计算占比
        stock_percentage = (total_market_value / total_assets * 100) if total_assets > 0 else 0
        cash_percentage = (total_cash / total_assets * 100) if total_assets > 0 else 0
        
        logger.info(f'成功获取资产分类数据：股票 {total_market_value:.2f}，现金 {total_cash:.2f}')
        return JsonResponse({
            'categories': [
                {
                    'category': '股票',
                    'totalAssets': round(total_market_value, 2),
                    'percentage': round(stock_percentage, 2)
                },
                {
                    'category': '现金',
                    'totalAssets': round(total_cash, 2),
                    'percentage': round(cash_percentage, 2)
                }
            ]
        })
        
    except Exception as e:
        logger.error(f'获取资产分类数据失败: {str(e)}', exc_info=True)
        # 发生错误时返回模拟数据
        return JsonResponse({
            'categories': [
                {'category': '股票', 'totalAssets': 2850000.00, 'percentage': 69.51},
                {'category': '现金', 'totalAssets': 1250000.00, 'percentage': 30.49}
            ]
        })


@api_view(['GET'])
def get_region_data(request):
    """
    获取地区分布数据
    API文档: /api/region-data/
    根据股票上市地区进行统计
    符合前端数据格式要求：使用regions字段，region和totalAssets字段名
    """
    logger.info('获取地区分布数据')
    
    # 检查是否使用模拟数据
    use_mock = request.GET.get('mock', 'true').lower() == 'true'
    
    if use_mock:
        # 模拟数据 - 符合前端格式要求
        region_data = {
            'regions': [  # 前端要求使用regions字段
                {
                    'region': '上海',  # 前端要求使用region字段
                    'totalAssets': 1353500.00,  # 前端要求使用totalAssets字段
                    'percentage': 28.77
                },
                {
                    'region': '深圳',
                    'totalAssets': 712500.00,
                    'percentage': 25.00
                },
                {
                    'region': '北京',
                    'totalAssets': 570000.00,
                    'percentage': 20.00
                },
                {
                    'region': '广州',
                    'totalAssets': 342000.00,
                    'percentage': 12.00
                },
                {
                    'region': '杭州',
                    'totalAssets': 228000.00,
                    'percentage': 8.00
                },
                {
                    'region': '其他',
                    'totalAssets': 177500.00,
                    'percentage': 6.23
                }
            ]
        }
        return JsonResponse(region_data)
    
    try:
        logger.info('开始获取地区分布数据（真实数据）')
        
        # 使用统一的交易接口连接工具
        xt_trader, connected = get_xt_trader_connection()
        if not connected:
            logger.error('连接交易接口失败')
            logger.info('自动切换到模拟数据模式')
            return JsonResponse({
                'regions': [
                    {'region': '上海', 'totalAssets': 1353500.00, 'percentage': 28.77}
                ]
            })

        # 查询所有账户信息
        accounts = xt_trader.query_account_infos()
        if not accounts:
            logger.warning('未查询到账户信息')
            return JsonResponse({'regions': []})

        # 获取股票地区信息
        from apps.utils.stock_info import get_stock_region
        
        # 按地区汇总
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
                logger.warning(f'处理账户 {acc} 时出错: {str(e)}')
                continue
        
        # 计算占比并转换为列表
        region_list = []
        for region, assets in region_data_dict.items():
            percentage = (assets / total_market_value * 100) if total_market_value > 0 else 0
            region_list.append({
                'region': region,
                'totalAssets': round(assets, 2),
                'percentage': round(percentage, 2)
            })
        
        # 按总资产降序排序
        region_list.sort(key=lambda x: x['totalAssets'], reverse=True)
        
        logger.info(f'成功获取 {len(region_list)} 个地区的数据')
        return JsonResponse({
            'regions': region_list
        })
        
    except Exception as e:
        logger.error(f'获取地区分布数据失败: {str(e)}', exc_info=True)
        # 发生错误时返回模拟数据
        return JsonResponse({
            'regions': [
                {'region': '上海', 'totalAssets': 1353500.00, 'percentage': 28.77}
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
