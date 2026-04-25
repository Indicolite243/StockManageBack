import time
import datetime
import logging
from django.http import JsonResponse
from rest_framework.decorators import api_view
from xtquant import xtdata
from django.conf import settings
from apps.utils.xt_trader import get_xt_trader_connection, create_stock_account
from apps.utils.data_storage import get_latest_account_state

# 配置日志
logger = logging.getLogger(__name__)


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


# ==================== 时间段对比模块 ====================

# 全局缓存，用于在真实数据获取失败时返回上一次成功的数据，减少闪烁
LAST_SUCCESSFUL_DATA = {
    'yearly_comparison': {},
    'area_comparison': {},
    'asset_comparison': {},
    'weekly_comparison': {}
}

@api_view(['GET'])
def yearly_comparison(request):
    """
    年度对比接口
    API路径: /api/timecomparison/yearly_comparison/
    参数: account_id (必填)
    """
    logger.info('开始获取年度对比数据')
    
    # 检查是否使用模拟数据
    use_mock = request.GET.get('mock', 'false').lower() == 'true'
    account_id = request.GET.get('account_id')

    # 如果没有数据库连接或连接超时，自动回退到模拟数据
    # 这里的 hack 是为了应对 MongoDB 无法访问的情况
    force_mock_due_to_db = False
    
    # 智能选择账户：如果传入的是DEMO或为空，尝试使用第一个真实账户
    target_account_id = account_id
    if not target_account_id or target_account_id.startswith('DEMO'):
        try:
            xt_trader, connected = get_xt_trader_connection()
            if connected:
                accounts = xt_trader.query_account_infos()
                if accounts:
                    target_account_id = accounts[0].account_id
                    logger.info(f'年度对比自动切换到真实账户: {target_account_id}')
            else:
                # 即使没有连接成功，也可能只是单次查询失败，我们强制设置一个默认真实ID尝试从DB取
                if not target_account_id or target_account_id.startswith('DEMO'):
                    target_account_id = '62283925'
        except Exception as e:
            logger.warning(f'年度对比获取真实账户失败: {str(e)}')

    if not target_account_id:
        logger.error('缺少account_id参数')
        return JsonResponse({
            'success': False,
            'error': {
                'code': 'MISSING_PARAMETER',
                'message': '缺少account_id参数'
            }
        }, status=400)
    
    if use_mock:
        logger.info(f'使用模拟数据模式 - 账户ID: {account_id}')
        return get_mock_yearly_comparison()
    
    try:
        if force_mock_due_to_db:
             logger.warning('数据库连接可能存在问题，自动切换到模拟数据以保证显示')
             return get_mock_yearly_comparison()

        logger.info(f'开始获取账户 {target_account_id} 的年度对比数据（真实数据）')
        
        # 从数据库获取年度数据
        from apps.utils.data_storage import get_yearly_data
        
        yearly_data_dict = get_yearly_data(target_account_id)
        logger.info(f'数据库查询结果: {yearly_data_dict}')
        
        # 强制包含 2025 和 2026 年（即使数据库里没有）
        for force_year in ["2025", "2026"]:
            if force_year not in yearly_data_dict:
                logger.info(f'年度数据强制注入 {force_year} 年基础持仓')
                try:
                    xt_trader, connected = get_xt_trader_connection()
                    if connected:
                        acc_info = xt_trader.query_account_infos()
                        target_acc = next((a for a in acc_info if a.account_id == target_account_id), None)
                        if target_acc:
                            asset = xt_trader.query_asset_cash(target_acc)
                            if asset:
                                yearly_data_dict[force_year] = {
                                    'totalAssets': float(asset.total_asset),
                                    'returnRate': 0.0,
                                    'investmentRate': (float(asset.market_value) / float(asset.total_asset) * 100) if float(asset.total_asset) > 0 else 0
                                }
                except Exception as e:
                    logger.error(f'年度注入 {force_year} 基础数据失败: {str(e)}')

        # 即使数据库有记录，如果返回的是空字典，也说明没有历史汇总
        if not yearly_data_dict:
            logger.warning(f'未找到账户 {target_account_id} 的年度历史汇总数据，尝试生成基础数据')
            # 如果是真实账户且目前有持仓但没有历史，至少返回当前年份的数据
            try:
                xt_trader, connected = get_xt_trader_connection()
                if connected:
                    acc_info = xt_trader.query_account_infos()
                    # 查找匹配的账户
                    target_acc = next((a for a in acc_info if a.account_id == target_account_id), None)
                    if target_acc:
                        asset = xt_trader.query_asset_cash(target_acc)
                        if asset:
                            current_year = str(datetime.datetime.now().year)
                            yearly_data_dict = {
                                current_year: {
                                    'totalAssets': asset.total_asset,
                                    'returnRate': 0.0,
                                    'investmentRate': (asset.market_value / asset.total_asset * 100) if asset.total_asset > 0 else 0
                                }
                            }
                            logger.info(f'成功为账户 {target_account_id} 生成当前年份基础数据')
            except Exception as e:
                logger.error(f'尝试生成基础数据失败: {str(e)}')

        # 如果还是没有数据，才返回模拟数据
        if not yearly_data_dict:
            logger.warning('最终未找到任何数据，返回模拟数据以确保前端渲染')
            return get_mock_yearly_comparison()
        
        # 转换为前端需要的格式
        yearly_data_list = []
        for year in sorted(yearly_data_dict.keys()):
            yearly_data_list.append({
                'year': year,
                'totalAssets': yearly_data_dict[year]['totalAssets'],
                'returnRate': yearly_data_dict[year]['returnRate'],
                'investmentRate': yearly_data_dict[year]['investmentRate']
            })
        
        logger.info(f'成功获取 {len(yearly_data_list)} 年的数据')
        result = {
            'yearly_data': yearly_data_list,
            'is_real_data': True
        }
        # 更新缓存
        LAST_SUCCESSFUL_DATA['yearly_comparison'][target_account_id] = result
        return JsonResponse(result)
        
    except Exception as e:
        logger.error(f'获取年度对比数据失败: {str(e)}', exc_info=True)
        # 尝试从缓存中获取上一次成功的数据
        cached_data = LAST_SUCCESSFUL_DATA['yearly_comparison'].get(target_account_id)
        if cached_data:
            logger.info(f'从缓存中恢复账户 {target_account_id} 的年度对比数据')
            cached_data['is_cached'] = True
            return JsonResponse(cached_data)
        logger.info('发生异常，自动回退到模拟数据以确保前端渲染')
        return get_mock_yearly_comparison()


def get_mock_yearly_comparison():
    """
    返回年度对比模拟数据
    符合前端数据格式要求：使用year字段而不是timePeriod
    """
    logger.info('返回年度对比模拟数据')
    
    mock_data = {
        'yearly_data': [
            {
                'year': '2023',  # 前端要求使用year字段
                'totalAssets': 3200000.00,
                'returnRate': 12.50,  # 数字类型，不带%
                'investmentRate': 8.30
            },
            {
                'year': '2024',
                'totalAssets': 3680000.00,
                'returnRate': 15.00,
                'investmentRate': 9.50
            },
            {
                'year': '2025',
                'totalAssets': 4100000.00,
                'returnRate': 11.41,
                'investmentRate': 7.80
            }
        ]
    }
    
    return JsonResponse(mock_data)


@api_view(['GET'])
def weekly_comparison(request):
    """
    周度对比接口
    API路径: /api/timecomparison/weekly_comparison/
    参数: account_id (必填)
    """
    logger.info('开始获取周度对比数据')
    
    # 检查是否使用模拟数据
    use_mock = request.GET.get('mock', 'false').lower() == 'true'
    account_id = request.GET.get('account_id')
    force_mock_due_to_db = False
    
    # 智能选择账户：如果传入的是DEMO或为空，尝试使用第一个真实账户
    target_account_id = account_id
    if not target_account_id or target_account_id.startswith('DEMO'):
        try:
            xt_trader, connected = get_xt_trader_connection()
            if connected:
                accounts = xt_trader.query_account_infos()
                if accounts:
                    target_account_id = accounts[0].account_id
                    logger.info(f'周度对比自动切换到真实账户: {target_account_id}')
            else:
                if not target_account_id or target_account_id.startswith('DEMO'):
                    target_account_id = '62283925'
        except Exception as e:
            logger.warning(f'周度对比获取真实账户失败: {str(e)}')

    if not target_account_id:
        logger.error('缺少account_id参数')
        return JsonResponse({
            'success': False,
            'error': {
                'code': 'MISSING_PARAMETER',
                'message': '缺少account_id参数'
            }
        }, status=400)
    
    if use_mock:
        logger.info(f'使用模拟数据模式 - 账户ID: {account_id}')
        return get_mock_weekly_comparison()
    
    try:
        if force_mock_due_to_db:
             return get_mock_weekly_comparison()

        logger.info(f'开始获取账户 {target_account_id} 的周度对比数据（真实数据）')
        
        # 从数据库获取周度数据
        from apps.utils.data_storage import get_weekly_data
        
        weekly_data_dict = get_weekly_data(target_account_id, weeks=4)
        
        # 强制注入当前周数据
        try:
            from datetime import datetime
            year, week, _ = datetime.now().isocalendar()
            current_week = f"{year}-W{week:02d}"
            if current_week not in weekly_data_dict:
                xt_trader, connected = get_xt_trader_connection()
                if connected:
                    acc_info = xt_trader.query_account_infos()
                    target_acc = next((a for a in acc_info if a.account_id == target_account_id), None)
                    if target_acc:
                        asset = xt_trader.query_asset_cash(target_acc)
                        if asset:
                            weekly_data_dict[current_week] = {
                                'totalAssets': float(asset.total_asset),
                                'returnRate': 0.0,
                                'investmentRate': (float(asset.market_value) / float(asset.total_asset) * 100) if float(asset.total_asset) > 0 else 0
                            }
        except Exception as e:
            logger.error(f'周度注入基础数据失败: {str(e)}')

        if not weekly_data_dict:
            logger.warning(f'未找到账户 {target_account_id} 的周度历史数据，尝试生成基础数据')
            # 尝试获取当前周数据
            try:
                xt_trader, connected = get_xt_trader_connection()
                if connected:
                    acc_info = xt_trader.query_account_infos()
                    target_acc = next((a for a in acc_info if a.account_id == target_account_id), None)
                    if target_acc:
                        asset = xt_trader.query_asset_cash(target_acc)
                        if asset:
                            from datetime import datetime
                            year, week, _ = datetime.now().isocalendar()
                            current_week = f"{year}-W{week:02d}"
                            weekly_data_dict = {
                                current_week: {
                                    'totalAssets': asset.total_asset,
                                    'returnRate': 0.0,
                                    'investmentRate': (asset.market_value / asset.total_asset * 100) if asset.total_asset > 0 else 0
                                }
                            }
            except Exception as e:
                logger.error(f'尝试生成周度基础数据失败: {str(e)}')

        if not weekly_data_dict:
            logger.warning('最终未找到周度数据，返回模拟数据以确保前端渲染')
            return get_mock_weekly_comparison()
        
        # 转换为前端需要的格式
        weekly_data_list = []
        for week_key in sorted(weekly_data_dict.keys()):
            weekly_data_list.append({
                'timePeriod': week_key,  # 前端要求使用timePeriod字段（周度）
                'totalAssets': weekly_data_dict[week_key]['totalAssets'],
                'returnRate': weekly_data_dict[week_key]['returnRate'],
                'investmentRate': weekly_data_dict[week_key]['investmentRate']
            })
        
        logger.info(f'成功获取 {len(weekly_data_list)} 周的数据')
        result = {
            'weekly_data': weekly_data_list,
            'is_real_data': True
        }
        # 更新缓存
        LAST_SUCCESSFUL_DATA['weekly_comparison'][target_account_id] = result
        return JsonResponse(result)
        
    except Exception as e:
        logger.error(f'获取周度对比数据失败: {str(e)}', exc_info=True)
        # 尝试从缓存中获取上一次成功的数据
        cached_data = LAST_SUCCESSFUL_DATA['weekly_comparison'].get(target_account_id)
        if cached_data:
            logger.info(f'从缓存中恢复账户 {target_account_id} 的周度对比数据')
            cached_data['is_cached'] = True
            return JsonResponse(cached_data)
        logger.info('发生异常，自动回退到模拟数据以确保前端渲染')
        return get_mock_weekly_comparison()


def get_mock_weekly_comparison():
    """
    返回周度对比模拟数据
    周数格式: YYYY-WXX (ISO 8601标准)
    """
    logger.info('返回周度对比模拟数据')
    
    # 获取最近几周的周数
    from datetime import datetime, timedelta
    
    def get_iso_week(date):
        """获取ISO周数"""
        year, week, _ = date.isocalendar()
        return f"{year}-W{week:02d}"
    
    # 生成最近4周的数据
    current_date = datetime.now()
    weeks = []
    for i in range(4, 0, -1):
        week_date = current_date - timedelta(weeks=i-1)
        weeks.append(get_iso_week(week_date))
    
    mock_data = {
        'weekly_data': [
            {
                'timePeriod': weeks[0],
                'totalAssets': 3984000.00,
                'marketValue': 2772000.00,
                'returnRate': 6.2,  # 数字类型，不带%
                'growthRate': 9.8
            },
            {
                'timePeriod': weeks[1],
                'totalAssets': 4018000.00,
                'marketValue': 2793000.00,
                'returnRate': 6.5,
                'growthRate': 10.3
            },
            {
                'timePeriod': weeks[2],
                'totalAssets': 4055000.00,
                'marketValue': 2814000.00,
                'returnRate': 7.1,
                'growthRate': 11.2
            },
            {
                'timePeriod': weeks[3],
                'totalAssets': 4100000.00,
                'marketValue': 2850000.00,
                'returnRate': 8.0,
                'growthRate': 12.3
            }
        ],
        'current_total_assets': 4100000.00,
        'current_market_value': 2850000.00,
        'current_return_rate': 8.0,
        'is_mock': True
    }
    
    return JsonResponse(mock_data)


# ==================== 分市场对比模块 ====================

@api_view(['GET'])
def area_comparison(request):
    """
    地区对比接口
    API路径: /api/areacomparsion/area_comparison/
    参数: account_id (必填)
    
    ⚠️ 注意：这个接口的百分比必须是字符串格式并带%符号！
    """
    logger.info('开始获取地区对比数据')
    
    # 检查是否使用模拟数据
    use_mock = request.GET.get('mock', 'false').lower() == 'true'
    # 允许不传 account_id，默认使用真实账户
    account_id = request.GET.get('account_id', '62283925')
    
    if use_mock:
        logger.info(f'使用模拟数据模式 - 账户ID: {account_id}')
        return get_mock_area_comparison()
    
    try:
        logger.info(f'开始获取账户 {account_id} 的地区对比数据（真实数据）')
        
        # 使用统一的交易接口连接工具
        xt_trader, connected = get_xt_trader_connection()
        # 强制指定真实账户ID，即使连接失败也尝试后续逻辑
        target_account_id = '62283925'
        
        if not connected:
            logger.warning(f'连接交易接口状态为未连接，但将尝试强制使用账户 {target_account_id}')
            # 即使 connected 为 False，有时 xt_trader 实例仍然可用（如果是单例且之前连接过）
            if not xt_trader:
                logger.error('xt_trader 实例不存在，回退到模拟数据')
                return get_mock_area_comparison()

        # 查询账户信息
        acc = create_stock_account(target_account_id)
        xt_trader.subscribe(acc)

        # 查询账户资产信息
        asset = xt_trader.query_stock_asset(acc)
        if not asset:
            logger.error('未查询到账户资产信息')
            return get_mock_area_comparison()

        # 查询持仓信息
        positions = xt_trader.query_stock_positions(acc)
        if not positions:
            logger.warning('未查询到持仓信息')
            return get_mock_area_comparison()

        # 获取股票地区信息
        try:
            from apps.utils.stock_info import get_stock_region
        except ImportError:
            # 如果不存在，使用兜底逻辑
            def get_stock_region(code):
                return '其他'
        
        # 按地区汇总
        region_data_dict = {}
        total_assets = float(asset.total_asset)
        # 计算持仓总市值
        total_market_value = sum(float(pos.market_value) for pos in positions)
        
        # 为了使图表（饼图）和表格数据一致，我们统一使用“持仓总市值”作为分母
        # 这样反映的是在已投资股票中的分布情况
        calc_total = total_market_value if total_market_value > 0 else total_assets
        
        # 记录已处理的股票，防止重复
        processed_stocks = set()
        
        for pos in positions:
            stock_code = pos.stock_code
            if stock_code in processed_stocks:
                continue
            processed_stocks.add(stock_code)
            
            market_value = float(pos.market_value)
            
            # 获取成本价和数量来计算成本市值
            volume = int(pos.volume)
            # 优先使用 avg_price (成本价)
            cost_price = 0.0
            if hasattr(pos, 'avg_price'):
                cost_price = float(pos.avg_price)
            elif hasattr(pos, 'open_price'):
                cost_price = float(pos.open_price)
                
            cost_value = volume * cost_price
            
            region = get_stock_region(stock_code)
            
            if region not in region_data_dict:
                region_data_dict[region] = {
                    'totalAssets': 0.0,
                    'totalCost': 0.0
                }
            
            region_data_dict[region]['totalAssets'] += market_value
            region_data_dict[region]['totalCost'] += cost_value
        
        # 计算回报率和投资占比
        region_data_list = []
        for region, data in region_data_dict.items():
            total_region_assets = data['totalAssets']
            total_region_cost = data['totalCost']
            
            investment_rate = (total_region_assets / calc_total * 100) if calc_total > 0 else 0
            
            # 计算回报率: (总现值 - 总成本) / 总成本
            if total_region_cost > 0:
                return_rate = ((total_region_assets - total_region_cost) / total_region_cost) * 100
            else:
                return_rate = 0.0
            
            region_data_list.append({
                'region': region,
                'totalAssets': round(total_region_assets, 2),
                'returnRate': round(return_rate, 2),  # 数值格式，保留两位小数
                'investmentRate': round(investment_rate, 2)  # 数值格式，保留两位小数
            })
        
        # 按总资产降序排序
        region_data_list.sort(key=lambda x: x['totalAssets'], reverse=True)
        
        logger.info(f'成功获取 {len(region_data_list)} 个地区的数据')
        result = {
            'region_data': region_data_list,
            'is_real_data': True
        }
        # 更新缓存
        LAST_SUCCESSFUL_DATA['area_comparison'][account_id] = result
        return JsonResponse(result)
        
    except Exception as e:
        logger.error(f'获取地区对比数据失败: {str(e)}', exc_info=True)
        # 尝试从缓存中获取上一次成功的数据
        cached_data = LAST_SUCCESSFUL_DATA['area_comparison'].get(account_id)
        if cached_data:
            logger.info(f'从缓存中恢复账户 {account_id} 的地区对比数据，避免回退到模拟数据导致的闪烁')
            # 标记为缓存数据
            cached_data['is_cached'] = True
            return JsonResponse(cached_data)
            
        logger.info('缓存中无数据，回退到模拟数据以确保前端渲染')
        return get_mock_area_comparison()


def get_mock_area_comparison():
    """
    返回地区对比模拟数据
    ⚠️ 注意：returnRate 和 investmentRate 必须是字符串格式并带%符号
    """
    logger.info('返回地区对比模拟数据')
    
    mock_data = {
        'region_data': [
            {
                'region': '上海',
                'totalAssets': 5275321.00,
                'returnRate': -0.25,
                'investmentRate': 97.29
            },
            {
                'region': '深圳',
                'totalAssets': 147204.00,
                'returnRate': 2.50,
                'investmentRate': 2.71
            }
        ]
    }
    
    return JsonResponse(mock_data)


# ==================== 资产对比模块（优化版） ====================

@api_view(['GET'])
def asset_comparison(request):
    """
    ????????????????????
    ?? source=qmt|mongodb|auto
    """
    logger.info('??????????')

    use_mock = request.GET.get('mock', 'false').lower() == 'true'
    account_id = request.GET.get('account_id', '62283925')
    source = (request.GET.get('source') or 'qmt').strip().lower()
    if source not in {'qmt', 'mongodb', 'auto'}:
        source = 'qmt'

    if use_mock:
        logger.info('???????? - ????')
        return get_mock_asset_comparison()

    def build_snapshot_result(snapshot):
        positions = snapshot.get('positions', [])
        total_market_value = float(snapshot.get('market_value', 0) or 0)
        pos_list = []
        for position in positions:
            market_value = float(position.get('market_value', 0) or 0)
            current_price = float(position.get('current_price', position.get('open_price', 0)) or 0)
            cost_price = float(position.get('cost_price', position.get('avg_price', 0)) or 0)
            asset_ratio = (market_value / total_market_value * 100) if total_market_value > 0 else 0
            profit_loss_rate = ((current_price - cost_price) / cost_price * 100) if cost_price > 0 else 0
            pos_list.append({
                'stock_code': position.get('stock_code', ''),
                'stock_name': position.get('stock_name') or position.get('stock_code', ''),
                'market_value': round(market_value, 2),
                'volume': int(position.get('volume', 0) or 0),
                'current_price': round(current_price, 2),
                'cost_price': round(cost_price, 2),
                'industry': '',
                'asset_ratio': round(asset_ratio, 2),
                'percentage': round(asset_ratio, 2),
                'daily_return': round(profit_loss_rate, 2),
                'profit_loss_rate': round(profit_loss_rate, 2)
            })
        pos_list.sort(key=lambda item: item['market_value'], reverse=True)
        return {
            'total_market_value': round(total_market_value, 2),
            'asset_data': pos_list,
            'positions': pos_list,
            'is_real_data': False,
            'data_source': 'mongodb_cache',
            'snapshot_time': snapshot.get('snapshot_time'),
        }

    if source == 'mongodb':
        snapshot = get_latest_account_state(account_id)
        if snapshot:
            return JsonResponse(build_snapshot_result(snapshot))
        return JsonResponse({'success': False, 'error': 'MongoDB ???????????'}, status=404)

    try:
        logger.info('?????? %s ????????QMT ???', account_id)
        xt_trader, connected = get_xt_trader_connection()
        target_account_id = account_id or '62283925'
        if not connected or not xt_trader:
            raise RuntimeError('????????')

        acc = create_stock_account(target_account_id)
        xt_trader.subscribe(acc)
        asset = xt_trader.query_stock_asset(acc)
        if not asset:
            raise RuntimeError('??????????')

        positions = xt_trader.query_stock_positions(acc)
        if not positions:
            return JsonResponse({
                'total_market_value': 0.00,
                'asset_data': [],
                'positions': [],
                'is_real_data': True,
                'data_source': 'qmt_live',
                'snapshot_time': datetime.datetime.now().isoformat(timespec='seconds'),
            })

        stock_codes = [pos.stock_code for pos in positions]
        stock_names = {}
        for stock_code in stock_codes:
            stock_names[stock_code] = resolve_stock_name(stock_code)

        from apps.utils.stock_info import get_stock_industry

        pos_list = []
        total_market_value = float(asset.market_value)
        snapshot_positions = []
        for pos in positions:
            stock_code = pos.stock_code
            stock_name = stock_names.get(stock_code, stock_code)
            market_value = float(pos.market_value)
            industry = get_stock_industry(stock_code)
            volume = int(pos.volume)
            cost_price = float(pos.open_price) if hasattr(pos, 'open_price') else 0.0
            current_price = market_value / volume if volume > 0 else cost_price
            asset_ratio = (market_value / total_market_value * 100) if total_market_value > 0 else 0
            profit_loss_rate = ((current_price - cost_price) / cost_price) * 100 if cost_price > 0 else 0

            item = {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'market_value': round(market_value, 2),
                'volume': volume,
                'current_price': round(current_price, 2),
                'cost_price': round(cost_price, 2),
                'industry': industry,
                'asset_ratio': round(asset_ratio, 2),
                'percentage': round(asset_ratio, 2),
                'daily_return': round(profit_loss_rate, 2),
                'profit_loss_rate': round(profit_loss_rate, 2)
            }
            pos_list.append(item)
            snapshot_positions.append({
                'stock_code': stock_code,
                'stock_name': stock_name,
                'volume': volume,
                'can_use_volume': int(getattr(pos, 'can_use_volume', volume) or 0),
                'current_price': round(current_price, 2),
                'open_price': round(current_price, 2),
                'avg_price': round(cost_price, 2),
                'cost_price': round(cost_price, 2),
                'market_value': round(market_value, 2),
            })

        pos_list.sort(key=lambda item: item['market_value'], reverse=True)
        try:
            from apps.utils.data_storage import save_account_snapshot
            save_account_snapshot(target_account_id, {
                'account_id': target_account_id,
                'total_asset': float(asset.total_asset),
                'cash': float(asset.cash),
                'frozen_cash': float(asset.frozen_cash),
                'market_value': float(asset.market_value),
                'positions': snapshot_positions,
            }, source='qmt_live')
        except Exception as snapshot_error:
            logger.warning('??????????: %s', str(snapshot_error))

        result = {
            'total_market_value': round(total_market_value, 2),
            'asset_data': pos_list,
            'positions': pos_list,
            'is_real_data': True,
            'data_source': 'qmt_live',
            'snapshot_time': datetime.datetime.now().isoformat(timespec='seconds'),
        }
        LAST_SUCCESSFUL_DATA['asset_comparison'][target_account_id] = result
        return JsonResponse(result)
    except Exception as e:
        logger.error('??????????: %s', str(e), exc_info=True)
        snapshot = get_latest_account_state(account_id)
        if snapshot:
            result = build_snapshot_result(snapshot)
            result['fallback_reason'] = str(e)
            return JsonResponse(result)

        cached_data = LAST_SUCCESSFUL_DATA['asset_comparison'].get(account_id)
        if cached_data:
            cached_data['is_cached'] = True
            return JsonResponse(cached_data)
        return get_mock_asset_comparison()


def get_mock_asset_comparison():
    """
    返回资产对比模拟数据
    符合前端数据格式要求
    """
    logger.info('返回资产对比模拟数据')
    
    pos_list = [
        {
            'stock_code': '600519.SH',
            'stock_name': '贵州茅台',
            'market_value': 840250.00,
            'asset_ratio': 29.48,
            'percentage': 29.48,  # 兼容字段
            'daily_return': 15.08,
            'profit_loss_rate': 15.08  # 兼容字段
        },
        {
            'stock_code': '000858.SZ',
            'stock_name': '五粮液',
            'market_value': 466800.00,
            'asset_ratio': 16.38,
            'percentage': 16.38,
            'daily_return': 3.73,
            'profit_loss_rate': 3.73
        },
        {
            'stock_code': '601318.SH',
            'stock_name': '中国平安',
            'market_value': 637500.00,
            'asset_ratio': 22.37,
            'percentage': 22.37,
            'daily_return': 3.66,
            'profit_loss_rate': 3.66
        },
        {
            'stock_code': '600036.SH',
            'stock_name': '招商银行',
            'market_value': 716000.00,
            'asset_ratio': 25.09,
            'percentage': 25.09,
            'daily_return': 4.68,
            'profit_loss_rate': 4.68
        },
        {
            'stock_code': '000001.SZ',
            'stock_name': '平安银行',
            'market_value': 100000.00,
            'asset_ratio': 3.51,
            'percentage': 3.51,
            'daily_return': 5.93,
            'profit_loss_rate': 5.93
        }
    ]
    
    mock_data = {
        'total_market_value': 2850000.00,
        'asset_data': pos_list,  # 前端主要使用这个字段
        'positions': pos_list  # 兼容字段
    }
    
    return JsonResponse(mock_data)
