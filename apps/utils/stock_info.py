"""
Stock metadata helpers.

Currently provides a minimal `get_stock_region` implementation used by
`apps.account.views.get_region_data`.
You can later replace this with a real data source (DB, API, etc.).
"""

from typing import Literal


Region = Literal["上海", "深圳", "北京", "广州", "杭州", "其他"]


def get_stock_industry(stock_code: str) -> str:
    """
    获取股票所属行业
    这是一个占位实现，实际可结合 xtdata.get_stock_sector 或数据库获取
    """
    if not stock_code:
        return "其他"

    # 简单映射一些常见股票用于演示
    industry_map = {
        '600519': '食品饮料',
        '000858': '食品饮料',
        '601318': '非银金融',
        '600036': '银行',
        '000001': '银行',
        '600887': '食品饮料',
        '601012': '电力设备',
        '300750': '电力设备',
        '000651': '家用电器',
        '600030': '非银金融',
        '002415': '电子',
        '600276': '医药生物',
        '000333': '家用电器',
        '601888': '社会服务',
        '002594': '汽车',
        '300059': '非银金融',
    }

    code = stock_code.split('.')[0]
    return industry_map.get(code, '其他')


def get_stock_region(stock_code: str) -> Region:
    """
    Return the listing region for a given stock code.

    This is a placeholder implementation that infers region from the code
    suffix / prefix. Adjust the rules to match your real business logic.
    """
    if not stock_code:
        return "其他"

    code = stock_code.upper()

    # Very rough mapping rules – customize as needed
    if code.endswith(".SH"):
        return "上海"
    if code.endswith(".SZ"):
        return "深圳"

    # Example prefixes – tweak/remove if not needed
    if code.startswith("BJ"):
        return "北京"

    return "其他"


