from django.urls import path
from .views import (
    asset_comparison,
    yearly_comparison,
    weekly_comparison,
    area_comparison
)
from .attribution_views import asset_attribution
from .strategy_views import run_strategy, download_strategy_report

# 时间段对比模块路由 (供主 urls.py 引用)
timecomparison_urlpatterns = [
    path('yearly_comparison/', yearly_comparison, name='yearly_comparison'),
    path('weekly_comparison/', weekly_comparison, name='weekly_comparison'),
]

# 分市场对比模块路由 (供主 urls.py 引用)
areacomparison_urlpatterns = [
    path('area_comparison/', area_comparison, name='area_comparison'),
]

# 策略与资产对比模块路由
urlpatterns = [
    path('asset_comparison/', asset_comparison, name='asset_comparison'),
    path('asset_attribution/', asset_attribution, name='asset_attribution'),
    path('run-strategy/', run_strategy, name='run-strategy'),
    path('download-strategy-report/', download_strategy_report, name='download-strategy-report'),
]
