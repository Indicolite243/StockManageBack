"""
URL configuration for project01 project.
"""
from django.contrib import admin
from django.urls import path, include
from django.http import JsonResponse
from rest_framework.decorators import api_view
from apps.Comparison.urls import timecomparison_urlpatterns, areacomparison_urlpatterns
from apps.Comparison.strategy_views import run_strategy, download_strategy_report

# 策略接口占位符
@api_view(['GET'])
def strategies_placeholder(request):
    return JsonResponse({
        'message': '策略功能待实现',
        'strategies': [],
        'status': 'placeholder'
    })

urlpatterns = [
    path('admin/', admin.site.urls),
    # 认证模块
    path('api/auth/', include('apps.auth.urls')),
    # 账户信息模块
    path('api/', include('apps.account.urls')),
    
    # --- 策略执行核心接口 (直接注册，确保最高优先级) ---
    path('api/run-strategy/', run_strategy, name='run-strategy'),
    path('api/download-strategy-report/', download_strategy_report, name='download-strategy-report'),
    
    # 资产对比模块
    path('api/', include('apps.Comparison.urls')),
    # 时间段对比模块
    path('api/timecomparison/', include((timecomparison_urlpatterns, 'timecomparison'))),
    # 分市场对比模块
    path('api/areacomparsion/', include((areacomparison_urlpatterns, 'areacomparsion'))),
    # 风险阈值模块
    path('api/risk-threshold/', include('apps.risk_threshold.urls')),
    # 策略列表占位
    path('api/strategies/', strategies_placeholder, name='strategies'),
]
