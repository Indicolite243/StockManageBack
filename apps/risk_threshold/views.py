"""Risk threshold views backed by MongoDB account snapshots."""

import logging
from datetime import datetime, timedelta

import numpy as np
from django.http import JsonResponse
from rest_framework.decorators import api_view

from apps.utils.data_storage import get_account_history, get_latest_account_state

logger = logging.getLogger(__name__)


def get_mock_account_history(days=30):
    np.random.seed(42)
    base_value = 4100000.00
    current_date = datetime.now()
    history = []
    current_value = base_value
    for i in range(days, 0, -1):
        date = (current_date - timedelta(days=i)).strftime('%Y-%m-%d')
        daily_return = np.random.normal(0.001, 0.015)
        current_value = max(current_value * (1 + daily_return), base_value * 0.7)
        history.append({
            'date': date,
            'total_assets': round(current_value, 2),
            'market_value': round(current_value * 0.7, 2),
            'cash': round(current_value * 0.3, 2),
            'snapshot_time': datetime.now().isoformat(timespec='seconds'),
        })
    return history


def calculate_max_principal_loss(account_history):
    if not account_history:
        return None
    initial_capital = account_history[0]['total_assets']
    current_capital = account_history[-1]['total_assets']
    loss_amount = initial_capital - current_capital
    loss_rate = (loss_amount / initial_capital * 100) if initial_capital > 0 else 0
    return {
        'max_loss_amount': round(loss_amount, 2),
        'max_loss_rate': round(loss_rate, 2),
        'initial_capital': round(initial_capital, 2),
        'current_capital': round(current_capital, 2),
    }


def calculate_volatility(account_history):
    if not account_history or len(account_history) < 2:
        return None
    daily_returns = []
    for i in range(1, len(account_history)):
        prev_value = account_history[i - 1]['total_assets']
        curr_value = account_history[i]['total_assets']
        if prev_value > 0:
            daily_returns.append((curr_value - prev_value) / prev_value)
    if not daily_returns:
        return None
    daily_volatility = np.std(daily_returns) * 100
    annual_volatility = daily_volatility * np.sqrt(252)
    if annual_volatility < 10:
        volatility_level = 'low'
    elif annual_volatility < 20:
        volatility_level = 'medium'
    else:
        volatility_level = 'high'
    return {
        'daily_volatility': round(daily_volatility, 2),
        'annual_volatility': round(annual_volatility, 2),
        'volatility_level': volatility_level,
    }


def calculate_max_drawdown(account_history):
    if not account_history or len(account_history) < 2:
        return None
    max_drawdown = 0
    max_drawdown_amount = 0
    peak_value = account_history[0]['total_assets']
    peak_date = account_history[0]['date']
    valley_value = peak_value
    valley_date = peak_date
    current_peak = peak_value
    current_peak_date = peak_date
    for record in account_history:
        current_value = record['total_assets']
        current_date = record['date']
        if current_value > current_peak:
            current_peak = current_value
            current_peak_date = current_date
        drawdown = (current_peak - current_value) / current_peak if current_peak > 0 else 0
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            max_drawdown_amount = current_peak - current_value
            peak_value = current_peak
            peak_date = current_peak_date
            valley_value = current_value
            valley_date = current_date
    return {
        'max_drawdown': round(max_drawdown * 100, 2),
        'max_drawdown_amount': round(max_drawdown_amount, 2),
        'peak_value': round(peak_value, 2),
        'peak_date': peak_date,
        'valley_value': round(valley_value, 2),
        'valley_date': valley_date,
    }


def calculate_var(account_history, confidence_level=0.95):
    if not account_history or len(account_history) < 2:
        return None
    daily_returns = []
    for i in range(1, len(account_history)):
        prev_value = account_history[i - 1]['total_assets']
        curr_value = account_history[i]['total_assets']
        if prev_value > 0:
            daily_returns.append((curr_value - prev_value) / prev_value)
    if not daily_returns:
        return None
    current_value = account_history[-1]['total_assets']
    var_percentile = np.percentile(daily_returns, (1 - confidence_level) * 100)
    var_amount = abs(current_value * var_percentile)
    var_rate = abs(var_percentile * 100)
    return {
        'var_amount': round(var_amount, 2),
        'var_rate': round(var_rate, 2),
        'confidence_level': confidence_level * 100,
        'current_value': round(current_value, 2),
    }


def get_status_label(value, warning_threshold, danger_threshold):
    if value >= danger_threshold:
        return '危险'
    if value >= warning_threshold:
        return '预警'
    return '正常'


def map_status_code(status_label):
    return {'正常': 'normal', '预警': 'warning', '危险': 'danger'}.get(status_label, 'normal')


def get_risk_level(max_loss_rate, volatility, max_drawdown, var_rate):
    risk_score = 0
    if max_loss_rate > 20:
        risk_score += 25
    elif max_loss_rate > 10:
        risk_score += 15
    elif max_loss_rate > 5:
        risk_score += 5
    if volatility > 30:
        risk_score += 25
    elif volatility > 20:
        risk_score += 15
    elif volatility > 10:
        risk_score += 5
    if max_drawdown > 30:
        risk_score += 25
    elif max_drawdown > 20:
        risk_score += 15
    elif max_drawdown > 10:
        risk_score += 5
    if var_rate > 5:
        risk_score += 25
    elif var_rate > 3:
        risk_score += 15
    elif var_rate > 2:
        risk_score += 5
    if risk_score >= 60:
        return 'high', risk_score
    if risk_score >= 30:
        return 'medium', risk_score
    return 'low', risk_score


def get_history_for_risk(account_id, days=30, allow_mock=True):
    history = get_account_history(account_id, days=days)
    if history:
        return history, False, 'mongodb_history'
    if allow_mock:
        return get_mock_account_history(days), True, 'mock'
    return [], False, 'mongodb_history'


@api_view(['GET'])
def get_risk_assessment(request):
    logger.info('收到风险评估请求')
    account_id = request.GET.get('account_id')
    days = int(request.GET.get('days', 30))
    use_mock = request.GET.get('mock', 'false').lower() == 'true'

    if not account_id:
        latest_snapshot = get_latest_account_state()
        if latest_snapshot:
            account_id = latest_snapshot.get('account_id')
    if not account_id:
        return JsonResponse({'success': False, 'error': {'code': 'MISSING_PARAMETER', 'message': '缺少 account_id 参数'}}, status=400)

    history, is_mock, data_source = get_history_for_risk(account_id, days=days, allow_mock=use_mock or True)
    if not history:
        return JsonResponse({'success': False, 'error': {'code': 'NO_HISTORY', 'message': 'MongoDB 中暂无可用历史快照'}}, status=404)

    max_loss = calculate_max_principal_loss(history)
    volatility = calculate_volatility(history)
    max_dd = calculate_max_drawdown(history)
    var = calculate_var(history, confidence_level=0.95)

    max_loss_status = get_status_label(abs(max_loss['max_loss_rate']), 10, 20)
    volatility_status = get_status_label(volatility['annual_volatility'], 20, 30)
    max_dd_status = get_status_label(max_dd['max_drawdown'], 15, 25)
    var_status = get_status_label(var['var_rate'], 3, 5)

    risk_level, risk_score = get_risk_level(
        abs(max_loss['max_loss_rate']),
        volatility['annual_volatility'],
        max_dd['max_drawdown'],
        var['var_rate'],
    )

    if risk_level == 'low':
        recommendation = '当前风险较低，可继续维持现有仓位结构。'
    elif risk_level == 'medium':
        recommendation = '当前风险处于中等水平，建议关注波动放大与回撤扩张。'
    else:
        recommendation = '当前风险偏高，建议降低仓位或增加防御配置。'

    response_data = {
        'account_id': account_id,
        'assessment_date': datetime.now().strftime('%Y-%m-%d'),
        'period_days': days,
        'period_days_available': len(history),
        'data_source': data_source,
        'snapshot_time': history[-1].get('snapshot_time') if history else None,
        'is_mock': is_mock,
        'max_principal_loss': {**max_loss, 'status': max_loss_status},
        'volatility': {**volatility, 'status': volatility_status},
        'max_drawdown': {**max_dd, 'status': max_dd_status},
        'var': {**var, 'status': var_status},
        'overall_risk': {
            'risk_level': risk_level,
            'risk_score': risk_score,
            'recommendation': recommendation,
        },
    }
    return JsonResponse(response_data)


@api_view(['GET'])
def get_max_principal_loss(request):
    account_id = request.GET.get('account_id')
    days = int(request.GET.get('days', 30))
    history, is_mock, data_source = get_history_for_risk(account_id, days=days, allow_mock=True)
    result = calculate_max_principal_loss(history) or {}
    result['is_mock'] = is_mock
    result['data_source'] = data_source
    return JsonResponse(result)


@api_view(['GET'])
def get_volatility(request):
    account_id = request.GET.get('account_id')
    days = int(request.GET.get('days', 30))
    history, is_mock, data_source = get_history_for_risk(account_id, days=days, allow_mock=True)
    result = calculate_volatility(history) or {}
    result['is_mock'] = is_mock
    result['data_source'] = data_source
    return JsonResponse(result)


@api_view(['GET'])
def get_max_drawdown(request):
    account_id = request.GET.get('account_id')
    days = int(request.GET.get('days', 30))
    history, is_mock, data_source = get_history_for_risk(account_id, days=days, allow_mock=True)
    result = calculate_max_drawdown(history) or {}
    result['is_mock'] = is_mock
    result['data_source'] = data_source
    return JsonResponse(result)


@api_view(['GET'])
def get_var_value(request):
    account_id = request.GET.get('account_id')
    days = int(request.GET.get('days', 30))
    confidence = float(request.GET.get('confidence', 0.95))
    history, is_mock, data_source = get_history_for_risk(account_id, days=days, allow_mock=True)
    result = calculate_var(history, confidence_level=confidence) or {}
    result['is_mock'] = is_mock
    result['data_source'] = data_source
    return JsonResponse(result)
