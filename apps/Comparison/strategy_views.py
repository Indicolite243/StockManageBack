import os
import subprocess
import json
import logging
import sys
import glob
import math
import uuid
import hashlib
import re
from typing import Tuple
import numpy as np
from django.http import JsonResponse, FileResponse
from rest_framework.decorators import api_view, parser_classes
from rest_framework.parsers import MultiPartParser, FormParser
from django.conf import settings

logger = logging.getLogger(__name__)

# 默认使用当前的 Python 解释器环境
PYTHON_EXECUTABLE = sys.executable


def is_mindgo_strategy(script_content: str) -> bool:
    """
    判断上传脚本是否为 SuperMind / MindGo 风格策略。
    这类脚本依赖 `mindgo_api`，需要走兼容执行器。
    """
    lowered = script_content.lower()
    return (
        'mindgo_api' in lowered
        or 'from mindgo_api import *' in lowered
        or 'import mindgo_api' in lowered
    )


def sanitize_filename(filename: str) -> str:
    """Keep uploaded names readable while avoiding path traversal and odd characters."""
    base_name = os.path.basename(filename or "strategy.py")
    safe_name = re.sub(r'[^A-Za-z0-9._-]+', '_', base_name).strip('._')
    return safe_name or "strategy.py"


def build_upload_paths(upload_dir: str, original_filename: str) -> Tuple[str, str, str, str, str]:
    """
    Save each upload under a unique request id so repeated ETF.py uploads never overwrite
    earlier files or confuse later debugging.
    """
    request_id = uuid.uuid4().hex[:12]
    safe_name = sanitize_filename(original_filename)
    name_root, name_ext = os.path.splitext(safe_name)
    if not name_ext:
        name_ext = '.py'

    stored_original_name = f"{name_root}_{request_id}{name_ext}"
    stored_injected_name = f"{name_root}_{request_id}_injected{name_ext}"
    original_path = os.path.join(upload_dir, stored_original_name)
    injected_path = os.path.join(upload_dir, stored_injected_name)
    return request_id, stored_original_name, stored_injected_name, original_path, injected_path


def save_uploaded_market_files(uploaded_files, target_dir: str):
    saved_files = []
    for uploaded_file in uploaded_files:
        original_name = uploaded_file.name or "market_data.xlsx"
        safe_name = sanitize_filename(original_name)
        if not safe_name.lower().endswith(".xlsx"):
            continue

        target_path = os.path.join(target_dir, safe_name)
        with open(target_path, "wb") as destination:
            for chunk in uploaded_file.chunks():
                destination.write(chunk)

        saved_files.append({
            "original_name": original_name,
            "saved_name": safe_name,
            "saved_path": target_path,
        })
    return saved_files


def extract_missing_market_files(error_text: str):
    missing_files = []
    if not error_text:
        return missing_files

    patterns = [
        r"未找到行情文件[:：]\s*([^\r\n]+?\.xlsx)",
        r"No such file or directory[:：]?\s*'([^']+?\.xlsx)'",
        r"FileNotFoundError[:：].*?([A-Za-z0-9._-]+\.xlsx)",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, error_text, flags=re.IGNORECASE)
        for match in matches:
            missing_files.append(os.path.basename(match.strip()))

    deduped = []
    for item in missing_files:
        if item not in deduped:
            deduped.append(item)
    return deduped


def resolve_engine_type(selected_engine: str, script_content: str) -> str:
    """
    Support explicit engine selection from the UI while keeping auto-detection as a fallback.
    Valid values:
    - auto
    - mindgo
    - backtrader
    """
    normalized = (selected_engine or 'auto').strip().lower()
    if normalized not in {'auto', 'mindgo', 'backtrader'}:
        normalized = 'auto'

    if normalized == 'auto':
        return 'mindgo' if is_mindgo_strategy(script_content) else 'backtrader'
    return normalized


@api_view(['GET'])
def download_strategy_report(request):
    """
    下载本次回测生成的指定报告文件。
    API路径: /api/download-strategy-report/
    """
    try:
        etf_dir = os.path.abspath(os.path.join(settings.BASE_DIR, '..', 'ETF'))
        requested_path = (request.GET.get('path') or '').strip()
        if not requested_path:
            return JsonResponse({'status': 'error', 'message': '缺少报告文件路径参数'}, status=400)

        target_path = os.path.abspath(requested_path)
        logger.info(f">>> 下载指定报告文件: {target_path}")

        if not target_path.startswith(etf_dir):
            return JsonResponse({'status': 'error', 'message': '报告文件路径不合法'}, status=400)

        if not os.path.isfile(target_path):
            return JsonResponse({'status': 'error', 'message': '本次回测的报告文件不存在或尚未生成'}, status=404)

        response = FileResponse(open(target_path, 'rb'))
        response['Content-Type'] = 'application/octet-stream'
        response['Content-Disposition'] = f'attachment; filename="{os.path.basename(target_path)}"'
        return response
    except Exception as e:
        logger.error(f"下载报告失败: {str(e)}")
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)


def inject_date_params_into_script(script_content: str, start_date: str, end_date: str) -> str:
    """
    在用户上传的脚本开头注入环境变量设置，确保日期参数始终生效。
    无论用户脚本是新版还是旧版，注入的代码都会在脚本最早执行，
    覆盖任何后续的硬编码默认值。
    """
    injection_header = (
        "# ===== [系统自动注入] 回测日期参数 =====\n"
        "import os as _os_injected\n"
        f"_os_injected.environ['BACKTEST_START_DATE'] = '{start_date}'\n"
        f"_os_injected.environ['BACKTEST_END_DATE'] = '{end_date}'\n"
        "# ===== [注入结束] =====\n\n"
    )
    logger.info(f">>> 已注入日期参数: {start_date} ~ {end_date}")
    return injection_header + script_content


def normalize_performance_data(raw_data: dict) -> dict:
    """
    对脚本生成的原始回测数据进行规范化处理：

    1. 确保 strategy（策略收益）和 benchmark（基准收益）均从 0% 开始
       - 用户脚本可能因为 Backtrader 数据对齐问题导致基准第一个值不是 0%
       - 我们用第一个值作为基准点，重新计算所有相对收益率

    2. 重新精确计算所有关键指标：
       - 总收益率、年化收益率、最大回撤、夏普比率

    3. 重新计算超额收益（strategy - benchmark 的差值）
    """
    if raw_data.get('_skip_normalization'):
        logger.info(">>> 检测到脚本结果已按目标口径生成，跳过二次规范化")
        return raw_data

    dates = raw_data.get('dates', [])
    strategy = raw_data.get('strategy', [])
    benchmark = raw_data.get('benchmark', [])
    n = len(dates)

    if n == 0 or not strategy or not benchmark:
        logger.warning(">>> 数据为空，跳过规范化")
        return raw_data

    logger.info(f">>> 开始规范化数据：{n} 个数据点")
    logger.info(f">>> 原始 strategy[0]={strategy[0]}, strategy[-1]={strategy[-1]}")
    logger.info(f">>> 原始 benchmark[0]={benchmark[0]}, benchmark[-1]={benchmark[-1]}")

    # ---------------------------------------------------------------
    # 步骤 1: 规范化策略收益
    # 如果策略收益已经是从 0% 开始的，则不需要调整
    # 如果不是，则整体平移使第一个值为 0%
    # ---------------------------------------------------------------
    s0 = strategy[0]
    if abs(s0) > 0.01:
        # 策略收益不从 0% 开始，需要平移
        # 假设脚本用的是 (value/start_cash - 1)*100 的公式
        # 如果 s0 = X%，说明脚本第一天就有 X% 的收益，这是不正确的
        # 正确做法：把所有值都减去 s0，让第一个值为 0%
        strategy_norm = [round(s - s0, 4) for s in strategy]
        logger.info(f">>> 策略收益已平移 {-s0:.2f}%（原始首值为 {s0:.2f}%）")
    else:
        strategy_norm = [round(s, 4) for s in strategy]

    # ---------------------------------------------------------------
    # 步骤 2: 规范化基准收益
    # 基准收益的第一个值应该是 0%，如果不是则需要规范化
    # 有两种情况：
    #   a. 基准收益是累计收益率形式（如 48.94%）-> 需要整体平移
    #   b. 基准收益是相对于某个错误起点的 -> 需要重新计算
    # 最安全的方式：平移使第一个值为 0%
    # ---------------------------------------------------------------
    b0 = benchmark[0]
    if abs(b0) > 0.01:
        benchmark_norm = [round(b - b0, 4) for b in benchmark]
        logger.info(f">>> 基准收益已平移 {-b0:.2f}%（原始首值为 {b0:.2f}%）")
    else:
        benchmark_norm = [round(b, 4) for b in benchmark]

    # ---------------------------------------------------------------
    # 步骤 3: 重新计算超额收益
    # ---------------------------------------------------------------
    excess_norm = [round(s - b, 4) for s, b in zip(strategy_norm, benchmark_norm)]

    # ---------------------------------------------------------------
    # 步骤 4: 重新计算关键指标
    # ---------------------------------------------------------------
    # 4.1 总收益率
    total_return = strategy_norm[-1] if strategy_norm else 0.0

    # 4.2 年化收益率
    # 计算实际交易日数
    trading_days = n
    if trading_days > 0:
        # 使用 (1 + total_return/100)^(252/trading_days) - 1 公式
        annual_return = ((1 + total_return / 100) ** (252.0 / trading_days) - 1) * 100
    else:
        annual_return = 0.0

    # 4.3 最大回撤
    # 将策略收益率转换为净值序列（从 1.0 开始）
    nv_series = np.array([(1 + s / 100) for s in strategy_norm])
    peak = np.maximum.accumulate(nv_series)
    drawdown = (nv_series - peak) / peak
    max_dd = float(np.min(drawdown)) if len(drawdown) > 0 else 0.0

    # 4.4 夏普比率
    # 计算日收益率序列
    if len(strategy_norm) > 1:
        daily_returns = np.diff(strategy_norm)  # 每日收益率变动（百分点）
        daily_returns_pct = daily_returns / 100  # 转换为小数
        if daily_returns_pct.std() > 0:
            # 假设无风险年化利率 3%，日化为 3/252
            rf_daily = 0.03 / 252
            excess_daily = daily_returns_pct - rf_daily
            sharpe = (excess_daily.mean() / excess_daily.std()) * math.sqrt(252)
        else:
            sharpe = 0.0
    else:
        sharpe = 0.0

    # ---------------------------------------------------------------
    # 步骤 5: 组装最终数据
    # ---------------------------------------------------------------
    metrics_recalculated = {
        'total_return': f"{total_return:.2f}%",
        'annual_return': f"{annual_return:.2f}%",
        'max_drawdown': f"{abs(max_dd) * 100:.2f}%",
        'sharpe_ratio': f"{sharpe:.2f}"
    }

    logger.info(f">>> 规范化完成:")
    logger.info(f"    strategy: 首值={strategy_norm[0]:.2f}%, 末值={strategy_norm[-1]:.2f}%")
    logger.info(f"    benchmark: 首值={benchmark_norm[0]:.2f}%, 末值={benchmark_norm[-1]:.2f}%")
    logger.info(f"    metrics: {metrics_recalculated}")

    # 保留原始 metrics 中脚本自己计算的值（如果我们的计算有问题可以对比）
    original_metrics = raw_data.get('metrics', {})

    return {
        'dates': dates,
        'strategy': strategy_norm,
        'benchmark': benchmark_norm,
        'excess': excess_norm,
        'metrics': metrics_recalculated,
        '_original_metrics': original_metrics,   # 保留原始指标供调试
        '_normalized': True                        # 标记已规范化
    }


@api_view(['POST'])
@parser_classes([MultiPartParser, FormParser])
def run_strategy(request):
    """
    上传并运行策略文件
    API路径: /api/run-strategy/

    工作流程：
    1. 接收用户上传的 .py 策略文件 + 时间范围参数
    2. 在脚本开头注入日期环境变量（确保任何版本的脚本都能正确接收时间参数）
    3. 执行脚本（以 ETF 目录为工作目录，脚本可以读取本地的 xlsx 数据文件）
    4. 读取脚本生成的 strategy_performance.json
    5. 对数据进行规范化处理（确保收益率从 0% 开始，重新计算准确的指标）
    6. 返回规范化后的数据给前端
    """
    logger.info(">>> 收到策略执行请求")
    try:
        strategy_file = request.FILES.get('file')
        market_files = request.FILES.getlist('market_files')
        start_date = request.POST.get('start_date', '').strip()
        end_date = request.POST.get('end_date', '').strip()
        benchmark_symbol = request.POST.get('benchmark_symbol', '').strip()
        requested_engine = request.POST.get('engine_type', 'auto').strip().lower()
        enable_bear_protection = request.POST.get('enable_bear_protection', 'false').strip().lower() in ('1', 'true', 'yes', 'on')

        if not strategy_file:
            return JsonResponse({'status': 'error', 'message': '未上传文件'}, status=400)

        logger.info(
            f">>> 上传文件名: {strategy_file.name}, 回测时间范围: {start_date} 至 {end_date}, "
            f"benchmark={benchmark_symbol}, engine={requested_engine}, bear_protection={enable_bear_protection}"
        )

        # ---------------------------------------------------------------
        # 步骤 1: 读取上传文件内容（兼容多种编码）
        # ---------------------------------------------------------------
        raw_bytes = strategy_file.read()
        original_upload_name = strategy_file.name or 'strategy.py'
        file_hash = hashlib.md5(raw_bytes).hexdigest()
        script_content = None
        for encoding in ['utf-8', 'utf-8-sig', 'gbk', 'gb2312', 'latin-1']:
            try:
                script_content = raw_bytes.decode(encoding)
                logger.info(f">>> 成功以 {encoding} 编码读取脚本，共 {len(script_content)} 字符")
                break
            except (UnicodeDecodeError, LookupError):
                continue

        if script_content is None:
            return JsonResponse({
                'status': 'error',
                'message': '无法解码上传文件，请确保文件使用 UTF-8 或 GBK 编码'
            }, status=400)

        # ---------------------------------------------------------------
        # 步骤 2: 注入日期参数到脚本开头
        # ---------------------------------------------------------------
        engine_type = resolve_engine_type(requested_engine, script_content)

        if requested_engine == 'mindgo' and not is_mindgo_strategy(script_content):
            return JsonResponse({
                'status': 'error',
                'message': '当前脚本不像 MindGo / SuperMind 策略，请改选 Backtrader 或 Auto。'
            }, status=400)

        if requested_engine == 'backtrader' and is_mindgo_strategy(script_content):
            return JsonResponse({
                'status': 'error',
                'message': '当前脚本是 MindGo / SuperMind 风格，请改选 MindGo 或 Auto。'
            }, status=400)

        if not benchmark_symbol:
            benchmark_symbol = '000300.SH' if engine_type == 'mindgo' else '510300.SH'

        if start_date and end_date:
            script_content = inject_date_params_into_script(script_content, start_date, end_date)
        else:
            logger.warning(">>> 未提供时间范围，脚本将使用其内部默认时间")

        # ---------------------------------------------------------------
        # 步骤 3: 保存注入后的脚本
        # ---------------------------------------------------------------
        upload_dir = os.path.join(settings.BASE_DIR, 'uploads', 'strategies')
        os.makedirs(upload_dir, exist_ok=True)

        request_id, stored_original_name, stored_injected_name, original_file_path, injected_file_path = build_upload_paths(
            upload_dir,
            original_upload_name,
        )

        with open(injected_file_path, 'w', encoding='utf-8') as f:
            f.write(script_content)

        logger.info(f">>> 注入后的脚本已保存至: {injected_file_path}")

        # 同时保存原始文件
        with open(original_file_path, 'wb') as f:
            f.write(raw_bytes)
        logger.info(
            f">>> 本次请求 request_id={request_id}, original={stored_original_name}, injected={stored_injected_name}, md5={file_hash}"
        )

        # ---------------------------------------------------------------
        # 步骤 4: 执行策略脚本
        # ---------------------------------------------------------------
        # 以 ETF 目录为工作目录（用户脚本通常从这里读取 xlsx 数据文件）
        etf_dir = os.path.abspath(os.path.join(settings.BASE_DIR, '..', 'ETF'))
        result_json_path = os.path.join(etf_dir, 'strategy_performance.json')
        saved_market_files = save_uploaded_market_files(market_files, etf_dir)
        if saved_market_files:
            logger.info(f">>> ??? {len(saved_market_files)} ?????? ETF ??")

        # 删除旧的结果文件
        if os.path.exists(result_json_path):
            try:
                os.remove(result_json_path)
                logger.info(">>> 已删除旧的 strategy_performance.json")
            except Exception as e:
                logger.warning(f"无法删除旧结果文件: {e}")

        execution_data = None
        script_stderr = ""
        script_stdout = ""
        use_mindgo_runner = engine_type == 'mindgo'
        executor_type = 'mindgo_runner' if use_mindgo_runner else 'python_script'

        try:
            env = os.environ.copy()
            env['STRATEGY_NO_PLOT'] = '1'       # 禁用图形窗口弹出
            env['MPLBACKEND'] = 'Agg'            # matplotlib 使用非交互后端
            env['PYTHONIOENCODING'] = 'utf-8'    # 确保脚本输出编码正确
            env['BACKTEST_BENCHMARK'] = benchmark_symbol
            env['BACKTEST_ENABLE_BEAR_PROTECTION'] = '1' if enable_bear_protection else '0'
            env['BACKTEST_DATA_DIR'] = etf_dir

            # 双重保险：同时通过环境变量传递日期
            if start_date:
                env['BACKTEST_START_DATE'] = start_date
            if end_date:
                env['BACKTEST_END_DATE'] = end_date

            if use_mindgo_runner:
                runner_path = os.path.join(settings.BASE_DIR, 'apps', 'Comparison', 'mindgo_runner.py')
                command = [PYTHON_EXECUTABLE, runner_path, injected_file_path]
                logger.info(">>> 检测到 MindGo / SuperMind 策略，使用兼容执行器运行")
            else:
                command = [PYTHON_EXECUTABLE, injected_file_path]
                logger.info(">>> 检测到普通 Python 策略，使用原有执行方式运行")

            logger.info(f">>> 开始执行: {' '.join(command)}")
            logger.info(f">>> 工作目录: {etf_dir}")

            process = subprocess.run(
                command,
                cwd=etf_dir,          # 以 ETF 目录运行，脚本可访问本地 xlsx 文件
                env=env,
                timeout=300,          # 5分钟超时
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace'
            )

            script_stdout = process.stdout or ""
            script_stderr = process.stderr or ""

            if process.returncode != 0:
                logger.error(f">>> 脚本执行失败 (退出码 {process.returncode})")
                logger.error(f">>> stderr (前1500字符): {script_stderr[:1500]}")

                combined_error_text = f"{script_stdout}\n{script_stderr}"
                missing_market_files = extract_missing_market_files(combined_error_text)

                if missing_market_files:
                    return JsonResponse({
                        'status': 'error',
                        'message': '?????????????????????????? xlsx ???',
                        'error_code': 'missing_market_data',
                        'missing_market_files': missing_market_files,
                        'detail': script_stderr[-2000:] if script_stderr else combined_error_text[-2000:],
                    }, status=400)

                if not os.path.exists(result_json_path):
                    return JsonResponse({
                        'status': 'error',
                        'message': f'策略脚本执行失败（退出码 {process.returncode}）',
                        'detail': script_stderr[-2000:] if script_stderr else '无错误输出',
                        'stdout': script_stdout[-500:] if script_stdout else ''
                    }, status=500)
            else:
                logger.info(">>> 脚本执行成功")
                if script_stdout:
                    # 截取关键日志行
                    key_lines = [l for l in script_stdout.split('\n')
                                 if any(k in l for k in ['回测', '区间', 'START', 'END', '结束', '成功', '失败', 'ERROR'])]
                    if key_lines:
                        logger.info(f">>> 脚本关键输出:\n" + '\n'.join(key_lines[:20]))

            # ---------------------------------------------------------------
            # 步骤 5: 读取结果 JSON
            # ---------------------------------------------------------------
            if os.path.exists(result_json_path):
                with open(result_json_path, 'r', encoding='utf-8') as f:
                    execution_data = json.load(f)
                logger.info(f">>> 成功读取结果 JSON，包含 {len(execution_data.get('dates', []))} 个数据点")
            else:
                logger.error(f">>> 结果文件不存在: {result_json_path}")

        except subprocess.TimeoutExpired:
            logger.error(">>> 脚本运行超时 (300s)")
            return JsonResponse({
                'status': 'error',
                'message': '策略运行超时（超过5分钟），请检查脚本逻辑或缩短回测时间范围'
            }, status=500)
        except Exception as e:
            logger.error(f">>> 脚本运行异常: {str(e)}", exc_info=True)
            return JsonResponse({'status': 'error', 'message': f'脚本运行异常: {str(e)}'}, status=500)

        # ---------------------------------------------------------------
        # 步骤 6: 校验结果
        # ---------------------------------------------------------------
        if not execution_data:
            return JsonResponse({
                'status': 'error',
                'message': '策略运行完成但未生成性能数据文件，请确认脚本在 stop() 方法中调用了 export_performance_to_json()',
                'detail': script_stderr[-2000:] if script_stderr else '无错误输出'
            }, status=500)

        if not execution_data.get('dates') or not isinstance(execution_data['dates'], list):
            return JsonResponse({
                'status': 'error',
                'message': '策略生成的数据格式不正确（缺少有效的 dates 列表）'
            }, status=500)

        # ---------------------------------------------------------------
        # 步骤 7: 【关键】规范化数据
        # 确保策略收益和基准收益都从 0% 开始，重新精确计算所有指标
        # ---------------------------------------------------------------
        normalized_data = normalize_performance_data(execution_data)
        engine_info = execution_data.get('engine', {}) if isinstance(execution_data, dict) else {}
        artifacts = execution_data.get('artifacts', {}) if isinstance(execution_data, dict) else {}
        normalized_data['execution_meta'] = {
            'request_id': request_id,
            'uploaded_filename': original_upload_name,
            'stored_original_name': stored_original_name,
            'stored_injected_name': stored_injected_name,
            'uploaded_file_md5': file_hash,
            'executor_type': executor_type,
            'strategy_format': 'mindgo' if use_mindgo_runner else 'python',
            'requested_engine': requested_engine or 'auto',
            'resolved_engine': engine_type,
            'benchmark_symbol': benchmark_symbol,
            'bear_protection_enabled': enable_bear_protection,
            'result_json_path': result_json_path,
            'engine_info': engine_info,
            'artifacts': artifacts,
            'uploaded_market_files': saved_market_files,
        }

        dates_count = len(normalized_data['dates'])
        first_date = normalized_data['dates'][0]
        last_date = normalized_data['dates'][-1]
        final_strategy = normalized_data['strategy'][-1] if normalized_data['strategy'] else 0
        final_benchmark = normalized_data['benchmark'][-1] if normalized_data['benchmark'] else 0

        logger.info(f">>> 数据规范化完成：{dates_count} 个交易日，{first_date} ~ {last_date}")
        logger.info(f">>> 最终收益：策略={final_strategy:.2f}%，基准={final_benchmark:.2f}%")

        return JsonResponse({
            'status': 'success',
            'message': f'策略执行完成，共 {dates_count} 个交易日数据（{first_date} 至 {last_date}）',
            'data': normalized_data,
            'is_mock': False,
            'date_range': {
                'start': first_date,
                'end': last_date,
            }
        })

    except Exception as e:
        logger.error(f">>> run_strategy 异常: {str(e)}", exc_info=True)
        return JsonResponse({'status': 'error', 'message': str(e)}, status=500)
