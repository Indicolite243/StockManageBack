import os
import logging
import sys
import threading
import time
from typing import Any, Dict

from django.conf import settings

logger = logging.getLogger(__name__)

_scheduler_lock = threading.Lock()
_scheduler_started = False
_scheduler_thread = None

SKIP_COMMANDS = {
    'check',
    'shell',
    'migrate',
    'makemigrations',
    'collectstatic',
    'createsuperuser',
    'test',
    'sync_qmt_snapshots',
}


def should_start_snapshot_scheduler() -> bool:
    if not getattr(settings, 'ENABLE_QMT_SNAPSHOT_SCHEDULER', True):
        return False

    command = sys.argv[1] if len(sys.argv) > 1 else ''
    if command in SKIP_COMMANDS:
        return False
    if command and command != 'runserver':
        return False

    run_main = os.environ.get('RUN_MAIN')
    if command == 'runserver' and run_main not in (None, 'true', 'True'):
        return False

    return True


def sync_qmt_snapshots_once() -> Dict[str, Any]:
    from apps.account.views_runtime import fetch_live_accounts_from_qmt

    response = fetch_live_accounts_from_qmt()
    accounts = response.get('accounts', [])
    logger.info('QMT 快照同步完成: %s 个账户, 时间: %s', len(accounts), response.get('snapshot_time'))
    return response


def _snapshot_scheduler_loop() -> None:
    interval = max(15, int(getattr(settings, 'ACCOUNT_SNAPSHOT_SYNC_INTERVAL_SECONDS', 30)))
    logger.info('QMT 快照定时同步线程已启动，间隔 %s 秒', interval)

    while True:
        try:
            sync_qmt_snapshots_once()
        except Exception as exc:
            logger.warning('QMT 快照定时同步失败: %s', str(exc))
        time.sleep(interval)


def start_snapshot_scheduler() -> None:
    global _scheduler_started, _scheduler_thread

    if not should_start_snapshot_scheduler():
        return

    with _scheduler_lock:
        if _scheduler_started:
            return

        _scheduler_thread = threading.Thread(
            target=_snapshot_scheduler_loop,
            name='qmt-snapshot-scheduler',
            daemon=True,
        )
        _scheduler_thread.start()
        _scheduler_started = True
