from django.core.management.base import BaseCommand

from apps.account.snapshot_scheduler import sync_qmt_snapshots_once
from apps.utils.data_storage import get_all_latest_account_states


class Command(BaseCommand):
    help = 'Sync current QMT account snapshot into local MongoDB.'

    def handle(self, *args, **options):
        response = sync_qmt_snapshots_once()
        accounts = response.get('accounts', [])
        snapshots = get_all_latest_account_states()

        self.stdout.write(self.style.SUCCESS('QMT -> MongoDB 同步完成'))
        self.stdout.write(f"同步账户数: {len(accounts)}")
        self.stdout.write(f"最新快照数: {len(snapshots)}")
        self.stdout.write(f"同步时间: {response.get('snapshot_time')}")
        for account in accounts:
            self.stdout.write(f"- {account.get('account_id')} 持仓数: {len(account.get('positions', []))}")
