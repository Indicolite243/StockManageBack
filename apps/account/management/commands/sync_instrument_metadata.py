from django.core.management.base import BaseCommand

from apps.utils.db import get_mongodb_db
from apps.utils.stock_info import (
    ensure_metadata_storage_ready,
    get_instrument_metadata,
    infer_instrument_type,
    normalize_stock_code,
    sync_instrument_metadata,
)


POSITION_COLLECTION_FIELDS = (
    ("latest_account_state", "positions"),
    ("account_snapshots", "positions"),
    ("account_snapshots_highfreq", "major_positions"),
    ("account_snapshots_daily", "positions"),
)


class Command(BaseCommand):
    help = "Sync instrument metadata into MongoDB and backfill stored snapshots."

    def add_arguments(self, parser):
        parser.add_argument(
            "--code",
            action="append",
            dest="codes",
            help="Optional stock code to sync. Repeatable, for example --code 600519.SH",
        )
        parser.add_argument(
            "--skip-backfill",
            action="store_true",
            help="Only sync metadata table and skip snapshot backfill.",
        )

    def handle(self, *args, **options):
        ensure_metadata_storage_ready()
        instruments = self._collect_instruments(options.get("codes") or [])
        if not instruments:
            self.stdout.write(self.style.WARNING("没有收集到可同步的股票或 ETF 代码"))
            return

        synced = 0
        failed = 0
        self.stdout.write(f"准备同步 {len(instruments)} 个代码到 instrument_metadata")
        for index, (stock_code, stock_name) in enumerate(sorted(instruments.items()), start=1):
            try:
                metadata = sync_instrument_metadata(stock_code, stock_name or "")
                synced += 1
                industry = metadata.get("industry", "--") if metadata else "--"
                instrument_type = metadata.get("instrument_type", "--") if metadata else "--"
                self.stdout.write(f"[{index}/{len(instruments)}] {stock_code} -> {industry} ({instrument_type})")
            except Exception as exc:
                failed += 1
                self.stdout.write(self.style.WARNING(f"[{index}/{len(instruments)}] {stock_code} 同步失败: {exc}"))

        updated_docs = 0
        if not options.get("skip_backfill"):
            updated_docs = self._backfill_snapshots()

        self.stdout.write(self.style.SUCCESS("行业映射表同步完成"))
        self.stdout.write(f"成功: {synced}")
        self.stdout.write(f"失败: {failed}")
        self.stdout.write(f"回填文档数: {updated_docs}")

    def _collect_instruments(self, explicit_codes):
        instruments = {}
        for raw_code in explicit_codes:
            code = normalize_stock_code(raw_code)
            if code:
                instruments[code] = ""

        db = get_mongodb_db()
        for collection_name, positions_field in POSITION_COLLECTION_FIELDS:
            collection = db[collection_name]
            cursor = collection.find({}, {positions_field: 1})
            for document in cursor:
                for position in document.get(positions_field, []) or []:
                    code = normalize_stock_code(position.get("stock_code", ""))
                    if not code:
                        continue
                    name = str(position.get("stock_name", "") or "")
                    if code not in instruments or (not instruments[code] and name):
                        instruments[code] = name
        return instruments

    def _backfill_snapshots(self):
        db = get_mongodb_db()
        updated_docs = 0
        for collection_name, positions_field in POSITION_COLLECTION_FIELDS:
            collection = db[collection_name]
            cursor = collection.find({}, {positions_field: 1})
            for document in cursor:
                positions = document.get(positions_field, []) or []
                new_positions = []
                changed = False
                for position in positions:
                    code = normalize_stock_code(position.get("stock_code", ""))
                    if not code:
                        new_positions.append(position)
                        continue

                    stock_name = str(position.get("stock_name", "") or "")
                    metadata = get_instrument_metadata(code, stock_name=stock_name, allow_remote=False) or {}
                    industry = metadata.get("industry") or position.get("industry") or "未分类"
                    instrument_type = metadata.get("instrument_type") or position.get("instrument_type") or infer_instrument_type(code, stock_name)
                    canonical_name = metadata.get("stock_name") or stock_name or code

                    updated_position = dict(position)
                    if updated_position.get("stock_code") != code:
                        updated_position["stock_code"] = code
                        changed = True
                    if updated_position.get("stock_name") != canonical_name:
                        updated_position["stock_name"] = canonical_name
                        changed = True
                    if updated_position.get("industry") != industry:
                        updated_position["industry"] = industry
                        changed = True
                    if updated_position.get("instrument_type") != instrument_type:
                        updated_position["instrument_type"] = instrument_type
                        changed = True

                    new_positions.append(updated_position)

                if changed:
                    collection.update_one({"_id": document["_id"]}, {"$set": {positions_field: new_positions}})
                    updated_docs += 1
                    self.stdout.write(f"已回填 {collection_name} / {document['_id']}")
        return updated_docs
