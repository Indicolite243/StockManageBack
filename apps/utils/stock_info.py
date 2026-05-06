"""
Instrument metadata helpers.

This module keeps a local MongoDB-backed metadata table for stocks and ETFs.
It uses a local cache first and can optionally enrich metadata from Eastmoney.
"""

from __future__ import annotations

import json
import logging
import ssl
import time
from datetime import datetime
from typing import Dict, Iterable, Literal, Optional
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from apps.utils.db import get_mongodb_db

logger = logging.getLogger(__name__)

Region = Literal["\u4e0a\u6d77", "\u6df1\u5733", "\u5317\u4eac", "\u5e7f\u5dde", "\u676d\u5dde", "\u5176\u4ed6"]

METADATA_COLLECTION_NAME = "instrument_metadata"
DEFAULT_TIMEOUT_SECONDS = 4
REMOTE_RETRY_TIMES = 2

INDUSTRY_UNKNOWN = "\u672a\u5206\u7c7b"

ETF_KEYWORD_MAP = {
    "\u94f6\u884c": "\u94f6\u884cETF",
    "\u8bc1\u5238": "\u8bc1\u5238ETF",
    "\u533b\u836f": "\u533b\u836fETF",
    "\u533b\u7597": "\u533b\u7597ETF",
    "\u82af\u7247": "\u82af\u7247ETF",
    "\u534a\u5bfc\u4f53": "\u534a\u5bfc\u4f53ETF",
    "\u519b\u5de5": "\u519b\u5de5ETF",
    "\u9152": "\u6d88\u8d39ETF",
    "\u6d88\u8d39": "\u6d88\u8d39ETF",
    "\u7ea2\u5229": "\u7ea2\u5229ETF",
    "\u65b0\u80fd\u6e90": "\u65b0\u80fd\u6e90ETF",
    "\u5149\u4f0f": "\u5149\u4f0fETF",
    "\u7164\u70ad": "\u7164\u70adETF",
    "\u6709\u8272": "\u6709\u8272ETF",
    "\u9ec4\u91d1": "\u9ec4\u91d1ETF",
    "\u901a\u4fe1": "\u901a\u4fe1ETF",
    "\u592e\u4f01": "\u592e\u4f01ETF",
    "\u56fd\u4f01": "\u56fd\u4f01ETF",
    "\u6e2f\u80a1": "\u6e2f\u80a1ETF",
    "\u6052\u751f": "\u6e2f\u80a1ETF",
    "\u7eb3\u6307": "\u6d77\u5916ETF",
    "\u6807\u666e": "\u6d77\u5916ETF",
    "\u65e5\u7ecf": "\u6d77\u5916ETF",
    "\u5fb7\u56fd": "\u6d77\u5916ETF",
    "\u6cd5\u56fd": "\u6d77\u5916ETF",
}

BROAD_ETF_KEYWORDS = (
    "\u6caa\u6df1300",
    "\u4e2d\u8bc1500",
    "\u4e2d\u8bc11000",
    "\u4e0a\u8bc150",
    "\u79d1\u521b50",
    "\u521b\u4e1a\u677f",
    "\u6df1\u8bc1100",
    "A50",
    "\u5bbd\u57fa",
    "\u5168\u6307",
    "300ETF",
    "500ETF",
    "1000ETF",
    "50ETF",
)


def _metadata_collection():
    return get_mongodb_db()[METADATA_COLLECTION_NAME]


def ensure_metadata_storage_ready() -> None:
    collection = _metadata_collection()
    collection.create_index("stock_code", unique=True, name="stock_code_unique")
    collection.create_index([("updated_at", -1)], name="updated_at_desc")
    collection.create_index([("instrument_type", 1)], name="instrument_type_asc")


def normalize_stock_code(stock_code: str) -> str:
    code = str(stock_code or "").strip().upper()
    if not code:
        return ""
    if "." in code:
        return code
    if code.startswith(("SH", "SZ", "BJ")):
        prefix = code[:2]
        raw = code[2:]
        if prefix == "SH":
            return f"{raw}.SH"
        if prefix == "SZ":
            return f"{raw}.SZ"
        return f"{raw}.BJ"
    if code.startswith(("60", "68", "50", "51", "52", "56", "58", "88")):
        return f"{code}.SH"
    if code.startswith(("00", "30", "15", "16", "12", "18")):
        return f"{code}.SZ"
    if code.startswith(("43", "83", "87")):
        return f"{code}.BJ"
    return code


def _code_part(stock_code: str) -> str:
    return normalize_stock_code(stock_code).split(".")[0]


def infer_instrument_type(stock_code: str, stock_name: str = "") -> str:
    code = _code_part(stock_code)
    name = str(stock_name or "").upper()
    if "REIT" in name:
        return "REIT"
    if "ETF" in name:
        return "ETF"
    if code.startswith(("50", "51", "52", "56", "58", "15", "16")):
        return "ETF"
    if code.startswith(("11", "12")):
        return "CONVERTIBLE_BOND"
    if code.startswith(("88", "39")) and ("\u6307\u6570" in stock_name or "INDEX" in name):
        return "INDEX"
    return "STOCK"


def classify_etf_category(stock_name: str, stock_code: str = "") -> str:
    name = str(stock_name or "")
    if any(keyword in name for keyword in BROAD_ETF_KEYWORDS):
        return "\u5bbd\u57faETF"
    for keyword, category in ETF_KEYWORD_MAP.items():
        if keyword in name:
            return category
    instrument_type = infer_instrument_type(stock_code, stock_name)
    if instrument_type == "REIT":
        return "REIT"
    if instrument_type == "ETF":
        return "ETF"
    return INDUSTRY_UNKNOWN


def _build_secid_variants(stock_code: str) -> Iterable[str]:
    normalized = normalize_stock_code(stock_code)
    if not normalized:
        return []
    raw, _, market = normalized.partition(".")
    if market == "SH":
        return (f"1.{raw}",)
    if market == "SZ":
        return (f"0.{raw}",)
    if market == "BJ":
        return (f"0.{raw}",)
    return (f"1.{raw}", f"0.{raw}")


def _http_get_json(url: str, params: Dict[str, str]) -> Dict:
    query = urlencode(params)
    request = Request(
        f"{url}?{query}",
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    context = ssl._create_unverified_context()
    with urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS, context=context) as response:
        payload = response.read().decode("utf-8", errors="ignore")
    return json.loads(payload)


def fetch_remote_instrument_metadata(stock_code: str, stock_name: str = "") -> Dict[str, str]:
    normalized = normalize_stock_code(stock_code)
    if not normalized:
        return {}

    last_error = None
    for secid in _build_secid_variants(normalized):
        for attempt in range(REMOTE_RETRY_TIMES):
            try:
                payload = _http_get_json(
                    "https://push2.eastmoney.com/api/qt/stock/get",
                    {
                        "secid": secid,
                        "fields": "f57,f58,f127,f128",
                    },
                )
                data = payload.get("data") or {}
                remote_name = str(data.get("f58") or stock_name or "").strip()
                remote_industry = str(data.get("f127") or "").strip()
                instrument_type = infer_instrument_type(normalized, remote_name or stock_name)

                if instrument_type in {"ETF", "REIT"}:
                    remote_industry = classify_etf_category(remote_name or stock_name, normalized)

                if not remote_industry:
                    remote_industry = INDUSTRY_UNKNOWN

                return {
                    "stock_code": normalized,
                    "stock_name": remote_name or stock_name or normalized,
                    "industry": remote_industry,
                    "industry_source": "eastmoney_quote",
                    "instrument_type": instrument_type,
                    "raw_sector": str(data.get("f128") or "").strip(),
                }
            except (TimeoutError, URLError, OSError, ValueError) as exc:
                last_error = exc
                time.sleep(0.25 * (attempt + 1))

    if last_error:
        logger.warning("Remote metadata fetch failed for %s: %s", normalized, last_error)

    instrument_type = infer_instrument_type(normalized, stock_name)
    fallback_industry = classify_etf_category(stock_name, normalized) if instrument_type in {"ETF", "REIT"} else INDUSTRY_UNKNOWN
    return {
        "stock_code": normalized,
        "stock_name": stock_name or normalized,
        "industry": fallback_industry,
        "industry_source": "fallback_rule",
        "instrument_type": instrument_type,
        "raw_sector": "",
    }


def upsert_instrument_metadata(metadata: Dict[str, str]) -> Optional[Dict]:
    normalized = normalize_stock_code(metadata.get("stock_code", ""))
    if not normalized:
        return None

    ensure_metadata_storage_ready()
    now = datetime.now()
    document = {
        "stock_code": normalized,
        "stock_name": metadata.get("stock_name", normalized),
        "industry": metadata.get("industry", INDUSTRY_UNKNOWN),
        "industry_source": metadata.get("industry_source", "fallback_rule"),
        "instrument_type": metadata.get("instrument_type", infer_instrument_type(normalized, metadata.get("stock_name", ""))),
        "raw_sector": metadata.get("raw_sector", ""),
        "updated_at": now,
        "updated_time": now.isoformat(timespec="seconds"),
    }
    _metadata_collection().update_one({"stock_code": normalized}, {"$set": document}, upsert=True)
    return document


def get_instrument_metadata(stock_code: str, stock_name: str = "", allow_remote: bool = False) -> Optional[Dict]:
    normalized = normalize_stock_code(stock_code)
    if not normalized:
        return None

    ensure_metadata_storage_ready()
    document = _metadata_collection().find_one({"stock_code": normalized})
    if document:
        if stock_name and not document.get("stock_name"):
            _metadata_collection().update_one(
                {"stock_code": normalized},
                {"$set": {"stock_name": stock_name, "updated_time": datetime.now().isoformat(timespec="seconds")}},
            )
            document["stock_name"] = stock_name
        return document

    if allow_remote:
        return upsert_instrument_metadata(fetch_remote_instrument_metadata(normalized, stock_name))

    instrument_type = infer_instrument_type(normalized, stock_name)
    fallback = {
        "stock_code": normalized,
        "stock_name": stock_name or normalized,
        "industry": classify_etf_category(stock_name, normalized) if instrument_type in {"ETF", "REIT"} else INDUSTRY_UNKNOWN,
        "industry_source": "fallback_rule",
        "instrument_type": instrument_type,
        "raw_sector": "",
    }
    return fallback


def sync_instrument_metadata(stock_code: str, stock_name: str = "") -> Optional[Dict]:
    metadata = fetch_remote_instrument_metadata(stock_code, stock_name)
    return upsert_instrument_metadata(metadata)


def get_stock_industry(stock_code: str, stock_name: str = "", allow_remote: bool = False) -> str:
    metadata = get_instrument_metadata(stock_code, stock_name=stock_name, allow_remote=allow_remote)
    if not metadata:
        return INDUSTRY_UNKNOWN
    return str(metadata.get("industry") or INDUSTRY_UNKNOWN)


def get_stock_region(stock_code: str) -> Region:
    if not stock_code:
        return "\u5176\u4ed6"

    code = normalize_stock_code(stock_code)
    if code.endswith(".SH"):
        return "\u4e0a\u6d77"
    if code.endswith(".SZ"):
        return "\u6df1\u5733"
    if code.endswith(".BJ"):
        return "\u5317\u4eac"
    return "\u5176\u4ed6"
