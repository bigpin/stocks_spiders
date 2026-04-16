#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
补全/修复 stock_detail_data.csv（估值缓存）。

思路：
- 从 stock_list.txt 读取全量股票代码
- 读取现有 stock_detail_data.csv，找出缺失的股票代码
- 对缺失代码低并发拉取 baostock K 线（只需要最后一条记录的 close/peTTM/pbMRQ 等）
- 合并去重并写回 stock_detail_data.csv

用法示例：
  python scripts/data/fill_stock_detail_data.py --workers 2 --retries 5
  python scripts/data/fill_stock_detail_data.py --only-missing --limit 200
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_STOCK_LIST = os.path.join(PROJECT_ROOT, "stock_list.txt")
DEFAULT_OUT = os.path.join(PROJECT_ROOT, "stock_detail_data.csv")


def _ensure_import_path():
    spiders_dir = os.path.join(PROJECT_ROOT, "Spiders")
    if spiders_dir not in sys.path:
        sys.path.insert(0, spiders_dir)


def _normalize_code(code: str | None) -> str | None:
    if not code:
        return None
    code = str(code).strip()
    if not code:
        return None
    if code.startswith(("sh", "sz", "bj")) and len(code) >= 8:
        return code
    if len(code) == 6 and code.isdigit():
        if code.startswith(("60", "68")):
            return f"sh{code}"
        if code.startswith(("00", "30")):
            return f"sz{code}"
        if code.startswith(("83", "87", "92", "43", "82", "88")):
            return f"bj{code}"
    return code


def _read_stock_list(path: str) -> list[str]:
    codes: list[str] = []
    if not os.path.exists(path):
        return codes
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw or raw.startswith("#"):
                continue
            code = raw.split("\t", 1)[0].strip()
            code = _normalize_code(code)
            if code:
                codes.append(code)
    return codes


CSV_FIELDS = [
    "stock_id",
    "stock_name",
    "new_price",
    "percentage_change",
    "price_change",
    "trading_volume",
    "trading_value",
    "highest_price",
    "lowest_price",
    "opening_price",
    "closing_price",
    "turnover_rate",
    "pe",
    "pb",
]


def _read_existing_csv(path: str) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for r in reader:
            code = _normalize_code(r.get("stock_id") or r.get("stock_code") or r.get("股票代码"))
            if not code:
                continue
            # 只保留关心字段（避免老文件多余列）
            rows[code] = {k: (r.get(k, "") if r.get(k, "") is not None else "") for k in CSV_FIELDS}
            rows[code]["stock_id"] = code
    return rows


def _safe_float(v):
    if v is None:
        return None
    try:
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _fetch_last_row(code: str, start_date: str | None, end_date: str | None, retries: int):
    """
    子进程执行：拉取 K 线 -> 取最后一行 -> 转为 CSV 行 dict。
    """
    _ensure_import_path()
    from spiders.baostock_helper import fetch_one_baostock_worker

    c, name, df = fetch_one_baostock_worker(
        stock_code=code,
        start_date=start_date,
        end_date=end_date,
        max_retries=retries,
        list_name=None,
    )
    if df is None or getattr(df, "empty", True):
        return code, None
    last = df.iloc[-1]
    pe = _safe_float(last.get("peTTM"))
    pb = _safe_float(last.get("pbMRQ"))

    row = {
        "stock_id": c,
        "stock_name": name or c,
        "new_price": _safe_float(last.get("close")),
        "percentage_change": _safe_float(last.get("change_rate")),
        "price_change": "",
        "trading_volume": _safe_float(last.get("volume")),
        "trading_value": _safe_float(last.get("amount")),
        "highest_price": _safe_float(last.get("high")),
        "lowest_price": _safe_float(last.get("low")),
        "opening_price": _safe_float(last.get("open")),
        "closing_price": "",
        "turnover_rate": _safe_float(last.get("turnover")),
        "pe": pe,
        "pb": pb,
    }
    # CSV 里保持空字符串而不是 None
    for k, v in list(row.items()):
        if v is None:
            row[k] = ""
    return c, row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stock-list", default=DEFAULT_STOCK_LIST)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--retries", type=int, default=5)
    ap.add_argument("--only-missing", action="store_true", help="只补缺失股票（默认也是如此；该参数用于语义明确）")
    ap.add_argument("--limit", type=int, default=0, help="最多补多少只（0=不限制）")
    ap.add_argument("--start-date", default=None, help="K线开始日期 YYYY-MM-DD（默认None=由worker内部处理）")
    ap.add_argument("--end-date", default=datetime.now().strftime("%Y-%m-%d"), help="K线结束日期 YYYY-MM-DD")
    args = ap.parse_args()

    all_codes = _read_stock_list(args.stock_list)
    existing = _read_existing_csv(args.out)

    all_set = set(all_codes)
    exist_set = set(existing.keys())
    missing = [c for c in all_codes if c not in exist_set]

    if args.limit and args.limit > 0:
        missing = missing[: args.limit]

    print(f"[fill_stock_detail_data] total in stock_list: {len(all_codes)}")
    print(f"[fill_stock_detail_data] existing csv rows: {len(existing)}")
    print(f"[fill_stock_detail_data] missing to fetch: {len(missing)} (workers={args.workers}, retries={args.retries})")

    if not missing:
        print("[fill_stock_detail_data] nothing to do.")
        return 0

    workers = max(1, int(args.workers))
    fetched_ok = 0
    fetched_fail = 0

    # 低并发补齐：避免打爆数据源
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(_fetch_last_row, code, args.start_date, args.end_date, args.retries): code
            for code in missing
        }
        for i, fut in enumerate(as_completed(futs), 1):
            code = futs[fut]
            try:
                c, row = fut.result(timeout=240)
            except Exception as e:
                fetched_fail += 1
                if i == 1 or i % 50 == 0 or i == len(missing):
                    print(f"[fill_stock_detail_data] progress {i}/{len(missing)} ok={fetched_ok} fail={fetched_fail} (last_err={e})")
                continue

            if row:
                existing[c] = row
                fetched_ok += 1
            else:
                fetched_fail += 1

            if i == 1 or i % 50 == 0 or i == len(missing):
                print(f"[fill_stock_detail_data] progress {i}/{len(missing)} ok={fetched_ok} fail={fetched_fail}")

    # 写回：按 stock_list 顺序输出，便于 diff
    tmp_path = args.out + ".tmp"
    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_FIELDS)
        for code in all_codes:
            r = existing.get(code)
            if not r:
                continue
            w.writerow([r.get(k, "") for k in CSV_FIELDS])

    os.replace(tmp_path, args.out)
    print(f"[fill_stock_detail_data] wrote: {args.out}")
    print(f"[fill_stock_detail_data] final rows: {len(existing)} (ok_added={fetched_ok}, fail={fetched_fail})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

