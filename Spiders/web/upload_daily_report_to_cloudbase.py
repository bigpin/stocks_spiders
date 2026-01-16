#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Upload one daily analysis report (kdj_signals_YYYYMMDD.txt) to CloudBase DB (single collection).

Docs written into collection (default: stock_signals):
  - stock_summary: _id == report_id == report_{report_date}_{stock_code}
  - signal_event : _id == event_{report_date}_{stock_code}_{signal_date}_{signal_type}
                 report_id field links to stock_summary
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any, Dict, List

# Allow running from any working directory:
sys.path.insert(0, os.path.dirname(__file__))

from cloudbase_http import CloudBaseClient, get_cloudbase_config  # noqa: E402
from report_ids import make_event_id, make_report_id, report_date_from_filename  # noqa: E402
from report_parser import parse_daily_report_file  # noqa: E402


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def build_docs(file_path: str) -> List[Dict[str, Any]]:
    report_date = report_date_from_filename(file_path)
    _, sections = parse_daily_report_file(file_path)
    docs: List[Dict[str, Any]] = []

    source_file = os.path.basename(file_path)
    ingested_at = _now_iso()

    for sec in sections:
        stock_code = sec.stock_code
        stock_name = sec.stock_name
        report_id = make_report_id(report_date, stock_code)

        summary_doc = {
            "_id": report_id,
            "report_id": report_id,
            "doc_type": "stock_summary",
            "report_date": report_date,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "overall_success_rate": sec.overall_success_rate,
            "total_signal_count": sec.total_signal_count,
            "total_success_count": sec.total_success_count,
            "source_file": source_file,
            "ingested_at": ingested_at,
        }
        docs.append(summary_doc)

        for ev in sec.events:
            signal_date = ev["signal_date"]
            signal_type = ev["signal_type"]
            event_id = make_event_id(report_date, stock_code, signal_date, signal_type)

            event_doc = {
                "_id": event_id,
                "doc_type": "signal_event",
                "report_id": report_id,
                "report_date": report_date,
                "stock_code": stock_code,
                "stock_name": stock_name,
                "signal_date": signal_date,
                "signal_type": signal_type,
                "signal_label": ev.get("signal_label"),
                "signal_success_rate": ev.get("signal_success_rate"),
                "signal_total": ev.get("signal_total"),
                "overall_success_rate": ev.get("overall_success_rate"),
                "close": ev.get("close"),
                "metrics": ev.get("metrics") or {},
                "source_file": source_file,
                "ingested_at": ingested_at,
            }
            docs.append(event_doc)

    return docs


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser(description="Upload daily report to CloudBase DB via HTTP API")
    p.add_argument("--file", required=True, help="Path to kdj_signals_YYYYMMDD.txt")
    p.add_argument("--collection", default="stock_signals", help="CloudBase collection name")
    p.add_argument("--dotenv", default="", help="Optional .env path (defaults to Spiders/web/.env if exists)")
    p.add_argument("--dry-run", action="store_true", help="Parse and print stats only, no upload")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    file_path = os.path.abspath(args.file)
    if not os.path.exists(file_path):
        print(f"[ERROR] file not found: {file_path}", file=sys.stderr)
        return 2

    dotenv = args.dotenv.strip()
    if not dotenv:
        default_env = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(default_env):
            dotenv = default_env

    docs = build_docs(file_path)
    summary_cnt = sum(1 for d in docs if d.get("doc_type") == "stock_summary")
    event_cnt = sum(1 for d in docs if d.get("doc_type") == "signal_event")
    print(f"[INFO] parsed docs: total={len(docs)}, summaries={summary_cnt}, events={event_cnt}")
    if args.verbose:
        # print just first 2 docs to avoid spam
        for d in docs[:2]:
            print(f"[DEBUG] sample_doc_id={d.get('_id')} type={d.get('doc_type')}")

    if args.dry_run:
        return 0

    cfg = get_cloudbase_config(dotenv_path=dotenv or None)
    client = CloudBaseClient(cfg)

    ok = 0
    for d in docs:
        doc_id = d["_id"]
        payload = dict(d)
        payload.pop("_id", None)  # set() uses doc_id separately; keep _id only as id, not data field
        client.doc_set(collection=args.collection, doc_id=doc_id, data=payload)
        ok += 1
        if ok % 50 == 0:
            print(f"[INFO] uploaded {ok}/{len(docs)} ...")

    print(f"[OK] uploaded docs: {ok}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

