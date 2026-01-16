#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CloudBase DB management helpers for collection `stock_signals` (single collection).

Subcommands:
  - get-report: fetch full report (stock_summary + signal_event list) via report_id linkage
  - query-events: query signal_event docs by simple filters
  - delete-report: delete a full report (summary + all events) by report_date+stock_code
  - stats: quick counts
  - export-report: export full report to JSON file
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

# Allow running from any working directory:
sys.path.insert(0, os.path.dirname(__file__))

from cloudbase_http import CloudBaseClient, CloudBaseError, get_cloudbase_config  # noqa: E402
from report_ids import make_report_id  # noqa: E402


def _parse_resp_data(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = resp.get("data")
    if not data:
        return []
    out: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, str):
            try:
                out.append(json.loads(item))
            except Exception:
                # Keep raw
                out.append({"_raw": item})
        elif isinstance(item, dict):
            out.append(item)
        else:
            out.append({"_raw": item})
    return out


def _print_json(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def fetch_report(
    client: CloudBaseClient,
    *,
    collection: str,
    report_id: str,
    events_page_size: int = 100,
) -> Dict[str, Any]:
    summary_resp = client.doc_get(collection=collection, doc_id=report_id)
    summary_list = _parse_resp_data(summary_resp)
    summary = summary_list[0] if summary_list else None

    events: List[Dict[str, Any]] = []
    skip = 0
    while True:
        resp = client.where_get(
            collection=collection,
            where_obj={"doc_type": "signal_event", "report_id": report_id},
            order_by=("signal_date", "asc"),
            limit=events_page_size,
            skip=skip,
        )
        batch = _parse_resp_data(resp)
        events.extend(batch)
        if len(batch) < events_page_size:
            break
        skip += events_page_size

    return {"report_id": report_id, "summary": summary, "events": events}


def cmd_get_report(args: argparse.Namespace) -> int:
    cfg = get_cloudbase_config(dotenv_path=args.dotenv or None)
    client = CloudBaseClient(cfg)
    report_id = make_report_id(args.report_date, args.stock_code)
    obj = fetch_report(client, collection=args.collection, report_id=report_id)
    _print_json(obj)
    return 0


def cmd_query_events(args: argparse.Namespace) -> int:
    cfg = get_cloudbase_config(dotenv_path=args.dotenv or None)
    client = CloudBaseClient(cfg)
    where: Dict[str, Any] = {"doc_type": "signal_event"}
    if args.report_date:
        where["report_date"] = args.report_date
    if args.stock_code:
        where["stock_code"] = args.stock_code
    if args.signal_type:
        where["signal_type"] = args.signal_type

    limit = min(max(int(args.limit), 1), 100)
    resp = client.where_get(collection=args.collection, where_obj=where, order_by=("signal_date", "asc"), limit=limit)
    events = _parse_resp_data(resp)
    _print_json({"where": where, "events": events})
    return 0


def _delete_doc(client: CloudBaseClient, *, collection: str, doc_id: str) -> None:
    q = f'db.collection("{collection}").doc("{doc_id}").remove()'
    client.database_delete(q)


def cmd_delete_report(args: argparse.Namespace) -> int:
    if not args.yes:
        print("[ERROR] delete is destructive. Add --yes to confirm.", file=sys.stderr)
        return 2

    cfg = get_cloudbase_config(dotenv_path=args.dotenv or None)
    client = CloudBaseClient(cfg)
    report_id = make_report_id(args.report_date, args.stock_code)

    # Delete events first (robust approach: list ids then delete individually).
    deleted_events = 0
    skip = 0
    while True:
        resp = client.where_get(
            collection=args.collection,
            where_obj={"doc_type": "signal_event", "report_id": report_id},
            order_by=("signal_date", "asc"),
            limit=100,
            skip=skip,
        )
        batch = _parse_resp_data(resp)
        if not batch:
            break
        for ev in batch:
            ev_id = ev.get("_id")
            if ev_id:
                _delete_doc(client, collection=args.collection, doc_id=ev_id)
                deleted_events += 1
        if len(batch) < 100:
            break
        skip += 100

    # Delete summary (report header)
    deleted_summary = 0
    try:
        _delete_doc(client, collection=args.collection, doc_id=report_id)
        deleted_summary = 1
    except CloudBaseError:
        deleted_summary = 0

    _print_json(
        {
            "report_id": report_id,
            "deleted_summary": deleted_summary,
            "deleted_events": deleted_events,
        }
    )
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    cfg = get_cloudbase_config(dotenv_path=args.dotenv or None)
    client = CloudBaseClient(cfg)
    where_events: Dict[str, Any] = {"doc_type": "signal_event"}
    where_summary: Dict[str, Any] = {"doc_type": "stock_summary"}
    if args.report_date:
        where_events["report_date"] = args.report_date
        where_summary["report_date"] = args.report_date

    ev_resp = client.where_count(collection=args.collection, where_obj=where_events)
    su_resp = client.where_count(collection=args.collection, where_obj=where_summary)

    def _count_from(resp: Dict[str, Any]) -> Optional[int]:
        if "count" in resp:
            try:
                return int(resp["count"])
            except Exception:
                pass
        data = _parse_resp_data(resp)
        if data and isinstance(data[0], dict):
            for k in ("total", "count"):
                if k in data[0]:
                    try:
                        return int(data[0][k])
                    except Exception:
                        return None
        return None

    _print_json(
        {
            "where_events": where_events,
            "where_summary": where_summary,
            "events_count": _count_from(ev_resp),
            "summary_count": _count_from(su_resp),
        }
    )
    return 0


def cmd_export_report(args: argparse.Namespace) -> int:
    cfg = get_cloudbase_config(dotenv_path=args.dotenv or None)
    client = CloudBaseClient(cfg)
    report_id = make_report_id(args.report_date, args.stock_code)
    obj = fetch_report(client, collection=args.collection, report_id=report_id)
    out_path = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"[OK] exported to {out_path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Manage CloudBase collection stock_signals (single collection)")
    p.add_argument("--collection", default="stock_signals")
    p.add_argument("--dotenv", default="", help="Optional .env path (defaults to Spiders/web/.env if exists)")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_get = sub.add_parser("get-report", help="Fetch a full report (summary + events)")
    p_get.add_argument("--report_date", required=True, help="YYYY-MM-DD")
    p_get.add_argument("--stock_code", required=True, help="e.g. sh601231")
    p_get.set_defaults(func=cmd_get_report)

    p_q = sub.add_parser("query-events", help="Query signal events with simple filters")
    p_q.add_argument("--report_date", default="")
    p_q.add_argument("--stock_code", default="")
    p_q.add_argument("--signal_type", default="")
    p_q.add_argument("--limit", default="50")
    p_q.set_defaults(func=cmd_query_events)

    p_del = sub.add_parser("delete-report", help="Delete a full report by report_date+stock_code")
    p_del.add_argument("--report_date", required=True, help="YYYY-MM-DD")
    p_del.add_argument("--stock_code", required=True, help="e.g. sh601231")
    p_del.add_argument("--yes", action="store_true", help="Confirm deletion")
    p_del.set_defaults(func=cmd_delete_report)

    p_stats = sub.add_parser("stats", help="Count docs (events/summaries) optionally by report_date")
    p_stats.add_argument("--report_date", default="")
    p_stats.set_defaults(func=cmd_stats)

    p_exp = sub.add_parser("export-report", help="Export full report to a JSON file")
    p_exp.add_argument("--report_date", required=True, help="YYYY-MM-DD")
    p_exp.add_argument("--stock_code", required=True, help="e.g. sh601231")
    p_exp.add_argument("--out", required=True, help="Output JSON path")
    p_exp.set_defaults(func=cmd_export_report)

    return p


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)
    if not args.dotenv:
        default_env = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(default_env):
            args.dotenv = default_env
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

