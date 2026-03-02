#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
导出云数据库 shortcut 集合为 JSON 或 CSV，便于备份或迁移。

使用方法：
    # 导出为 JSON（默认）
    python export_shortcuts.py [--output shortcuts.json]

    # 导出为 CSV
    python export_shortcuts.py --csv [--output shortcuts.csv]
"""

import argparse
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cloudbase_lib import CloudBaseClient, get_cloudbase_config, CloudBaseError

COLLECTION_NAME = "shortcut"


def _parse_data(resp):
    data = resp.get("data", [])
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return []
    if not isinstance(data, list):
        data = [data] if data else []
    result = []
    for item in data:
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except json.JSONDecodeError:
                continue
        if isinstance(item, dict):
            result.append(item)
    return result


def get_client(dotenv: str = "") -> CloudBaseClient:
    if not dotenv:
        for path in [
            os.path.join(os.path.dirname(__file__), ".env"),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "cloudbase_lib", ".env"),
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "web", ".env"),
        ]:
            if os.path.exists(path):
                dotenv = path
                break
    cfg = get_cloudbase_config(dotenv_path=dotenv or None)
    return CloudBaseClient(cfg)


def export_json(client: CloudBaseClient, path: str) -> None:
    resp = client.where_get(
        collection=COLLECTION_NAME,
        where_obj={},
        order_by=("createdAt", "desc"),
        limit=500,
    )
    items = _parse_data(resp)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"[OK] 已导出 {len(items)} 条到 {path}")


def export_csv(client: CloudBaseClient, path: str) -> None:
    resp = client.where_get(
        collection=COLLECTION_NAME,
        where_obj={},
        order_by=("createdAt", "desc"),
        limit=500,
    )
    items = _parse_data(resp)
    if not items:
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["_id", "name", "url", "icon", "keywords", "description", "clickCount", "createdAt", "updatedAt"])
        print(f"[OK] 已导出 0 条到 {path}")
        return
    keys = ["_id", "name", "url", "icon", "keywords", "description", "clickCount", "createdAt", "updatedAt"]
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(items)
    print(f"[OK] 已导出 {len(items)} 条到 {path}")


def main():
    parser = argparse.ArgumentParser(description="导出 shortcut 集合为 JSON 或 CSV")
    parser.add_argument("--output", "-o", default="shortcuts.json", help="输出文件路径")
    parser.add_argument("--csv", action="store_true", help="导出为 CSV（否则为 JSON）")
    parser.add_argument("--dotenv", default="", help=".env 文件路径")
    args = parser.parse_args()

    try:
        client = get_client(args.dotenv)
    except Exception as e:
        print(f"[ERROR] 连接云数据库失败: {e}")
        return 1

    try:
        if args.csv:
            export_csv(client, args.output)
        else:
            export_json(client, args.output)
    except CloudBaseError as e:
        if "not exist" in str(e).lower() or "-502005" in str(e):
            print("[WARN] 集合 shortcut 不存在或为空")
            if not args.csv:
                with open(args.output, "w", encoding="utf-8") as f:
                    json.dump([], f, ensure_ascii=False, indent=2)
                print(f"[OK] 已写入空数组到 {args.output}")
            return 0
        print(f"[ERROR] 导出失败: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
