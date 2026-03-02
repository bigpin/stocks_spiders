#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
列出云数据库 shortcut 集合中的快捷方式。

使用方法：
    # 列出全部
    python list_shortcuts.py --list

    # 按关键词过滤（本地过滤）
    python list_shortcuts.py --list --keyword "打卡"
"""

import argparse
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


def list_shortcuts(client: CloudBaseClient, keyword: str = "") -> list:
    try:
        resp = client.where_get(
            collection=COLLECTION_NAME,
            where_obj={},
            order_by=("createdAt", "desc"),
            limit=200,
        )
    except CloudBaseError as e:
        if "not exist" in str(e).lower() or "-502005" in str(e):
            return []
        raise
    items = _parse_data(resp)
    if keyword:
        kw = keyword.strip().lower()
        items = [
            i for i in items
            if kw in (i.get("name") or "").lower()
            or kw in (i.get("keywords") or "").lower()
            or kw in (i.get("description") or "").lower()
        ]
    return items


def main():
    parser = argparse.ArgumentParser(description="列出 shortcut 集合中的快捷方式")
    parser.add_argument("--list", "-l", action="store_true", help="列出所有快捷方式")
    parser.add_argument("--keyword", "-k", default="", help="按名称/关键词过滤（本地）")
    parser.add_argument("--dotenv", default="", help=".env 文件路径")
    args = parser.parse_args()

    if not args.list:
        parser.print_help()
        return 0

    try:
        client = get_client(args.dotenv)
    except Exception as e:
        print(f"[ERROR] 连接云数据库失败: {e}")
        return 1

    items = list_shortcuts(client, args.keyword)
    if not items:
        print("[INFO] 没有找到快捷方式")
        return 0

    print(f"\n{'名称':<20} {'点击':<8} {'_id':<26} 链接/说明")
    print("-" * 90)
    for i in items:
        name = (i.get("name") or "")[:18]
        clicks = i.get("clickCount", 0)
        doc_id = (i.get("_id") or "")[:24]
        url = (i.get("url") or i.get("description") or "")[:40]
        print(f"{name:<20} {clicks:<8} {doc_id:<26} {url}")
    print(f"\n共 {len(items)} 条")
    return 0


if __name__ == "__main__":
    sys.exit(main())
