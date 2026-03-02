#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
管理云数据库 shortcut 集合：添加、更新、删除、重置点击次数。
添加时若 URL 为网页链接且未传 --icon，会尝试从该页抓取 og:image / icon 作为 icon 链接写入数据库。

使用方法：
    # 添加（http(s) 链接会自动抓取页面 icon）
    python manage_shortcuts.py --add --name "打卡" --url "https://www.icloud.com/shortcuts/xxx" [--icon "可选直接指定"] [--keywords "a,b"] [--description "说明"]

    # 更新
    python manage_shortcuts.py --update --id <_id> [--name "新名"] [--url "..." ] [--icon "..."]

    # 删除
    python manage_shortcuts.py --delete --id <_id>

    # 重置某条点击次数
    python manage_shortcuts.py --reset-clicks --id <_id>
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cloudbase_lib import CloudBaseClient, get_cloudbase_config, CloudBaseError

COLLECTION_NAME = "shortcut"

# 抓取页面时请求头，避免被当爬虫
_FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
}


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()) + "+0800"


def _gen_id():
    return "sc_" + uuid.uuid4().hex[:20]


def _absolute_icon_url(icon: str, page_url: str) -> str:
    if not icon or not page_url:
        return icon or ""
    icon = icon.strip()
    if icon.startswith("http://") or icon.startswith("https://"):
        return icon
    from urllib.parse import urljoin, urlparse
    base = page_url.rsplit("/", 1)[0] + "/" if "/" in page_url else page_url + "/"
    try:
        return urljoin(base, icon)
    except Exception:
        return icon


def fetch_icon_from_url(url: str) -> str:
    """从网页 URL 抓取 icon 链接（og:image / apple-touch-icon / icon）。仅支持 http(s)。"""
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return ""
    try:
        req = urllib.request.Request(url, headers=_FETCH_HEADERS)
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError, Exception) as e:
        print(f"[WARN] 抓取页面失败，跳过 icon: {e}")
        return ""
    # og:image
    m = re.search(r'<meta[^>]+property\s*=\s*["\']og:image["\'][^>]+content\s*=\s*["\']([^"\']+)["\']', html, re.I)
    if m:
        return _absolute_icon_url(m.group(1).strip(), url)
    m = re.search(r'<meta[^>]+content\s*=\s*["\']([^"\']+)["\'][^>]+property\s*=\s*["\']og:image["\']', html, re.I)
    if m:
        return _absolute_icon_url(m.group(1).strip(), url)
    # 页面内 id="shortcut-icon" 的元素（如 iCloud 快捷方式页）：img src、data-src 或 style 中的 url(...)
    for pattern in (
        r'<[^>]*\sid\s*=\s*["\']shortcut-icon["\'][^>]*\ssrc\s*=\s*["\']([^"\']+)["\']',
        r'<[^>]*\ssrc\s*=\s*["\']([^"\']+)["\'][^>]*\sid\s*=\s*["\']shortcut-icon["\']',
        r'<[^>]*\sid\s*=\s*["\']shortcut-icon["\'][^>]*\sdata-src\s*=\s*["\']([^"\']+)["\']',
        r'<[^>]*\sdata-src\s*=\s*["\']([^"\']+)["\'][^>]*\sid\s*=\s*["\']shortcut-icon["\']',
        r'id\s*=\s*["\']shortcut-icon["\'][^>]*url\s*\(\s*["\']?([^"\')\s]+)["\']?\s*\)',
        r'id\s*=\s*["\']shortcut-icon["\'][^>]*url\s*\(\s*["\']([^"\']+)["\']\s*\)',
    ):
        m = re.search(pattern, html, re.I)
        if m:
            u = m.group(1).strip().strip("'\"")
            if u:
                return _absolute_icon_url(u, url)
    # link rel="apple-touch-icon" or "icon"
    for rel in ("apple-touch-icon", "icon", "shortcut icon"):
        m = re.search(r'<link[^>]+rel\s*=\s*["\'](?:[^"\']*\s+)?' + re.escape(rel) + r'(?:\s+[^"\']*)?["\'][^>]+href\s*=\s*["\']([^"\']+)["\']', html, re.I)
        if not m:
            m = re.search(r'<link[^>]+href\s*=\s*["\']([^"\']+)["\'][^>]+rel\s*=\s*["\'](?:[^"\']*\s+)?' + re.escape(rel) + r'(?:\s+[^"\']*)?["\']', html, re.I)
        if m:
            return _absolute_icon_url(m.group(1).strip(), url)
    return ""


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


def add_shortcut(
    client: CloudBaseClient,
    name: str,
    url: str,
    keywords: str = "",
    description: str = "",
    icon: str = "",
    fetch_icon: bool = True,
) -> str:
    url = url.strip()
    icon = (icon or "").strip()
    if fetch_icon and not icon and (url.startswith("http://") or url.startswith("https://")):
        print("[INFO] 正在从链接页抓取 icon...")
        icon = fetch_icon_from_url(url)
        if icon:
            print(f"[INFO] 已抓取 icon: {icon[:80]}{'...' if len(icon) > 80 else ''}")
    doc_id = _gen_id()
    data = {
        "name": name.strip(),
        "url": url,
        "keywords": keywords.strip(),
        "description": description.strip(),
        "icon": icon,
        "clickCount": 0,
        "createdAt": _now_iso(),
        "updatedAt": _now_iso(),
    }
    client.doc_set(collection=COLLECTION_NAME, doc_id=doc_id, data=data)
    return doc_id


def update_shortcut(
    client: CloudBaseClient,
    doc_id: str,
    name: str = None,
    url: str = None,
    keywords: str = None,
    description: str = None,
    icon: str = None,
) -> None:
    updates = {"updatedAt": _now_iso()}
    if name is not None:
        updates["name"] = name.strip()
    if url is not None:
        updates["url"] = url.strip()
    if keywords is not None:
        updates["keywords"] = keywords.strip()
    if description is not None:
        updates["description"] = description.strip()
    if icon is not None:
        updates["icon"] = icon.strip()
    if len(updates) <= 1:
        return
    # CloudBase update: need to build update query with .update({ data: {...} })
    js = json.dumps(updates, ensure_ascii=False)
    q = f'db.collection("{COLLECTION_NAME}").doc("{doc_id}").update({{data: {js}}})'
    client.database_update(q)


def delete_shortcut(client: CloudBaseClient, doc_id: str) -> None:
    q = f'db.collection("{COLLECTION_NAME}").doc("{doc_id}").remove()'
    client.database_delete(q)


def reset_clicks(client: CloudBaseClient, doc_id: str) -> None:
    js = json.dumps({"clickCount": 0, "updatedAt": _now_iso()}, ensure_ascii=False)
    q = f'db.collection("{COLLECTION_NAME}").doc("{doc_id}").update({{data: {js}}})'
    client.database_update(q)


def main():
    parser = argparse.ArgumentParser(
        description="管理 shortcut 集合",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--add", "-a", action="store_true", help="添加快捷方式")
    group.add_argument("--update", "-u", action="store_true", help="更新指定 id 的快捷方式")
    group.add_argument("--delete", "-d", action="store_true", help="删除指定 id")
    group.add_argument("--reset-clicks", "-r", action="store_true", help="将指定 id 的点击次数置 0")

    parser.add_argument("--id", default="", help="文档 _id（update/delete/reset-clicks 时必填）")
    parser.add_argument("--name", "-n", default="", help="名称（add 必填，update 选填）")
    parser.add_argument("--url", "-l", default="", help="链接（add 必填，update 选填）")
    parser.add_argument("--keywords", "-k", default="", help="关键词，逗号分隔")
    parser.add_argument("--description", default="", help="说明")
    parser.add_argument("--icon", "-i", default="", help="icon 图片链接；不传且 url 为网页时自动从页面抓取")
    parser.add_argument("--no-fetch-icon", action="store_true", help="添加时不自动抓取页面 icon")
    parser.add_argument("--dotenv", default="", help=".env 文件路径")

    args = parser.parse_args()

    try:
        client = get_client(args.dotenv)
    except Exception as e:
        print(f"[ERROR] 连接云数据库失败: {e}")
        return 1

    if args.add:
        if not args.name or not args.url:
            print("[ERROR] --add 需要 --name 和 --url")
            return 1
        doc_id = add_shortcut(
            client, args.name, args.url,
            keywords=args.keywords, description=args.description,
            icon=args.icon,
            fetch_icon=not args.no_fetch_icon,
        )
        print(f"[OK] 已添加，_id: {doc_id}")
        return 0

    if not args.id:
        print("[ERROR] --update / --delete / --reset-clicks 需要 --id")
        return 1

    if args.update:
        try:
            update_shortcut(
                client, args.id,
                name=args.name or None,
                url=args.url or None,
                keywords=args.keywords if args.keywords else None,
                description=args.description if args.description else None,
                icon=args.icon if args.icon else None,
            )
            print(f"[OK] 已更新 {args.id}")
        except CloudBaseError as e:
            print(f"[ERROR] 更新失败: {e}")
            return 1
        return 0

    if args.delete:
        try:
            delete_shortcut(client, args.id)
            print(f"[OK] 已删除 {args.id}")
        except CloudBaseError as e:
            print(f"[ERROR] 删除失败: {e}")
            return 1
        return 0

    if args.reset_clicks:
        try:
            reset_clicks(client, args.id)
            print(f"[OK] 已重置点击次数 {args.id}")
        except CloudBaseError as e:
            print(f"[ERROR] 重置失败: {e}")
            return 1
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
