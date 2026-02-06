#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
独立脚本：更新股票列表
优先使用 baostock，失败时回退到聚合数据接口（Scrapy 子进程）。
输出写入项目根目录的 stock_list.txt。
"""

import os
import sys
import argparse
from datetime import datetime, timedelta

# 脚本在 scripts/data 目录下，项目根目录为上两级
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STOCK_LIST_FILE = "stock_list.txt"
STOCK_LIST_CACHE_DAYS = 7


def is_cache_valid(stock_file_path, cache_days=STOCK_LIST_CACHE_DAYS):
    if not os.path.exists(stock_file_path) or os.path.getsize(stock_file_path) == 0:
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(stock_file_path))
    return mtime > datetime.now() - timedelta(days=cache_days)


def main():
    parser = argparse.ArgumentParser(description="更新股票列表（写入项目根目录 stock_list.txt）")
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="强制更新，忽略缓存",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="同 --force",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=STOCK_LIST_CACHE_DAYS,
        help=f"缓存有效天数（默认 {STOCK_LIST_CACHE_DAYS}），仅在不使用 --force 时生效",
    )
    args = parser.parse_args()
    force = args.force or args.no_cache

    stock_file_path = os.path.join(PROJECT_ROOT, STOCK_LIST_FILE)

    if not force and is_cache_valid(stock_file_path, args.days):
        print(f"[INFO] 股票列表缓存有效（{args.days} 天内），跳过更新。使用 --force 强制更新。")
        print(f"      文件: {stock_file_path}")
        return 0

    # 保证能 import spiders
    sys.path.insert(0, PROJECT_ROOT)
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "Spiders"))

    # 1) 优先 baostock
    try:
        from spiders.baostock_helper import get_stock_list_baostock
        codes = get_stock_list_baostock(a_share_only=True)
        if codes:
            with open(stock_file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(codes))
            print(f"[INFO] 股票列表已更新（baostock），共 {len(codes)} 只 -> {stock_file_path}")
            return 0
        print("[WARNING] baostock 返回空列表（可能为非交易日），尝试聚合接口...")
    except Exception as e:
        print(f"[WARNING] baostock 获取失败: {e}，尝试聚合接口...")

    # 2) 回退：聚合接口（子进程 Scrapy）
    script_dir = os.path.join(PROJECT_ROOT, "Spiders")
    code = """
import sys
sys.path.insert(0, "{script_dir}")
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from spiders.get_stock_list import StockListSpider

settings = get_project_settings()
settings.set("REQUEST_FINGERPRINTER_IMPLEMENTATION", "2.7")
process = CrawlerProcess(settings)
process.crawl(StockListSpider, api_key="8371893ed4ab2b2f75b59c7fa26bf2fe")
process.start()
"""
    import subprocess
    result = subprocess.run(
        [sys.executable, "-c", code.format(script_dir=script_dir)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode == 0:
        print(f"[INFO] 股票列表已更新（聚合接口） -> {stock_file_path}")
        return 0
    print(f"[ERROR] 更新失败 (exit code {result.returncode})")
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
