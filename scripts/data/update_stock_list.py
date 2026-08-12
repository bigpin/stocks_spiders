#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
独立脚本：更新股票列表
使用 baostock 获取 A 股列表，输出写入项目根目录的 stock_list.txt。
"""

import os
import sys as _sys, os as _os
_p = _os.path.dirname(_os.path.abspath(__file__))
while _p and _p != _os.path.dirname(_p) and not _os.path.isdir(_os.path.join(_p, 'Spiders')):
    _p = _os.path.dirname(_p)
if _p and _os.path.isdir(_os.path.join(_p, 'Spiders')) and _p not in _sys.path:
    _sys.path.insert(0, _p)
from Spiders.common.log import get_logger
logger = get_logger(__name__)
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
        logger.info(f"[INFO] 股票列表缓存有效（{args.days} 天内），跳过更新。使用 --force 强制更新。")
        logger.info(f"      文件: {stock_file_path}")
        return 0

    # 保证能 import spiders
    sys.path.insert(0, PROJECT_ROOT)
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "Spiders"))

    # 1) 优先 baostock
    try:
        from spiders.baostock_helper import get_stock_list_baostock_entries
        entries = get_stock_list_baostock_entries(a_share_only=True)
        if entries:
            with open(stock_file_path, "w", encoding="utf-8") as f:
                for code, nm in entries:
                    if nm:
                        f.write(f"{code}\t{nm}\n")
                    else:
                        f.write(f"{code}\n")
            logger.info(f"[INFO] 股票列表已更新（baostock），共 {len(entries)} 只 -> {stock_file_path}")
            return 0
        logger.warning("[WARNING] baostock 返回空列表（可能为非交易日）")
    except Exception as e:
        logger.warning(f"[WARNING] baostock 获取失败: {e}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
