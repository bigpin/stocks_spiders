#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将本地 SQLite (stock_signals.db) 同步到微信云开发数据库。

用法：
  python scripts/cloud/sync_sqlite_to_cloud.py --full        # 全量同步
  python scripts/cloud/sync_sqlite_to_cloud.py --incremental # 增量同步（最近7天）
  python scripts/cloud/sync_sqlite_to_cloud.py               # 默认增量
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import sys as _sys, os as _os
_p = _os.path.dirname(_os.path.abspath(__file__))
while _p and _p != _os.path.dirname(_p) and not _os.path.isdir(_os.path.join(_p, 'Spiders')):
    _p = _os.path.dirname(_p)
if _p and _os.path.isdir(_os.path.join(_p, 'Spiders')) and _p not in _sys.path:
    _sys.path.insert(0, _p)
from Spiders.common.log import get_logger
logger = get_logger(__name__)

from cloudbase_lib.client import CloudBaseClient, get_cloudbase_config


DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'stock_signals.db')


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def sync_signals(client: CloudBaseClient, rows, verbose=False):
    """同步 stock_signals 表到 web_signals 集合"""
    ok = 0
    fail = 0
    for i, row in enumerate(rows):
        data = dict(row)
        doc_id = str(data.pop('id'))
        # 移除 None 值，云数据库不接受某些 null 类型
        data = {k: v for k, v in data.items() if v is not None}
        try:
            client.doc_set(collection='web_signals', doc_id=doc_id, data=data)
            ok += 1
            if verbose and (ok % 50 == 0):
                logger.info(f"  [signals] 已同步 {ok}/{len(rows)}")
        except Exception as e:
            fail += 1
            if verbose:
                logger.info(f"  [signals] 失败 doc_id={doc_id}: {e}")
        time.sleep(0.1)  # 避免限流
    return ok, fail


def sync_daily_prices(client: CloudBaseClient, rows, verbose=False):
    """同步 stock_signal_daily_prices 表到 web_daily_prices 集合"""
    ok = 0
    fail = 0
    for i, row in enumerate(rows):
        data = dict(row)
        doc_id = str(data.pop('id'))
        data = {k: v for k, v in data.items() if v is not None}
        try:
            client.doc_set(collection='web_daily_prices', doc_id=doc_id, data=data)
            ok += 1
            if verbose and (ok % 200 == 0):
                logger.info(f"  [daily_prices] 已同步 {ok}/{len(rows)}")
        except Exception as e:
            fail += 1
            if verbose:
                logger.info(f"  [daily_prices] 失败 doc_id={doc_id}: {e}")
        time.sleep(0.1)
    return ok, fail


def cleanup_cloud_daily_prices(client: CloudBaseClient, days=30, verbose=False):
    """清理云数据库中超过 N 天的日线价格数据（按 days_from_signal 字段）"""
    # 云数据库 where + remove 每次最多删 1000 条，循环删除
    import json
    cutoff = days
    total_deleted = 0

    while True:
        where_js = json.dumps({"days_from_signal": {"$gte": cutoff}})
        query = f'db.collection("web_daily_prices").where({where_js}).limit(1000).remove()'
        try:
            resp = client.database_delete(query)
            deleted = resp.get('deleted', resp.get('stats', {}).get('removed', 0))
            if verbose:
                logger.info(f"  [cloud cleanup] 本轮删除 {deleted} 条")
            total_deleted += deleted
            if deleted == 0:
                break
            time.sleep(0.2)
        except Exception as e:
            if verbose:
                logger.info(f"  [cloud cleanup] 删除异常: {e}")
            break

    return total_deleted


def main():
    parser = argparse.ArgumentParser(description='同步 SQLite 到微信云开发数据库')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--full', action='store_true', help='全量同步')
    group.add_argument('--incremental', action='store_true', help='增量同步（最近7天的数据）')
    parser.add_argument('--days', type=int, default=7, help='增量同步的天数（默认7）')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细输出')
    parser.add_argument('--skip-prices', action='store_true', help='跳过 daily_prices 同步')
    parser.add_argument('--cleanup-days', type=int, default=30, help='清理超过 N 天的日线数据（默认30）')
    parser.add_argument('--skip-cleanup', action='store_true', help='跳过云数据库清理')
    args = parser.parse_args()

    # 加载 .env
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'cloudbase_lib', '.env')
    if not os.path.exists(dotenv_path):
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'Spiders', 'web', '.env')

    cfg = get_cloudbase_config(dotenv_path=dotenv_path)
    client = CloudBaseClient(cfg)

    conn = get_db()
    cursor = conn.cursor()

    is_full = args.full or not args.incremental  # 默认全量，除非指定 --incremental

    if is_full:
        logger.info("[模式] 全量同步")
        cursor.execute("SELECT * FROM stock_signals")
        signal_rows = cursor.fetchall()
        logger.info(f"  stock_signals: {len(signal_rows)} 条")

        if not args.skip_prices:
            cursor.execute("SELECT * FROM stock_signal_daily_prices")
            price_rows = cursor.fetchall()
            logger.info(f"  stock_signal_daily_prices: {len(price_rows)} 条")
        else:
            price_rows = []
    else:
        cutoff = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
        logger.info(f"[模式] 增量同步（{cutoff} 之后的数据）")
        cursor.execute("SELECT * FROM stock_signals WHERE insert_date >= ?", (cutoff,))
        signal_rows = cursor.fetchall()
        logger.info(f"  stock_signals: {len(signal_rows)} 条")

        if not args.skip_prices:
            # 同步关联的 daily_prices
            signal_ids = [row['id'] for row in signal_rows]
            if signal_ids:
                placeholders = ','.join(['?'] * len(signal_ids))
                cursor.execute(
                    f"SELECT * FROM stock_signal_daily_prices WHERE signal_id IN ({placeholders})",
                    signal_ids
                )
                price_rows = cursor.fetchall()
            else:
                price_rows = []
            logger.info(f"  stock_signal_daily_prices: {len(price_rows)} 条")
        else:
            price_rows = []

    conn.close()

    # 清理云数据库中过期的日线数据
    if not args.skip_cleanup and not args.skip_prices:
        logger.info(f"\n清理云数据库中超过 {args.cleanup_days} 天的日线数据...")
        t0 = time.time()
        deleted = cleanup_cloud_daily_prices(client, days=args.cleanup_days, verbose=args.verbose)
        logger.info(f"  清理完成: 删除 {deleted} 条, 耗时 {int(time.time() - t0)}s")

    # 开始同步
    logger.info("\n开始同步 stock_signals → web_signals ...")
    t0 = time.time()
    sig_ok, sig_fail = sync_signals(client, signal_rows, verbose=args.verbose)
    logger.info(f"  完成: 成功 {sig_ok}, 失败 {sig_fail}, 耗时 {int(time.time() - t0)}s")

    if not args.skip_prices and price_rows:
        logger.info("\n开始同步 stock_signal_daily_prices → web_daily_prices ...")
        t0 = time.time()
        price_ok, price_fail = sync_daily_prices(client, price_rows, verbose=args.verbose)
        logger.info(f"  完成: 成功 {price_ok}, 失败 {price_fail}, 耗时 {int(time.time() - t0)}s")

    logger.info(f"\n同步完成。")


if __name__ == '__main__':
    main()
