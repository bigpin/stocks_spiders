#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
抓取 gapup 训练数据入库（gapup.db）：
- 日线：池子成员股全历史（前复权，含换手/涨幅）
- 5分钟线：按半年分块抓取，股票级断点续抓（fetch_progress 记录进度）

用法：
  python scripts/gapup/fetch_data.py --test 3          # 抽池子前3只试抓
  python scripts/gapup/fetch_data.py --from-pool       # 全量抓池子成员股（3进程并行）
  python scripts/gapup/fetch_data.py --stocks sh600000 # 指定股票
"""

import argparse
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common
from common import (
    CONFIG, connect_db, import_pool_history, normalize_code,
    baostock_login_ok, is_blacklist_msg, baostock_quota_remaining,
)

logger = common.get_gapup_logger()

MINUTE_CHUNK_DAYS = 180  # 分钟线每次查询的日历天数（约120个交易日）
FETCH_WORKERS = common.CONFIG['baostock_workers']  # 控制并发（默认2）
COMPLETE_WITHIN_DAYS = 7 # 分钟进度距 end 不足 N 天视为已完成（跳过续抓）


def _fetch_worker(code, start_date, end_date):
    """子进程：拉单只股票全部数据（纯 baostock I/O，不碰数据库）。

    返回 dict：{code, daily(df|None), minute(df|None), daily_until, minute_until, error}
    """
    import pandas as pd
    out = {'code': code, 'daily': None, 'minute': None,
           'daily_until': None, 'minute_until': None, 'error': None}
    try:
        from common import fetch_daily_baostock, fetch_minute_baostock
        df = fetch_daily_baostock(code, start_date, end_date)
        if df is not None and not df.empty:
            out['daily'] = df
            out['daily_until'] = df.index[-1].strftime('%Y-%m-%d')

        chunks = []
        cur = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        while cur <= end_dt:
            chunk_end = min(cur + timedelta(days=MINUTE_CHUNK_DAYS - 1), end_dt)
            m = fetch_minute_baostock(
                code, cur.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d'))
            if m is not None and not m.empty:
                chunks.append(m)
                out['minute_until'] = m['date'].iloc[-1]
            elif out['minute_until'] is None:
                out['minute_until'] = chunk_end.strftime('%Y-%m-%d')
            cur = chunk_end + timedelta(days=1)
        if chunks:
            out['minute'] = pd.concat(chunks, ignore_index=True)
    except Exception as e:
        out['error'] = str(e)
    return out


def upsert_daily(conn, code, df):
    rows = []
    if df is not None and not df.empty:
        if 'preclose' not in df.columns or df['preclose'].isna().all():
            # 主系统 baostock 封装不保留 preclose 列，用复权前收补齐（宽度类指标依赖它）
            df = df.copy()
            df['preclose'] = df['close'].shift(1)
    for idx, r in df.iterrows():
        def _n(v):
            try:
                f = float(v)
                return f if f == f else None
            except (TypeError, ValueError):
                return None
        rows.append((code, idx.strftime('%Y-%m-%d'),
                     _n(r.get('open')), _n(r.get('high')), _n(r.get('low')), _n(r.get('close')),
                     _n(r.get('preclose')), _n(r.get('volume')), _n(r.get('amount')),
                     _n(r.get('turnover')), _n(r.get('change_rate'))))
    conn.executemany(
        "INSERT OR REPLACE INTO daily_bars VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    return len(rows)


def upsert_minute(conn, code, df):
    rows = []
    for _, r in df.iterrows():
        def _n(v):
            try:
                f = float(v)
                return f if f == f else None
            except (TypeError, ValueError):
                return None
        rows.append((code, r['date'], r['bar_start'],
                     _n(r['open']), _n(r['high']), _n(r['low']), _n(r['close']),
                     _n(r['volume']), _n(r['amount'])))
    conn.executemany(
        "INSERT OR REPLACE INTO minute_bars VALUES (?,?,?,?,?,?,?,?,?)", rows)
    return len(rows)


def persist_result(conn, res):
    """主进程：落库 + 更新进度。返回 (daily_rows, minute_rows)。"""
    code = res['code']
    d_n = m_n = 0
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if res.get('daily') is not None:
        d_n = upsert_daily(conn, code, res['daily'])
    if res.get('minute') is not None:
        m_n = upsert_minute(conn, code, res['minute'])
    conn.execute(
        "INSERT INTO fetch_progress (stock_code, daily_fetched_until, minute_fetched_until, updated_at) "
        "VALUES (?,?,?,?) ON CONFLICT(stock_code) DO UPDATE SET "
        "daily_fetched_until=COALESCE(excluded.daily_fetched_until, daily_fetched_until), "
        "minute_fetched_until=COALESCE(excluded.minute_fetched_until, minute_fetched_until), "
        "updated_at=excluded.updated_at",
        (code, res.get('daily_until'), res.get('minute_until'), now))
    conn.commit()
    return d_n, m_n


def get_pool_stocks(conn, limit=None):
    rows = conn.execute(
        "SELECT stock_code, MAX(date) FROM pool_history GROUP BY stock_code "
        "ORDER BY MAX(date) DESC").fetchall()
    codes = [normalize_code(r[0]) for r in rows]
    if limit:
        codes = codes[:limit]
    return codes


def get_top_liquid_stocks(conn, top_n, window_days=45):
    """按近 window_days 天日均成交额取全市场流动性 top N（扩大训练宇宙用）。

    依赖 daily_bars 里已有全市场日线（screener 每晚积累）；数据不足时返回空列表。
    """
    rows = conn.execute(
        "SELECT stock_code, COUNT(*) AS n, AVG(amount) AS avg_amt "
        "FROM daily_bars WHERE date >= date('now', ?) "
        "GROUP BY stock_code HAVING n >= 20 "
        "ORDER BY avg_amt DESC LIMIT ?",
        (f'-{window_days} days', top_n)).fetchall()
    return [normalize_code(r[0]) for r in rows]


def filter_incomplete(conn, codes, end_date):
    """剔除已抓完的股票（分钟进度距 end 不足 COMPLETE_WITHIN_DAYS 天）。"""
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    complete_before = (end_dt - timedelta(days=COMPLETE_WITHIN_DAYS)).strftime('%Y-%m-%d')
    prog = dict(conn.execute(
        "SELECT stock_code, minute_fetched_until FROM fetch_progress "
        "WHERE minute_fetched_until IS NOT NULL").fetchall())
    todo = [c for c in codes if not (prog.get(c) and prog[c] >= complete_before)]
    return todo


def main():
    parser = argparse.ArgumentParser(description='抓取 gapup 训练数据（baostock）')
    parser.add_argument('--test', type=int, metavar='N', help='只抓池子前N只（试抓验证）')
    parser.add_argument('--from-pool', action='store_true', help='抓池子全部成员股')
    parser.add_argument('--top', type=int, metavar='N',
                        help='按近45日均成交额抓全市场流动性top N（扩大训练宇宙，'
                             '需 daily_bars 已有全市场日线）')
    parser.add_argument('--max-stocks', type=int, metavar='N',
                        help='本次最多抓 N 只（扩宇宙建议每天 300~400 只分批跑，'
                             '避免单日请求量过大触发黑名单）')
    parser.add_argument('--stocks', nargs='+', help='指定股票代码列表')
    parser.add_argument('--workers', type=int, default=FETCH_WORKERS)
    parser.add_argument('--start', default=CONFIG['hist_start'])
    parser.add_argument('--end', default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()

    imported = import_pool_history()
    logger.info(f"池子历史导入完成（新增 {imported} 行）")

    conn = connect_db()
    if args.stocks:
        codes = [normalize_code(c) for c in args.stocks]
    elif args.top:
        codes = get_top_liquid_stocks(conn, args.top)
        if not codes:
            logger.error("daily_bars 还没有全市场日线数据，请先运行一次 screener.py 完成首抓")
            conn.close()
            return
        logger.info(f"流动性 top{args.top} 命中 {len(codes)} 只")
    elif args.test:
        codes = get_pool_stocks(conn, limit=args.test)
    elif args.from_pool:
        codes = get_pool_stocks(conn)
    else:
        parser.error('请指定 --test N / --from-pool / --top N / --stocks 之一')
        return
    codes = filter_incomplete(conn, codes, args.end)
    if args.max_stocks:
        codes = codes[:args.max_stocks]
    logger.info(f"待抓取 {len(codes)} 只（已完成的跳过），区间 {args.start} ~ {args.end}")

    # 黑名单探测：baostock 被拉黑时直接终止，避免空转加重封禁
    ok, msg = baostock_login_ok()
    if not ok:
        logger.error(f"baostock 登录失败（{msg}），任务终止。"
                     f"若为黑名单封禁通常数小时~1天自动解除，之后重跑即可断点续抓。")
        conn.close()
        return

    t0 = time.time()
    done = 0
    blacklist_streak = 0
    quota_aborted = False
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(_fetch_worker, c, args.start, args.end): c for c in codes}
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                res = fut.result()
            except Exception as e:
                logger.warning(f"[{done + 1}] {code} worker异常: {e}")
                continue
            if res['error'] and is_blacklist_msg(res['error']):
                blacklist_streak += 1
                if blacklist_streak >= 3:
                    logger.error(f"连续 {blacklist_streak} 只黑名单错误，终止本次抓取"
                                 f"（已完成 {done} 只，进度已保存，解除后重跑即可续抓）。")
                    break
            if res['error'] and '每日请求配额' in res['error']:
                logger.error(f"{res['error']}，终止本次抓取（已完成 {done} 只，"
                             f"进度已保存，明日配额恢复后重跑即可续抓）。")
                quota_aborted = True
                break
            if not res['error']:
                blacklist_streak = 0
            d_n, m_n = persist_result(conn, res)
            done += 1
            msg = f"[{done}/{len(codes)}] {code} 日线+{d_n} 分钟+{m_n}"
            if res['error']:
                logger.warning(f"{msg} 错误: {res['error']}")
            else:
                logger.info(msg)
            if done % 20 == 0:
                logger.info(f"进度 {done}/{len(codes)}，已用时 {int(time.time() - t0)}s，"
                            f"今日剩余配额 {baostock_quota_remaining()}")

    m_cnt = conn.execute("SELECT COUNT(*) FROM minute_bars").fetchone()[0]
    d_cnt = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]
    conn.close()
    logger.info(
        f"抓取完成：{len(codes)} 只，用时 {int(time.time() - t0)}s；"
        f"库内 daily_bars={d_cnt} 行，minute_bars={m_cnt} 行")


if __name__ == '__main__':
    main()
