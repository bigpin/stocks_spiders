#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
一级筛选（晚间运行）：全市场 -> 次日候选池（~100 只）。

流程：
1. 增量抓取全市场日线到 gapup.db（baostock，3进程并行，断点续抓；
   新股票首抓仅近 initial_days 天，轻量引导）
2. 硬规则过滤（流动性/换手/动量/量能，见 CONFIG['screener']）
3. 用「日线模型」（models/gapup_daily_model.pkl，按训练结果排序）给幸存者打分
4. 取 top max_candidates 写入 candidates 表（date=次日）

用法：
  python scripts/gapup/screener.py                    # 正常晚间运行
  python scripts/gapup/screener.py --no-fetch         # 跳过抓取（已更新过）
  python scripts/gapup/screener.py --calibrate        # 只看各规则通过率（调参用）
  python scripts/gapup/screener.py --for-date 2026-09-22
"""

import argparse
import os
import pickle
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common
from common import (
    CONFIG, MODELS_DIR, connect_db, normalize_code,
    baostock_login_ok, baostock_quota_remaining, baostock_quota_limit,
)

logger = common.get_gapup_logger()

import pandas as pd

from build_dataset import daily_feature_frame, screener_rules_mask
from fetch_data import upsert_daily

STALE_DAYS = 2          # （已废弃，由 expected_latest_trading_date 取代）
INITIAL_DAYS = 60       # 新股票首抓天数
FETCH_WORKERS = common.CONFIG['baostock_workers']  # 控制并发（默认2）
QUOTA_MIN_RESUME = 500  # 抓取阶段要求的最低剩余配额（不足则跳过抓取直接筛选）


def expected_latest_trading_date(now=None):
    """当前时刻应有最新日线的交易日（baostock 约 19:00 前完成当日更新）。

    用于判断个股日线是否"新鲜"：daily_fetched_until >= 该日期即无需再抓。
    周末回退到周五；工作日 19:00 前回退到上一交易日。
    """
    now = now or datetime.now()
    d = now.date()
    if d.weekday() >= 5 or now.hour < 19:
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
    return d.strftime('%Y-%m-%d')


# ---------------------------------------------------------------------------
# 全市场日线增量抓取
# ---------------------------------------------------------------------------
def _daily_fetch_worker(code, beg, end):
    """子进程：抓日线增量。"""
    from common import fetch_daily_baostock
    try:
        df = fetch_daily_baostock(code, beg, end)
        return {'code': code, 'df': df, 'error': None}
    except Exception as e:
        return {'code': code, 'df': None, 'error': str(e)}


def read_all_market_codes():
    """全市场代码（复用主系统的 stock_list.txt，每周由主任务刷新）。"""
    from Spiders.spiders.baostock_helper import read_stock_list_txt
    path = os.path.join(common.PROJECT_ROOT, 'stock_list.txt')
    codes, _ = read_stock_list_txt(path)
    return [normalize_code(c) for c in codes]


def update_all_market_daily(conn, end_date, limit=None):
    """增量更新全市场日线，返回更新股票数。"""
    codes = read_all_market_codes()
    if limit:
        codes = codes[:limit]
    prog = dict(conn.execute(
        "SELECT stock_code, daily_fetched_until FROM fetch_progress "
        "WHERE daily_fetched_until IS NOT NULL").fetchall())
    expected = expected_latest_trading_date()
    tasks = []
    for c in codes:
        until = prog.get(c)
        if until and until >= expected:
            continue
        beg = None
        if until:
            beg = (datetime.strptime(until, '%Y-%m-%d')
                   + timedelta(days=1)).strftime('%Y-%m-%d')
        tasks.append((c, beg, end_date))
    if not tasks:
        logger.info(f"全市场日线均已是最新（预期最新交易日 {expected}），无需抓取")
        return 0
    remaining = baostock_quota_remaining()
    if remaining < min(QUOTA_MIN_RESUME, len(tasks)):
        logger.warning(f"今日剩余配额 {remaining} 不足（待抓 {len(tasks)} 只），"
                       f"跳过抓取，直接用库内已有数据筛选")
        return 0
    logger.info(f"全市场日线待更新 {len(tasks)} 只（{FETCH_WORKERS} 进程，"
                f"预期最新交易日 {expected}，今日剩余配额 {remaining}）")

    t0 = time.time()
    done = ok = 0
    quota_aborted = False
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with ProcessPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futs = {ex.submit(_daily_fetch_worker, c, beg, end_date): (c, beg) for c, beg, _ in tasks}
        for fut in as_completed(futs):
            c, beg = futs[fut]
            try:
                res = fut.result()
            except Exception as e:
                logger.warning(f"{c} worker异常: {e}")
                continue
            done += 1
            if res['error'] and '每日请求配额' in res['error']:
                logger.error(f"{res['error']}，终止本次抓取（已更新 {done} 只，"
                             f"明日配额恢复后自动续抓）。")
                quota_aborted = True
                break
            if res['error']:
                logger.warning(f"[{done}/{len(tasks)}] {c} 失败: {res['error']}")
                continue
            df = res['df']
            n = 0
            last_until = beg or (
                datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=INITIAL_DAYS)
            ).strftime('%Y-%m-%d')
            if df is not None and not df.empty:
                n = upsert_daily(conn, c, df)
                last_until = df.index[-1].strftime('%Y-%m-%d')
                ok += 1
            conn.execute(
                "INSERT INTO fetch_progress (stock_code, daily_fetched_until, updated_at) "
                "VALUES (?,?,?) ON CONFLICT(stock_code) DO UPDATE SET "
                "daily_fetched_until=excluded.daily_fetched_until, updated_at=excluded.updated_at",
                (c, last_until, now))
            conn.commit()
            if done % 200 == 0:
                logger.info(f"日线进度 {done}/{len(tasks)}，成功 {ok}，"
                            f"用时 {int(time.time() - t0)}s，今日剩余配额 {baostock_quota_remaining()}")
    if quota_aborted:
        return ok
    logger.info(f"全市场日线更新完成：{ok}/{len(tasks)} 只成功，用时 {int(time.time() - t0)}s，"
                f"今日剩余配额 {baostock_quota_remaining()}")
    return ok


# ---------------------------------------------------------------------------
# 筛选
# ---------------------------------------------------------------------------
def load_daily_model():
    path = os.path.join(MODELS_DIR, 'gapup_daily_model.pkl')
    if not os.path.exists(path):
        return None, None
    with open(path, 'rb') as f:
        bundle = pickle.load(f)
    return bundle['model'], bundle['features']


def screen_one(conn, code, cfg, as_of_date=None):
    """单股筛选：返回 (as_of日期, 硬规则通过, 日线因子行) 或 None。"""
    ddf = pd.read_sql(
        "SELECT date, open, high, low, close, preclose, volume, amount, turn, pct_chg "
        "FROM daily_bars WHERE stock_code=? ORDER BY date",
        conn, params=(code,))
    if len(ddf) < 25:
        return None
    if as_of_date:
        ddf = ddf[ddf['date'] <= as_of_date]
        if len(ddf) < 25:
            return None
    frame = daily_feature_frame(ddf)
    amt20 = ddf.set_index('date')['amount'].rolling(20, min_periods=15).mean()
    mask = screener_rules_mask(frame, amount=amt20, screener_cfg=cfg)
    return ddf['date'].iloc[-1], bool(mask.iloc[-1]), frame.iloc[-1]


def run_screen(conn, for_date, cfg, use_model=True):
    """全市场筛选，返回候选 [(code, name, score)]。"""
    model, feats = (load_daily_model() if use_model else (None, None))
    if use_model and model is None:
        logger.warning("日线模型不存在，退化为纯规则排序（请先运行 train_model.py）")
    codes = read_all_market_codes()
    names = {}
    try:
        from Spiders.spiders.baostock_helper import read_stock_list_txt
        _, nm = read_stock_list_txt(os.path.join(common.PROJECT_ROOT, 'stock_list.txt'))
        names = nm or {}
    except Exception:
        pass

    t0 = time.time()
    passed = []
    for i, code in enumerate(codes, 1):
        r = screen_one(conn, code, cfg)
        if r is None:
            continue
        as_of, ok, factors = r
        if not ok:
            continue
        score = float('nan')
        if model is not None and feats:
            x = pd.DataFrame([[factors.get(c) for c in feats]], columns=feats)
            score = float(model.predict_proba(x)[0, 1])
        passed.append((code, names.get(code, ''), score))
        if i % 1000 == 0:
            logger.info(f"筛选进度 {i}/{len(codes)}，硬规则通过 {len(passed)}，用时 {int(time.time() - t0)}s")

    if model is not None:
        passed.sort(key=lambda t: (t[2] if t[2] == t[2] else -1), reverse=True)
    top = passed[:cfg['max_candidates']]
    logger.info(f"筛选完成：全市场 {len(codes)} -> 硬规则通过 {len(passed)} -> "
                f"候选 top{len(top)}（目标 {cfg['max_candidates']}），用时 {int(time.time() - t0)}s")
    return top, len(passed)


def next_trading_date(after=None):
    """after 之后的下一个交易日（baostock 交易日历；失败回退到工作日推算）。"""
    after = after or datetime.now()
    d0 = after + timedelta(days=1)
    try:
        import baostock as bs
        from Spiders.spiders.baostock_helper import login_baostock
        login_baostock()
        rs = bs.query_trade_dates(
            start_date=d0.strftime('%Y-%m-%d'),
            end_date=(d0 + timedelta(days=30)).strftime('%Y-%m-%d'))
        rows = []
        while rs.error_code == '0' and rs.next():
            rows.append(rs.get_row_data())
        for cal_date, is_trading in rows:
            if is_trading == '1':
                return cal_date
    except Exception as e:
        logger.warning(f"交易日历查询失败（{e}），回退工作日推算")
    d = d0
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.strftime('%Y-%m-%d')


def main():
    parser = argparse.ArgumentParser(description='一级筛选：全市场 -> 次日候选池')
    parser.add_argument('--no-fetch', action='store_true', help='跳过全市场日线增量抓取')
    parser.add_argument('--for-date', help='候选的决策日 YYYY-MM-DD（默认下一交易日）')
    parser.add_argument('--calibrate', action='store_true', help='只输出各规则通过率统计，不写库')
    parser.add_argument('--limit-fetch', type=int, help='限制抓取股票数（调试）')
    args = parser.parse_args()

    conn = connect_db()
    cfg = dict(CONFIG['screener'])
    end_date = datetime.now().strftime('%Y-%m-%d')

    if not args.no_fetch:
        ok, msg = baostock_login_ok()
        if not ok:
            logger.error(f"baostock 登录失败（{msg}），跳过全市场日线抓取。"
                         f"候选筛选仍会用库内已有数据继续（数据可能滞后）。")
        else:
            update_all_market_daily(conn, end_date, limit=args.limit_fetch)

    if args.calibrate:
        calibrate(conn, cfg)
        conn.close()
        return

    # 候选决策日：默认今天之后的下一个交易日（交易日历，避免节假日产垃圾池）
    if args.for_date:
        for_date = args.for_date
    else:
        for_date = next_trading_date()

    top, n_pass = run_screen(conn, for_date, cfg)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.executemany(
        "INSERT OR REPLACE INTO candidates (date, stock_code, stock_name, screener_score, created_at) "
        "VALUES (?,?,?,?,?)",
        [(for_date, c, n, s if s == s else None, now) for c, n, s in top])
    conn.commit()
    conn.close()
    logger.info(f"候选池已写入 candidates(date={for_date})：{len(top)} 只")
    for c, n, s in top[:10]:
        logger.info(f"  样例: {n}({c}) 模型分={s if s == s else 'NA'}")


def calibrate(conn, cfg):
    """调参视图：全市场最近一天的各规则通过数量（评估 ~100 候选目标差多远）。"""
    codes = read_all_market_codes()
    stats = {k: 0 for k in ('turnover', 'ret5_band', 'pos_high', 'vol_ratio', 'amount20', 'all')}
    total = 0
    for code in codes:
        ddf = pd.read_sql(
            "SELECT date, open, high, low, close, preclose, volume, amount, turn, pct_chg "
            "FROM daily_bars WHERE stock_code=? ORDER BY date", conn, params=(code,))
        if len(ddf) < 25:
            continue
        total += 1
        f = daily_feature_frame(ddf)
        last = f.iloc[-1]
        amt20 = ddf.set_index('date')['amount'].rolling(20, min_periods=15).mean().iloc[-1]
        c1 = bool(last['prev_turn'] >= cfg['min_turnover_prev'])
        c2 = bool(cfg['ret5_min'] <= last['ret_5'] <= cfg['ret5_max'])
        c3 = bool(last['pos_vs_20d_high'] >= cfg['pos_vs_20d_high_min'])
        c4 = bool(last['vol_ma_ratio'] >= cfg['vol_ma_ratio_min'])
        c5 = bool(amt20 >= cfg['min_avg_amount_20d'])
        stats['turnover'] += c1
        stats['ret5_band'] += c2
        stats['pos_high'] += c3
        stats['vol_ratio'] += c4
        stats['amount20'] += c5
        stats['all'] += (c1 and c2 and c3 and c4 and c5)
    logger.info(f"调参视图（有足够日线数据的 {total} 只）：")
    for k, v in stats.items():
        logger.info(f"  {k}: {v} 只通过")


if __name__ == '__main__':
    main()
