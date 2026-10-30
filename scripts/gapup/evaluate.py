#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
验证闭环：对已有 predictions 回填次日实际开盘，统计命中率。

数据源优先级：gapup.db daily_bars（screener 晚间已更新） -> 东方财富日线（盘中/未更新时）。
报告：整体命中率、TOP5 命中率、平均高开幅度、近20个预测日滚动命中率。

用法：
  python scripts/gapup/evaluate.py              # 回填所有待验证预测并输出报告
  python scripts/gapup/evaluate.py --date 2026-09-22
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common
from common import CONFIG, connect_db, normalize_code

logger = common.get_gapup_logger()

import pandas as pd


def next_trading_open_from_db(conn, code, pred_date):
    """从库内日线取 pred_date 的收盘与下一交易日开盘。返回 (t_close, t1_open, t1_date) 或 None。"""
    rows = conn.execute(
        "SELECT date, open, close FROM daily_bars WHERE stock_code=? AND date > ? "
        "ORDER BY date LIMIT 1", (code, pred_date)).fetchall()
    t = conn.execute(
        "SELECT close FROM daily_bars WHERE stock_code=? AND date=?",
        (code, pred_date)).fetchone()
    if not rows or not t or t[0] is None:
        return None
    t1 = rows[0]
    if t1[1] is None:
        return None
    return t[0], t1[1], t1[0]


def next_trading_open_fallback(code, pred_date):
    """外部源兜底：新浪优先（快、稳定），东财次之（自带退避重试，限流时慢）。"""
    from common import fetch_daily_sina, fetch_recent_daily_eastmoney
    for fetcher, kw in ((fetch_daily_sina, {'datalen': 10}),
                        (fetch_recent_daily_eastmoney, {'days': 10})):
        try:
            df = fetcher(code, **kw)
        except Exception as e:
            logger.info(f"{pred_date} {code}: {fetcher.__name__} 兜底失败: {e}")
            continue
        if df is None or df.empty:
            continue
        after = df[df['date'] > pred_date]
        before = df[df['date'] <= pred_date]
        if after.empty or before.empty:
            continue
        t_close = float(before['close'].iloc[-1])
        t1 = after.iloc[0]
        if not t_close or t1['open'] is None:
            continue
        return t_close, float(t1['open']), t1['date']
    return None


def t1_date_of(pred_date):
    """pred_date 的下一个工作日（节假日顺延由数据侧兜底，这里只挡'明天还没来'）。"""
    d = datetime.strptime(pred_date, '%Y-%m-%d') + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.strftime('%Y-%m-%d')


def fill_outcomes(conn, only_date=None):
    """回填待验证预测的 outcomes。T+1 尚未走完的预测静默跳过（次日自然会验证）。"""
    q = ("SELECT p.pred_date, p.stock_code, p.rank FROM predictions p "
         "LEFT JOIN outcomes o ON o.pred_date = p.pred_date AND o.stock_code = p.stock_code "
         "WHERE o.pred_date IS NULL")
    params = ()
    if only_date:
        q += " AND p.pred_date = ?"
        params = (only_date,)
    pending = conn.execute(q, params).fetchall()
    if not pending:
        logger.info("无待验证预测")
        return
    today = datetime.now().strftime('%Y-%m-%d')
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    not_due = filled = 0
    for pred_date, code, rank in pending:
        # T+1 还没走完（预测日是今天或更晚）：属正常待验证，不报错、不请求外部源
        if t1_date_of(pred_date) >= today:
            not_due += 1
            continue
        code_n = normalize_code(code)
        r = next_trading_open_from_db(conn, code_n, pred_date)
        used = 'db'
        if r is None:
            r = next_trading_open_fallback(code_n, pred_date)
            used = 'fallback'
        if r is None:
            # T+1 已过但仍无数据（停牌/节假日顺延）：留待下次运行
            logger.warning(f"{pred_date} {code_n}: T+1 数据仍不可得，留待下次验证")
            continue
        t_close, t1_open, t1_date = r
        gap = (t1_open / t_close - 1) * 100 if t_close else None
        hit = 1 if gap is not None and gap >= CONFIG['gap_threshold_pct'] else 0
        conn.execute(
            "INSERT OR REPLACE INTO outcomes "
            "(pred_date, stock_code, t_close, t1_open, t1_open_gap, is_high_open, checked_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (pred_date, code_n, t_close, t1_open, gap, hit, now_str))
        filled += 1
        if used == 'fallback':
            logger.info(f"{pred_date} {code_n}: 外部源兜底 gap={gap:.2f}% hit={hit}")
    conn.commit()
    if not_due:
        logger.info(f"{not_due} 条预测的次日尚未走完，正常待验证（不会请求外部数据源）")
    logger.info(f"回填完成 {filled}/{len(pending)}")

    # 策略信号（跌停复苏/连续强势）的次日开盘验证
    today2 = datetime.now().strftime('%Y-%m-%d')
    sigs = conn.execute(
        "SELECT signal_date, stock_code FROM strategy_signals WHERE t1_open_gap IS NULL").fetchall()
    s_filled = s_notdue = 0
    now_str2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for sig_date, code in sigs:
        if t1_date_of(sig_date) >= today2:
            s_notdue += 1
            continue
        code_n = normalize_code(code)
        r = next_trading_open_from_db(conn, code_n, sig_date)
        if r is None:
            r = next_trading_open_fallback(code_n, sig_date)
        if r is None:
            continue
        t_close, t1_open, t1_date = r
        gap = (t1_open / t_close - 1) * 100 if t_close else None
        # 30分钟实测：拉 T+1 当天分钟线，算开盘->10:00 收益（验证卖出纪律的兑现度）
        t1_30m = None
        try:
            from common import fetch_minute_window_eastmoney
            m = fetch_minute_window_eastmoney(code_n, t1_date, t1_date)
            if m is not None and not m.empty:
                m30 = m[m['bar_start'] <= '10:00'].sort_values('bar_start')
                if len(m30) >= 2:
                    t1_30m = (float(m30['close'].iloc[-1]) / float(m30['open'].iloc[0]) - 1) * 100
        except Exception as e:
            logger.info(f"策略信号 {sig_date} {code_n}: 30分钟数据获取失败 {e}")
        conn.execute(
            "UPDATE strategy_signals SET t1_open_gap=?, t1_30m_ret=?, checked_at=? "
            "WHERE signal_date=? AND stock_code=?",
            (gap, t1_30m, now_str2, sig_date, code_n))
        s_filled += 1
    conn.commit()
    if s_filled or s_notdue:
        logger.info(f"策略信号验证：回填 {s_filled}，待验证 {s_notdue}")


def report(conn):
    df = pd.read_sql(
        "SELECT o.pred_date, o.stock_code, o.t1_open_gap, o.is_high_open, p.rank "
        "FROM outcomes o JOIN predictions p "
        "ON p.pred_date = o.pred_date AND p.stock_code = o.stock_code", conn)
    if df.empty:
        logger.info("尚无已验证结果")
        return
    df = df.sort_values(['pred_date', 'rank'])
    n_days = df['pred_date'].nunique()
    logger.info("=" * 60)
    logger.info(f"累计验证：{len(df)} 条预测 / {n_days} 个预测日（{df['pred_date'].min()} ~ {df['pred_date'].max()}）")
    logger.info(f"整体高开命中率: {df['is_high_open'].mean() * 100:.1f}%"
                f"（基础阈值 ≥{CONFIG['gap_threshold_pct']}%）")
    logger.info(f"平均次日开盘涨幅: {df['t1_open_gap'].mean():+.2f}%")
    top5 = df[df['rank'] <= 5]
    if not top5.empty:
        logger.info(f"TOP5 命中率: {top5['is_high_open'].mean() * 100:.1f}%"
                    f" | TOP5 平均开盘涨幅: {top5['t1_open_gap'].mean():+.2f}%")
    recent_dates = sorted(df['pred_date'].unique())[-20:]
    recent = df[df['pred_date'].isin(recent_dates)]
    logger.info(f"近20个预测日: 整体命中 {recent['is_high_open'].mean() * 100:.1f}%"
                f" | TOP5 命中 {(recent[recent['rank'] <= 5]['is_high_open'].mean() * 100):.1f}%")
    # 最近5日逐日
    for d in sorted(df['pred_date'].unique())[-5:]:
        day = df[df['pred_date'] == d]
        t5 = day[day['rank'] <= 5]
        logger.info(f"  {d}: {day['is_high_open'].mean() * 100:.0f}%命中"
                    f"（{int(day['is_high_open'].sum())}/{len(day)}），"
                    f"TOP5 {t5['is_high_open'].mean() * 100:.0f}%"
                    f"（{int(t5['is_high_open'].sum())}/{len(t5)}）")


def main():
    parser = argparse.ArgumentParser(description='验证闭环：回填 outcomes 并输出命中率报告')
    parser.add_argument('--date', help='只验证某个预测日 YYYY-MM-DD')
    parser.add_argument('--report-only', action='store_true')
    args = parser.parse_args()

    conn = connect_db()
    if not args.report_only:
        fill_outcomes(conn, only_date=args.date)
    report(conn)
    conn.close()


if __name__ == '__main__':
    main()
