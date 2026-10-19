#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
构造 gapup 训练样本：对每只股票的每个交易日 T，
- 用 T 日截至 14:30 的分钟 bar 算盘中特征
- 用 T-1 及更早的日线算历史特征
- 标签 = T+1 开盘涨幅（复权口径）是否 >= 阈值

输出：data/gapup/training_samples.parquet（含 in_pool 标记，训练/评估自行切片）

用法：
  python scripts/gapup/build_dataset.py                 # 全部库内股票
  python scripts/gapup/build_dataset.py --stocks sh600000 ...
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common
from common import CONFIG, FEATURE_COLUMNS, connect_db, bars_closed_by

logger = common.get_gapup_logger()

import numpy as np
import pandas as pd

# 日线特征子集（晚间一级筛选的"日线模型"与盘中完整模型共用同一口径）
DAILY_FEATURE_COLUMNS = [
    'prev_ret_1', 'prev_turn', 'prev_amplitude',
    'ret_5', 'ret_10', 'ret_20', 'up_days',
    'vol_ma_ratio', 'pos_vs_20d_high', 'is_20d_close_high',
]


def daily_feature_frame(daily_df):
    """日线因子，按日期 S 的原始口径（不做 shift）。

    - 训练行 T 的日线特征 = frame.shift(1) 在 T 的取值（即 T-1 的因子）
    - 晚间筛选（D 日收盘后选 D+1 候选）= frame 最后一行（D 的因子），
      恰等于 D+1 行 shift(1) 的取值，两种用法天然对齐。
    """
    dd = daily_df.sort_values('date').set_index('date')
    f = pd.DataFrame(index=dd.index)
    f['prev_ret_1'] = dd['close'].pct_change()
    f['prev_turn'] = dd['turn']
    prev_close = dd['close'].shift(1)
    f['prev_amplitude'] = (dd['high'] - dd['low']) / prev_close.replace(0, np.nan)
    for k in (5, 10, 20):
        f[f'ret_{k}'] = dd['close'] / dd['close'].shift(k) - 1
    up = (dd['close'].pct_change() > 0).astype(int)
    grp = (up != up.shift(1)).cumsum()
    f['up_days'] = up.groupby(grp).cumsum()
    f['vol_ma_ratio'] = (dd['volume'].rolling(5).mean()
                         / dd['volume'].rolling(20).mean().replace(0, np.nan))
    high20 = dd['close'].rolling(20).max()
    f['pos_vs_20d_high'] = dd['close'] / high20 - 1
    f['is_20d_close_high'] = (dd['close'] >= high20).astype(float)
    return f


def screener_rules_mask(frame, amount=None, screener_cfg=None):
    """一级筛选硬规则（frame 为 daily_feature_frame 原始口径）。

    amount: 与 frame 同索引的近20日均成交额 Series（可为 None 则跳过该规则）。
    返回 bool Series；screener 晚间取 .iloc[-1]，训练对齐逐日取值。
    """
    from common import CONFIG
    cfg = screener_cfg or CONFIG['screener']
    m = (
        (frame['prev_ret_1'].notna() & frame['ret_20'].notna())
        & (frame['prev_turn'] >= cfg['min_turnover_prev'])
        & (frame['ret_5'] >= cfg['ret5_min']) & (frame['ret_5'] <= cfg['ret5_max'])
        & (frame['pos_vs_20d_high'] >= cfg['pos_vs_20d_high_min'])
        & (frame['vol_ma_ratio'] >= cfg['vol_ma_ratio_min'])
    )
    if amount is not None:
        m = m & (amount >= cfg['min_avg_amount_20d'])
    return m.fillna(False)


def compute_features(code, minute_df, daily_df, cutoff='14:30', freq_min=5):
    """特征工程（不含标签过滤）：返回按日期索引的特征 DataFrame。

    训练（build_dataset）与盘中预测（predict）共用，保证口径一致。
    输出列 = FEATURE_COLUMNS + ['t_close', 'next_open', 'next_date']；
    最后一行的 next_open/next_date 通常为 NaN（当日未知），标签由调用方处理。
    """
    if minute_df is None or minute_df.empty or daily_df is None or daily_df.empty:
        return None

    # ---- 日线基础：按日期排序，构造 T 日参考 ----
    d = daily_df.sort_values('date').reset_index(drop=True)
    d['prev_close'] = d['close'].shift(1)
    d['next_open'] = d['open'].shift(-1)
    d['next_date'] = d['date'].shift(-1)
    if len(d) < 25:
        return None

    # ---- 分钟线：截至 cutoff 已收盘的 bar ----
    m = bars_closed_by(minute_df, cutoff=cutoff, freq_min=freq_min).copy()
    m = m.sort_values(['date', 'bar_start'])
    # 日内运行最高收盘（用于 n_new_highs）
    m['run_max'] = m.groupby('date')['close'].cummax()
    m['prev_run_max'] = m.groupby('date')['run_max'].shift(1)
    m['is_new_high'] = m['close'] > m['prev_run_max']

    g = m.groupby('date')
    feat = pd.DataFrame(index=g.size().index)
    feat['cutoff_close'] = g['close'].last()          # 截至14:30现价
    feat['open_price'] = g['open'].first()            # 当日开盘价
    feat['day_high'] = g['high'].max()
    feat['day_low'] = g['low'].min()
    feat['vwap'] = g['amount'].sum() / g['volume'].sum().replace(0, np.nan)
    feat['n_new_highs'] = g['is_new_high'].sum()
    feat['vol_so_far'] = g['volume'].sum()
    # 分段bar
    morn = m[m['bar_start'] <= '11:25'].groupby('date')['close'].last()
    aft_open = m[m['bar_start'] >= '13:00'].groupby('date')['open'].first()
    last30_open = m[m['bar_start'] >= '14:00'].groupby('date')['open'].first()
    feat['mid_close'] = morn
    feat['aft_open'] = aft_open
    feat['last30_open'] = last30_open
    # 近20日"同时段"量均值（不含当日）
    feat['vol_so_far_ma20'] = feat['vol_so_far'].rolling(20, min_periods=10).mean().shift(1)

    # ---- 合并日线（T 日行：prev_close=昨日收盘，next_open=明日开盘）----
    feat = feat.join(d.set_index('date')[['prev_close', 'next_open', 'next_date',
                                          'turn', 'pct_chg', 'high', 'low', 'close',
                                          'volume', 'preclose']])
    # next_open 允许为 NaN（当日行：明日开盘未知，盘中预测要用这一行；
    # 训练侧由 build_stock_samples 再 dropna(t1_open) 剔除）
    feat = feat.dropna(subset=['prev_close', 'cutoff_close'])

    pc = feat['prev_close']
    out = pd.DataFrame(index=feat.index)
    out['ret_so_far'] = feat['cutoff_close'] / pc - 1
    out['open_gap'] = feat['open_price'] / pc - 1
    out['morning_ret'] = feat['mid_close'] / pc - 1
    out['afternoon_ret'] = feat['cutoff_close'] / feat['mid_close'] - 1
    out['vwap_pos'] = feat['cutoff_close'] / feat['vwap'] - 1
    rng = (feat['day_high'] - feat['day_low']).replace(0, np.nan)
    out['day_range_pos'] = (feat['cutoff_close'] - feat['day_low']) / rng
    out['intraday_range'] = rng / pc
    out['pullback_from_high'] = feat['cutoff_close'] / feat['day_high'] - 1
    out['vol_ratio_same'] = feat['vol_so_far'] / feat['vol_so_far_ma20']
    out['late_surge'] = feat['cutoff_close'] / feat['aft_open'] - 1
    out['last30m_momentum'] = feat['cutoff_close'] / feat['last30_open'] - 1
    out['n_new_highs'] = feat['n_new_highs'].astype(float)

    # ---- 历史日线特征（T-1 收盘口径 = daily_feature_frame 原始口径 shift(1)）----
    daily_feats = daily_feature_frame(d).shift(1)
    for col in DAILY_FEATURE_COLUMNS:
        out[col] = daily_feats[col]

    # ---- 次日开盘后30分钟（09:30-10:00）实际走势标签：高开后留不留的依据 ----
    m30 = m[m['bar_start'] <= '10:00']
    if not m30.empty:
        g30 = m30.sort_values('bar_start').groupby('date')
        d30 = pd.DataFrame({
            'open30': g30['open'].first(),      # 开盘价
            'close30': g30['close'].last(),     # ≈10:00价
            'high30': g30['high'].max(),
        })
        nd = feat['next_date']
        out['t1_30_open'] = nd.map(d30['open30'])
        out['t1_30_close'] = nd.map(d30['close30'])
        out['t1_30_high'] = nd.map(d30['high30'])
        out['t1_30m_ret'] = (out['t1_30_close'] / out['t1_30_open'] - 1) * 100   # 开盘买入->10:00卖
        out['t1_30m_high_ret'] = (out['t1_30_high'] / out['t1_30_open'] - 1) * 100  # 30分钟内最高冲高
        out['t1_to_30_ret'] = (out['t1_30_close'] / feat['close'] - 1) * 100  # 14:30买入->次日10:00卖

    # ---- 标签（仅历史日有值；当日行 next_open 为 NaN）----
    out['next_open_gap'] = (feat['next_open'] / feat['close'] - 1) * 100  # 决策日收盘口径
    out['is_high_open'] = (out['next_open_gap'] >= CONFIG['gap_threshold_pct']).astype(int)
    out['t_close'] = feat['close']
    out['t1_open'] = feat['next_open']
    # 次日全天收益（14:30 买入持有到次日收盘，实际交易口径）
    d_sorted = d.set_index('date')
    out['next_close'] = d_sorted['close'].shift(-1)
    out['next_ret'] = (out['next_close'] / feat['close'] - 1) * 100

    out = out.dropna(subset=['ret_so_far', 'prev_ret_1', 'ret_20', 'vol_ratio_same'])
    out.insert(0, 'stock_code', code)
    out.insert(1, 'date', out.index)
    return out.reset_index(drop=True)


def build_stock_samples(code, minute_df, daily_df, cutoff='14:30', freq_min=5):
    """训练样本：compute_features + 只保留能定标签的历史日。"""
    out = compute_features(code, minute_df, daily_df, cutoff=cutoff, freq_min=freq_min)
    if out is None or out.empty:
        return None
    return out.dropna(subset=['t1_open'])


def main():
    parser = argparse.ArgumentParser(description='构造 gapup 训练样本')
    parser.add_argument('--stocks', nargs='+')
    parser.add_argument('--limit', type=int, help='最多处理N只（调试用）')
    parser.add_argument('--out', default=os.path.join(common.DATA_DIR, 'training_samples.parquet'))
    args = parser.parse_args()

    conn = connect_db()
    codes = args.stocks
    if not codes:
        codes = [r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM minute_bars ORDER BY stock_code").fetchall()]
    if args.limit:
        codes = codes[:args.limit]
    pool_days = {(r[0], r[1]) for r in conn.execute(
        "SELECT stock_code, date FROM pool_history").fetchall()}
    logger.info(f"待构造 {len(codes)} 只股票的样本")

    t0 = time.time()
    frames = []
    for i, code in enumerate(codes, 1):
        mdf = pd.read_sql(
            "SELECT date, bar_start, open, high, low, close, volume, amount "
            "FROM minute_bars WHERE stock_code=? ORDER BY date, bar_start",
            conn, params=(code,))
        ddf = pd.read_sql(
            "SELECT date, open, high, low, close, preclose, volume, amount, turn, pct_chg "
            "FROM daily_bars WHERE stock_code=? ORDER BY date",
            conn, params=(code,))
        if mdf.empty or ddf.empty:
            continue
        samples = build_stock_samples(code, mdf, ddf)
        if samples is not None and not samples.empty:
            # 一级筛选对齐标记：该股票日是否满足晚间筛选硬规则（训练分布=部署分布）
            frame = daily_feature_frame(ddf)
            amt20 = ddf.set_index('date')['amount'].rolling(20, min_periods=15).mean()
            mask = screener_rules_mask(frame, amount=amt20)
            samples['passes_screener'] = samples['date'].map(
                lambda dt: bool(mask.get(dt, False)))
            frames.append(samples)
        if i % 50 == 0:
            logger.info(f"进度 {i}/{len(codes)}，累计样本 {sum(len(f) for f in frames)}，"
                        f"已用时 {int(time.time() - t0)}s")

    conn.close()
    if not frames:
        logger.warning("未生成任何样本")
        return
    df = pd.concat(frames, ignore_index=True)
    df['in_pool'] = df.apply(lambda r: (r['stock_code'], r['date']) in pool_days, axis=1)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_parquet(args.out, index=False)

    pos_rate = df['is_high_open'].mean() * 100
    pool_n = int(df['in_pool'].sum())
    pool_pos = df[df['in_pool']]['is_high_open'].mean() * 100 if pool_n else 0
    scr_n = int(df['passes_screener'].sum())
    scr_pos = df[df['passes_screener']]['is_high_open'].mean() * 100 if scr_n else 0
    logger.info(
        f"样本构造完成：{len(df)} 行（{df['stock_code'].nunique()} 只，"
        f"{df['date'].min()} ~ {df['date'].max()}），整体高开率 {pos_rate:.1f}%；"
        f"池子触发日样本 {pool_n} 行（高开率 {pool_pos:.1f}%）；"
        f"过一级筛选样本 {scr_n} 行（高开率 {scr_pos:.1f}%）；"
        f"输出 {args.out}，用时 {int(time.time() - t0)}s")


if __name__ == '__main__':
    main()
