#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
盘中精选（14:30 运行）：从当日候选池（screener 产出，~100只）中精选 top N 高开概率股。

数据流：
  candidates(date=今天)（来自 screener.py 晚间运行）
  -> 每股 1 次东财请求（近40天5分钟线，含当日实时；日线特征来自 gapup.db，
     过期则回退东财日线）
  -> compute_features（与训练同一套代码，截至 14:30）
  -> gapup_model.pkl 打分 -> 排序 -> TOP5 精选报告 + predictions 表（全量入库供评估）

用法：
  python scripts/gapup/predict.py                      # 盘中/盘后跑当日
  python scripts/gapup/predict.py --date 2026-09-22 --pool-from-db
  python scripts/gapup/predict.py --backtest 2026-09-10   # 离线：库内历史数据回测接线
"""

import argparse
import os
import pickle
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common
from common import (
    CONFIG, MODELS_DIR, connect_db, normalize_code, scenario_anchor,
)
from build_dataset import compute_features, daily_feature_frame, screener_rules_mask
from screener import screen_one

logger = common.get_gapup_logger()

import pandas as pd


def load_model(name='gapup_model.pkl'):
    path = os.path.join(MODELS_DIR, name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"模型不存在: {path}，请先运行 train_model.py")
    with open(path, 'rb') as f:
        bundle = pickle.load(f)
    return bundle['model'], bundle['features'], bundle.get('calibrator')


def get_candidates(conn, date):
    rows = conn.execute(
        "SELECT stock_code, stock_name, screener_score FROM candidates WHERE date=? "
        "ORDER BY screener_score DESC NULLS LAST", (date,)).fetchall()
    return [(normalize_code(r[0]), r[1] or '') for r in rows]


def backfill_candidates_from_db(conn, date):
    """回测兜底：candidates 表没有该日数据时，用库内股票（训练宇宙）按筛选规则现算。

    注意：库内只有训练宇宙的 382 只股，覆盖不到全市场，仅用于接线回测。
    """
    logger.warning(f"candidates 表无 {date} 数据，用库内股票现算（非全市场，仅回测用）")
    cfg = dict(CONFIG['screener'])
    codes = [r[0] for r in conn.execute(
        "SELECT DISTINCT stock_code FROM daily_bars").fetchall()]
    out = []
    for code in codes:
        r = screen_one(conn, code, cfg, as_of_date=(
            (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')))
        if r and r[1]:
            out.append((normalize_code(code), ''))
    return out


def live_features_one(conn, code, today):
    """东财/新浪分钟窗口 + 库内日线构造单股当日特征，返回 (特征行, None) 或 (None, 原因)。"""
    from common import fetch_minute_window, fetch_recent_daily_eastmoney, fetch_daily_sina
    beg = (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=60)).strftime('%Y-%m-%d')
    mdf, source = fetch_minute_window(code, beg, today)
    if mdf is None or mdf.empty or mdf['date'].iloc[-1] != today:
        return None, '当日分钟线不可得'

    # 日线优先用库内（screener 晚间已增量更新）；缺失超过5天才回退（当日bar由分钟数据聚合，
    # 日线特征 shift(1) 只用昨日及更早，容忍1-2天滞后）；回退优先新浪（东财常与分钟线同时被限流）
    ddf = pd.read_sql(
        "SELECT date, open, high, low, close, preclose, volume, amount, turn, pct_chg "
        "FROM daily_bars WHERE stock_code=? ORDER BY date", conn, params=(code,))
    stale_before = (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d')
    if ddf.empty or ddf['date'].iloc[-1] < stale_before:
        try:
            em = fetch_daily_sina(code, datalen=60)
        except Exception:
            em = None
        if em is None or em.empty:
            try:
                em = fetch_recent_daily_eastmoney(code, days=60)
            except Exception:
                em = None
        if em is not None and not em.empty:
            ddf = em
    if ddf.empty or len(ddf) < 25:
        return None, '日线不足'

    # 盘中当日尚无日线行：用分钟数据聚合一根"今日bar"追加
    # （日线特征全部 shift(1) 用昨日及更早数据，不受影响；仅补齐 prev_close 链）
    def _board_limit(c):
        """按板块返回当日涨跌幅限制：60/00 主板 10%，30/68 创业板科创板 20%，北交所 30%。"""
        c = normalize_code(c)
        return 0.30 if c.startswith('bj') else (0.20 if c[:2] in ('30', '68') else 0.10)

    tm = mdf[mdf['date'] == today]
    if tm.empty:
        return None, '今日无分钟bar'
    # 跨源一致性守卫：库内昨收（baostock）与今日实时价（东财）在除权日/数据缺口时失配，
    # 会算出超涨跌停限制的假涨幅（踩过 +27%/-17% 的坑）——失配即用东财日线修复缺口
    first_open = float(tm['open'].iloc[0])
    prev_close = float(ddf['close'].iloc[-1])
    lim = _board_limit(code) + 0.01
    if prev_close > 0 and abs(first_open / prev_close - 1) > lim:
        try:
            em = fetch_recent_daily_eastmoney(code, days=60)
        except Exception:
            em = None
        if em is not None and len(em) >= 25:
            ddf = em
            logger.info(f"{code}: 库内昨收与实时价失配（开盘偏离超{lim*100:.0f}%），已用东财日线修复")
    if ddf['date'].iloc[-1] < today:
        nan = float('nan')
        today_bar = pd.DataFrame([{
            'date': today,
            'open': float(tm['open'].iloc[0]), 'close': float(tm['close'].iloc[-1]),
            'high': float(tm['high'].max()), 'low': float(tm['low'].min()),
            'preclose': nan,
            'volume': float(tm['volume'].sum()), 'amount': float(tm['amount'].sum()),
            'turn': nan, 'pct_chg': nan,
        }])
        ddf = pd.concat([ddf, today_bar], ignore_index=True)
    # 修复后仍超限 → 数据不可信，弃用该股
    t_open = float(tm['open'].iloc[0])
    ref_close = float(ddf[ddf['date'] < today]['close'].iloc[-1]) if (ddf['date'] < today).any() else 0
    if ref_close > 0 and abs(t_open / ref_close - 1) > lim:
        return None, f'数据异常（开盘偏离昨收超{lim * 100:.0f}%，疑似除权或长缺口）'

    feats = compute_features(code, mdf, ddf)
    if feats is None or feats.empty:
        return None, '特征构造为空'
    last = feats.iloc[-1]
    if str(last['date'])[:10] != today:
        return None, f'特征末行日期 {last["date"]} != {today}'
    return last, None


def db_features_one(conn, code, target_date):
    """离线回测：用 gapup.db 的 baostock 历史数据构造 target_date 当日特征。"""
    mdf = pd.read_sql(
        "SELECT date, bar_start, open, high, low, close, volume, amount "
        "FROM minute_bars WHERE stock_code=? ORDER BY date, bar_start",
        conn, params=(code,))
    ddf = pd.read_sql(
        "SELECT date, open, high, low, close, preclose, volume, amount, turn, pct_chg "
        "FROM daily_bars WHERE stock_code=? ORDER BY date",
        conn, params=(code,))
    if mdf.empty or ddf.empty:
        return None, '库内无数据'
    feats = compute_features(code, mdf, ddf)
    if feats is None or feats.empty:
        return None, '特征构造为空'
    row = feats[feats['date'] == target_date]
    if row.empty:
        return None, f'{target_date} 无特征（停牌或数据缺失）'
    return row.iloc[0], None


def write_report(pred_date, pool_date, rows, out_path, scan_rows=None):
    """rows 按 prob 降序；TOP N 精选突出显示，其余为完整排名表。"""
    thr = CONFIG['gap_threshold_pct']
    top_n = CONFIG['top_n_output']
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"次日高开精选报告 - {pred_date}（预测 T+1 开盘涨幅 ≥ {thr}%）\n")
        f.write(f"候选池: {len(rows)} 只（来源 {pool_date} 晚间一级筛选）\n")
        f.write("=" * 70 + "\n")
        if not rows:
            f.write("今日无有效预测（候选为空或数据缺失）\n")
            return
        f.write(f"\n◆ 精选 TOP{min(top_n, len(rows))}（按高开概率）\n")
        for i, r in enumerate(rows[:top_n], 1):
            def _pct(v):
                return f"{v * 100:+.2f}%" if v == v else "—"
            anchor = r.get('anchor')
            anchor_txt = f"≈{anchor * 100:.0f}%" if anchor is not None else "—"
            f.write(
                f"\n[{i}] {r['name']}({r['code']})  高开概率 {r['prob'] * 100:.1f}%"
                f"（情景锚点 {anchor_txt}）\n"
                f"    情景: {r.get('scenario', '—')}\n"
                f"    盘中: 涨幅 {_pct(r.get('ret_so_far'))} | "
                f"量比 {r.get('vol_ratio_same', float('nan')):.2f} | "
                f"尾盘拉升 {_pct(r.get('late_surge'))}\n"
                f"    近5日 {_pct(r.get('ret_5'))} | "
                f"连涨 {int(r.get('up_days', 0) or 0)} 天 | "
                f"距20日高点 {_pct(r.get('pos_vs_20d_high'))}\n")
        f.write("\n" + "=" * 70 + f"\n完整排名（共 {len(rows)} 只）:\n")
        for i, r in enumerate(rows, 1):
            star = ' *' if i <= top_n else ''
            f.write(f"{i:3d}. {r['name']}({r['code']}) 概率 {r['prob'] * 100:.1f}%{star}\n")
        if scan_rows is not None:
            f.write("\n" + "=" * 70 + "\n◆ 策略信号（14:30 可买入，独立于候选池）\n")
            names = {'limitdown_recover': '跌停复苏 [历史胜率72%/中位+1.4%]',
                     'strong_continue': '连续强势 [历史胜率65%/中位+1.1%]'}
            if not scan_rows:
                f.write("今日无（跌停复苏/连续强势均未触发）\n")
            for st, code, name, pchg, tchg, dpos in scan_rows:
                f.write(
                    f"\n- {name}({code})  {names.get(st, st)}\n"
                    f"  昨日 {pchg * 100:+.1f}% → 今日 {tchg * 100:+.2f}%（日内位置 {dpos:.0%}）\n"
                    f"  打法：14:30 买入，次日高开≥1% 竞价卖，否则 10:00 前冲高卖，绝不拿收盘\n")


def scan_strategies(today):
    """全市场扫描两个验证过的可买入策略（东财实时快照 + 库内昨日日线）。

    - limitdown_recover 跌停复苏：昨跌≤-9.5% + 今14:30涨幅≥2% + 日内位置>0.75
      （历史：复合退出中位 +1.43%、胜率72%、7/7年全正）
    - strong_continue 连续强势：昨涨>3% + 今涨5~9.5%未封板 + 日内位置>0.85
      （历史：中位 +1.13%、胜率65%、7/7年全正）
    返回 [(strategy, code, name, prev_chg, today_chg, day_pos)]。
    """
    from common import fetch_market_snapshot_eastmoney
    snap = fetch_market_snapshot_eastmoney()
    if snap is None or snap.empty:
        logger.warning("全市场快照获取失败，跳过策略扫描")
        return []
    conn = connect_db()
    prev = pd.read_sql("""
        SELECT stock_code, close / p_close - 1 AS prev_chg
        FROM (SELECT stock_code, date, close,
                     LAG(close) OVER (PARTITION BY stock_code ORDER BY date) AS p_close
              FROM daily_bars)
        WHERE date = (SELECT MAX(date) FROM daily_bars WHERE date < ?) AND p_close > 0
    """, conn, params=(today,))
    conn.close()
    snap = snap.merge(prev, left_on='code', right_on='stock_code', how='left')
    snap = snap.dropna(subset=['price', 'preclose', 'prev_chg'])
    snap = snap[(snap['price'] > 0) & (snap['high'] > snap['low'])]
    snap = snap[~snap['name'].str.contains('ST', na=False)]
    snap['today_chg'] = snap['price'] / snap['preclose'] - 1
    snap['day_pos'] = (snap['price'] - snap['low']) / (snap['high'] - snap['low'])
    # 剔除昨日缺数据（prev_chg 为 NaN）与北交所（流动性差、规则不同）
    snap = snap.dropna(subset=['today_chg', 'day_pos'])
    snap = snap[~snap['code'].str.startswith('bj')]

    out = []
    ld = snap[(snap['prev_chg'] <= -0.095) & (snap['today_chg'] >= 0.02) & (snap['day_pos'] > 0.75)]
    for r in ld.itertuples():
        out.append(('limitdown_recover', r.code, r.name, r.prev_chg, r.today_chg, r.day_pos))
    sc = snap[(snap['prev_chg'] > 0.03) & snap['today_chg'].between(0.05, 0.095)
              & (snap['day_pos'] > 0.85) & (snap['amount'] > 5e7)]
    for r in sc.itertuples():
        out.append(('strong_continue', r.code, r.name, r.prev_chg, r.today_chg, r.day_pos))
    return out


def run_predict(args):
    conn = connect_db()
    today = args.backtest or args.date or datetime.now().strftime('%Y-%m-%d')
    if args.backtest:
        cand = get_candidates(conn, args.backtest)
        if not cand:
            cand = backfill_candidates_from_db(conn, args.backtest)
    else:
        cand = get_candidates(conn, today)
        if not cand:
            conn.close()
            logger.error(f"candidates 表无 {today} 的候选池，请先运行 screener.py（晚间任务）")
            return
    logger.info(f"预测日 {today}，候选池 {len(cand)} 只")

    model, feats_cols, calibrator = load_model()
    logger.info(f"模型加载完成（{len(feats_cols)} 特征"
                f"{'' if calibrator is None else '，含概率校准'}）")

    rows = []
    for i, (code, name) in enumerate(cand, 1):
        try:
            if args.backtest:
                last, err = db_features_one(conn, code, args.backtest)
            else:
                last, err = live_features_one(conn, code, today)
        except Exception as e:
            last, err = None, str(e)
        if last is None:
            logger.warning(f"[{i}/{len(cand)}] {code} {name}: 跳过 - {err}")
            continue
        x = pd.DataFrame([[last.get(c) for c in feats_cols]], columns=feats_cols)
        prob = float(model.predict_proba(x)[0, 1])
        if calibrator is not None:
            prob = float(calibrator.predict([prob])[0])
        d = {'code': code, 'name': name, 'prob': prob}
        sc_name, sc_rate = scenario_anchor(last)
        d['scenario'], d['anchor'] = sc_name, sc_rate
        for c in ('ret_so_far', 'vol_ratio_same', 'late_surge', 'ret_5', 'up_days',
                  'pos_vs_20d_high'):
            d[c] = last.get(c)
        rows.append(d)
        logger.info(f"[{i}/{len(cand)}] {code} {name}: 高开概率 {prob * 100:.1f}%")

    rows.sort(key=lambda r: r['prob'], reverse=True)

    # 策略信号扫描（跌停复苏/连续强势，可买入族，独立于模型候选池）
    scan = []
    if not args.backtest:
        try:
            scan = scan_strategies(today)
            for st, code, name, pchg, tchg, dpos in scan:
                logger.info(f"策略信号[{st}] {name}({code}): 昨{pchg*100:+.1f}% "
                            f"今{tchg*100:+.2f}% 位 {dpos:.0%}")
        except Exception as e:
            logger.warning(f"策略扫描失败: {e}")

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for rank, r in enumerate(rows, 1):
        conn.execute(
            "INSERT OR REPLACE INTO predictions "
            "(pred_date, pool_date, stock_code, stock_name, prob, rank, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (today, today, r['code'], r['name'], r['prob'], rank, now))
    for st, code, name, pchg, tchg, dpos in scan:
        conn.execute(
            "INSERT OR REPLACE INTO strategy_signals "
            "(signal_date, strategy, stock_code, stock_name, prev_chg, today_chg, day_pos, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (today, st, code, name, pchg, tchg, dpos, now))
    conn.commit()
    conn.close()

    out = os.path.join(common.PROJECT_ROOT, f"gapup_predictions_{today.replace('-', '')}.txt")
    write_report(today, today, rows, out, scan_rows=scan)
    logger.info(f"报告已写入 {out}；有效预测 {len(rows)}/{len(cand)} 只，策略信号 {len(scan)} 个")


def main():
    parser = argparse.ArgumentParser(description='盘中高开精选（从候选池选 top N）')
    parser.add_argument('--date', help='预测运行日 YYYY-MM-DD（默认今天）')
    parser.add_argument('--backtest', metavar='DATE',
                        help='离线回测：用 gapup.db 历史数据构造该日特征（不访问东财）')
    args = parser.parse_args()
    run_predict(args)


if __name__ == '__main__':
    main()
