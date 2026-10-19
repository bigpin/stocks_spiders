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
    if ddf['date'].iloc[-1] < today:
        tm = mdf[mdf['date'] == today]
        if not tm.empty:
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


def write_report(pred_date, pool_date, rows, out_path):
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
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for rank, r in enumerate(rows, 1):
        conn.execute(
            "INSERT OR REPLACE INTO predictions "
            "(pred_date, pool_date, stock_code, stock_name, prob, rank, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (today, today, r['code'], r['name'], r['prob'], rank, now))
    conn.commit()
    conn.close()

    out = os.path.join(common.PROJECT_ROOT, f"gapup_predictions_{today.replace('-', '')}.txt")
    write_report(today, today, rows, out)
    logger.info(f"报告已写入 {out}；有效预测 {len(rows)}/{len(cand)} 只")


def main():
    parser = argparse.ArgumentParser(description='盘中高开精选（从候选池选 top N）')
    parser.add_argument('--date', help='预测运行日 YYYY-MM-DD（默认今天）')
    parser.add_argument('--backtest', metavar='DATE',
                        help='离线回测：用 gapup.db 历史数据构造该日特征（不访问东财）')
    args = parser.parse_args()
    run_predict(args)


if __name__ == '__main__':
    main()
