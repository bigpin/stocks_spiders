"""
独立的信号计算 worker 模块 —— 纯 CPU 计算，可被 ProcessPoolExecutor pickle。
从 stock_kline.py 抽取"技术指标计算 + 信号分析 + 过滤 + 热度评分"逻辑，
主进程只需处理 I/O（写文件、SQLite）。
"""

import pandas as pd
import bisect
from .technical_indicators import TechnicalIndicators


# ---------------------------------------------------------------------------
# 过滤辅助（无 self 依赖，全部依赖入参 signal_filters）
# ---------------------------------------------------------------------------

def _is_st(df, pos, signal_filters):
    if not signal_filters.get('exclude_st', True):
        return False
    if 'is_st' not in df.columns:
        return False
    value = df.iloc[pos].get('is_st')
    return value in (1, '1')


def _passes_trade_status(df, pos, signal_filters):
    if not signal_filters.get('require_tradestatus', True):
        return True
    if 'trade_status' not in df.columns:
        return True
    value = df.iloc[pos].get('trade_status')
    if value in (0, '0'):
        return False
    return True


def _passes_liquidity_filters(df, pos, signal_type, signal_filters):
    liquidity_cfg = signal_filters['liquidity']
    avg_days = liquidity_cfg.get('avg_days', 20)
    start = max(0, pos - avg_days + 1)
    window = df.iloc[start:pos + 1]
    if window.empty:
        return False

    if 'amount' in window.columns:
        avg_amount = window['amount'].mean()
        if avg_amount is not None and avg_amount < liquidity_cfg.get('min_avg_amount', 0):
            return False

    if 'turnover' in window.columns:
        avg_turnover = window['turnover'].mean()
        if avg_turnover is not None and avg_turnover < liquidity_cfg.get('min_avg_turnover_rate', 0):
            return False

    if 'volume' in window.columns:
        avg_volume = window['volume'].mean()
        current_volume = df.iloc[pos]['volume']
        if avg_volume and current_volume is not None and avg_volume > 0:
            volume_ratio = current_volume / avg_volume
            trend_signals = {
                'macd_golden_cross', 'macd_zero_cross', 'ma_golden_cross',
                'dmi_golden_cross', 'dmi_adx_strong', 'boll_width_expand',
            }
            required_ratio = (
                liquidity_cfg.get('trend_volume_ratio')
                if signal_type in trend_signals
                else liquidity_cfg.get('min_volume_ratio')
            )
            if required_ratio and volume_ratio < required_ratio:
                return False
    return True


def _get_valuation_from_df(df):
    """从 K 线 DataFrame 最后一行取 PE/PB。"""
    pe, pb = None, None
    if df is not None and not df.empty:
        last = df.iloc[-1]
        pe_val = last.get('peTTM')
        pb_val = last.get('pbMRQ')
        if pe_val is not None and pd.notna(pe_val):
            try:
                pe = float(pe_val)
            except (TypeError, ValueError):
                pass
        if pb_val is not None and pd.notna(pb_val):
            try:
                pb = float(pb_val)
            except (TypeError, ValueError):
                pass
    return pe, pb


def _check_valuation_filters(df, signal_filters):
    valuation_cfg = signal_filters.get('valuation', {})
    if not valuation_cfg.get('enable', True):
        return True, 'disabled'
    pe, pb = _get_valuation_from_df(df)
    if pe is None and pb is None:
        return True, 'missing'
    pe_min = valuation_cfg.get('pe_min')
    pe_max = valuation_cfg.get('pe_max')
    pb_min = valuation_cfg.get('pb_min')
    pb_max = valuation_cfg.get('pb_max')
    if pe is not None and pe_min is not None and pe < pe_min:
        return False, 'blocked'
    if pe is not None and pe_max is not None and pe > pe_max:
        return False, 'blocked'
    if pb is not None and pb_min is not None and pb < pb_min:
        return False, 'blocked'
    if pb is not None and pb_max is not None and pb > pb_max:
        return False, 'blocked'
    return True, 'passed'


# ---------------------------------------------------------------------------
# 交易热度评分
# ---------------------------------------------------------------------------

def _compute_volume_heat_score(df, signal_filters):
    cfg = signal_filters.get('volume_heat') or {}
    if not cfg.get('enable', True):
        return None, {}
    ma_s = int(cfg.get('ma_short', 5))
    ma_l = int(cfg.get('ma_long', 20))
    ma_vr = int(cfg.get('ma_vol_recent', 3))
    w = cfg.get('weights') or {}
    w_trend = float(w.get('trend', 40))
    w_vol = float(w.get('vol_recent', 25))
    w_amt = float(w.get('amount', 35))
    lookback = int(cfg.get('percentile_lookback', 120))
    min_pct_samples = int(cfg.get('percentile_min_samples', 30))
    use_pct = bool(cfg.get('use_percentile_trend', True))
    liq_floor = float(cfg.get('liquidity_floor', 1e8))

    if 'volume' not in df.columns or len(df) < ma_l + 1:
        return None, {}
    vol = pd.to_numeric(df['volume'], errors='coerce').fillna(0.0)
    last_idx = len(df) - 1
    start_l = max(0, last_idx - ma_l + 1)
    vol_ma_l = float(vol.iloc[start_l:last_idx + 1].mean())
    if vol_ma_l <= 0:
        return None, {}
    start_s = max(0, last_idx - ma_s + 1)
    vol_ma_s = float(vol.iloc[start_s:last_idx + 1].mean())
    r_trend = vol_ma_s / vol_ma_l

    def _r_trend_at(j):
        if j < ma_l - 1:
            return None
        ss = max(0, j - ma_s + 1)
        sl = max(0, j - ma_l + 1)
        vs = float(vol.iloc[ss:j + 1].mean())
        vl = float(vol.iloc[sl:j + 1].mean())
        if vl <= 0:
            return None
        return vs / vl

    def _lin_map(x, lo, hi, out_max):
        if hi <= lo:
            return 0.0
        t = (x - lo) / (hi - lo)
        t = max(0.0, min(1.0, t))
        return round(t * out_max, 2)

    hist_trend = []
    if use_pct and last_idx >= ma_l - 1:
        j0 = max(ma_l - 1, last_idx - lookback + 1)
        for j in range(j0, last_idx + 1):
            rt = _r_trend_at(j)
            if rt is not None:
                hist_trend.append(rt)
    if use_pct and len(hist_trend) >= min_pct_samples:
        sh = sorted(hist_trend)
        rank_frac = bisect.bisect_right(sh, r_trend) / len(sh)
        s_trend = round(rank_frac * w_trend, 2)
    else:
        s_trend = _lin_map(r_trend, 0.75, 1.25, w_trend)

    vr_start = max(0, last_idx - ma_vr + 1)
    vol_ma_recent = float(vol.iloc[vr_start:last_idx + 1].mean())
    r_vol_recent = vol_ma_recent / vol_ma_l
    s_vol_raw = _lin_map(r_vol_recent, 0.8, 1.6, w_vol)
    dep = min(1.0, r_trend)
    s_vol = round(s_vol_raw * dep, 2)

    s_amt = 0.0
    r_amt = None
    if 'amount' in df.columns:
        amt = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
        amt_ma_l = float(amt.iloc[start_l:last_idx + 1].mean())
        amt_ma_s = float(amt.iloc[start_s:last_idx + 1].mean())
        if amt_ma_l > 0:
            r_amt = amt_ma_s / amt_ma_l
            s_amt = _lin_map(r_amt, 0.75, 1.25, w_amt)
    else:
        if last_idx >= ma_s + ma_l:
            prev_start = max(0, last_idx - ma_s - ma_l + 1)
            prev_end = last_idx - ma_s + 1
            vol_prev = float(vol.iloc[prev_start:prev_end].mean())
            if vol_prev > 0:
                r_amt = vol_ma_s / vol_prev
                s_amt = _lin_map(r_amt, 0.85, 1.35, w_amt)

    total_base = s_trend + s_vol + s_amt
    liq_factor = 1.0
    if 'amount' in df.columns and liq_floor > 0:
        amt = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
        avg_amt_20 = float(amt.iloc[start_l:last_idx + 1].mean())
        liq_factor = min(1.0, avg_amt_20 / liq_floor)

    total = round(min(100.0, total_base * liq_factor), 1)
    detail = {
        'vol_ma_ratio_5_20': round(r_trend, 3),
        'volume_ratio_3d_ma20': round(r_vol_recent, 3),
        'amount_ma_ratio_5_20': round(r_amt, 3) if r_amt is not None else None,
        'score_trend': s_trend,
        'score_vol_recent': s_vol,
        'score_vol_recent_raw': s_vol_raw,
        'trend_dependency': round(dep, 3),
        'score_amount': s_amt,
        'liquidity_discount': round(liq_factor, 3),
        'raw_total_before_liq': round(total_base, 2),
    }
    return total, detail


# ---------------------------------------------------------------------------
# 信号分析（完整版，不依赖 self）
# ---------------------------------------------------------------------------

def _analyze_signals(df, stock_code, current_time, signal_filters):
    """
    analyze_signals 的独立版本——与 StockKlineSpider.analyze_signals 逻辑一致，
    但不依赖 self，可在子进程中调用。
    返回与原方法相同的 dict。
    """
    signals = []
    signal_stats = {
        'kdj_oversold': {'success': 0, 'total': 0},
        'kdj_golden_cross': {'success': 0, 'total': 0},
        'kdj_divergence': {'success': 0, 'total': 0},
        'macd_golden_cross': {'success': 0, 'total': 0},
        'macd_zero_cross': {'success': 0, 'total': 0},
        'macd_divergence': {'success': 0, 'total': 0},
        'rsi_oversold': {'success': 0, 'total': 0},
        'rsi_golden_cross': {'success': 0, 'total': 0},
        'boll_bottom_touch': {'success': 0, 'total': 0},
        'boll_width_expand': {'success': 0, 'total': 0},
        'ma_golden_cross': {'success': 0, 'total': 0},
        'ma_support': {'success': 0, 'total': 0},
        'dmi_golden_cross': {'success': 0, 'total': 0},
        'dmi_adx_strong': {'success': 0, 'total': 0},
        'cci_oversold': {'success': 0, 'total': 0},
        'cci_zero_cross': {'success': 0, 'total': 0},
        'roc_zero_cross': {'success': 0, 'total': 0},
        'roc_divergence': {'success': 0, 'total': 0},
    }

    df = df.sort_index()
    min_history_days = signal_filters.get('min_history_days', 60)
    success_window_days = signal_filters.get('success_window_days', 14)
    empty_result = {
        'signal_stats': {},
        'overall_success_rate': 0,
        'total_signals': 0,
        'total_success': 0,
        'signals': [],
        'recent_signals': [],
    }
    if len(df) < min_history_days:
        return empty_result

    # ---------- 历史信号统计 ----------
    for i in range(1, len(df) - success_window_days):
        current_row = df.iloc[i]
        prev_row = df.iloc[i - 1]
        signals_for_day = _detect_signals(df, current_row, prev_row, i, is_recent=False)

        for signal, signal_type in signals_for_day:
            future_prices = df.iloc[i + 1:i + 1 + success_window_days]['close']
            if len(future_prices) < success_window_days or future_prices.isna().all():
                max_future_return = None
                success = None
            else:
                max_future_return = round(
                    ((future_prices.max() - current_row['close']) / current_row['close'] * 100), 2
                )
                success = max_future_return >= 5
                signal_stats[signal_type]['total'] += 1
                if success:
                    signal_stats[signal_type]['success'] += 1

            signals.append({
                'date': pd.to_datetime(df.index, format='%Y-%m-%d'),
                'signal_type': signal_type,
                'signal': signal,
                'close': current_row['close'],
                'k_value': current_row.get('K_9_3'),
                'd_value': current_row.get('D_9_3'),
                'j_value': current_row.get('J_9_3'),
                'macd': current_row.get('MACD_12_26_9'),
                'macd_signal': current_row.get('MACDs_12_26_9'),
                'rsi_6': current_row.get('RSI_6'),
                'rsi_12': current_row.get('RSI_12'),
                'cci': current_row.get('CCI_20'),
                'roc': current_row.get('ROC_12'),
                'dmi_plus': current_row.get('DMP_14'),
                'dmi_minus': current_row.get('DMN_14'),
                'adx': current_row.get('ADX_14'),
                'max_return': max_future_return,
                'success': success,
            })

    total_success = sum(s['success'] for s in signal_stats.values())
    total_signals = sum(s['total'] for s in signal_stats.values())
    overall_success_rate = round((total_success / total_signals * 100), 2) if total_signals > 0 else 0

    success_rates = {}
    for stype, stats in signal_stats.items():
        sr = round((stats['success'] / stats['total'] * 100), 2) if stats['total'] > 0 else 0
        success_rates[stype] = {
            'success_rate': sr,
            'total_signals': stats['total'],
            'success_count': stats['success'],
        }

    # ---------- 最近 3 天信号 ----------
    recent_signals = []
    valuation_checked = 0
    valuation_blocked = 0
    valuation_missing = 0
    valuation_candidates = 0

    if len(df) >= 4:
        df.index = pd.to_datetime(df.index)
        trading_days = df[df.index.dayofweek < 5].index
        last_3_trading_days = trading_days[-3:]
        last_3_days = df.loc[last_3_trading_days].copy()

        if current_time != last_3_trading_days[-1].strftime('%Y-%m-%d'):
            return {
                'signal_stats': 0,
                'overall_success_rate': 0,
                'total_signals': 0,
                'total_success': 0,
                'signals': [],
                'recent_signals': [],
            }

        for i in range(len(last_3_days)):
            current_row = last_3_days.iloc[i]
            if i > 0:
                prev_row = last_3_days.iloc[i - 1]
            else:
                prev_date = trading_days[trading_days.get_loc(last_3_trading_days[0]) - 1]
                prev_row = df.loc[prev_date]

            signals_for_day = _detect_signals(df, current_row, prev_row, i, is_recent=True)

            for signal, signal_type in signals_for_day:
                valuation_candidates += 1
                current_pos = df.index.get_loc(last_3_days.index[i])
                if _is_st(df, current_pos, signal_filters):
                    continue
                if not _passes_trade_status(df, current_pos, signal_filters):
                    continue
                if not _passes_liquidity_filters(df, current_pos, signal_type, signal_filters):
                    continue
                passed, status = _check_valuation_filters(df, signal_filters)
                if status in ('passed', 'blocked'):
                    valuation_checked += 1
                elif status == 'missing':
                    valuation_missing += 1
                if status == 'blocked':
                    valuation_blocked += 1
                if not passed:
                    continue
                if (signal_stats[signal_type]['total'] > 8
                        and success_rates[signal_type]['success_rate'] >= 60
                        and overall_success_rate >= 50):
                    signal_data = {
                        'date': last_3_days.index[i],
                        'signal_type': signal_type,
                        'signal': signal,
                        'close': current_row['close'],
                        'signal_total': signal_stats[signal_type]['total'],
                        'signal_success_rate': success_rates[signal_type]['success_rate'],
                        'overall_success_rate': overall_success_rate,
                    }
                    if signal_type.startswith('kdj'):
                        signal_data.update({
                            'k_value': current_row.get('K_9_3'),
                            'd_value': current_row.get('D_9_3'),
                            'j_value': current_row.get('J_9_3'),
                        })
                    elif signal_type.startswith('macd'):
                        signal_data.update({
                            'macd': current_row.get('MACD_12_26_9'),
                            'macd_signal': current_row.get('MACDs_12_26_9'),
                        })
                    elif signal_type.startswith('rsi'):
                        signal_data.update({
                            'RSI_6': current_row.get('RSI_6'),
                            'RSI_12': current_row.get('RSI_12'),
                        })
                    elif signal_type.startswith('boll'):
                        signal_data.update({
                            'BBL_20_2.0': current_row.get('BBL_20_2.0'),
                            'BBM_20_2.0': current_row.get('BBM_20_2.0'),
                            'BBU_20_2.0': current_row.get('BBU_20_2.0'),
                        })
                    elif signal_type.startswith('ma'):
                        signal_data.update({
                            'SMA_5': current_row.get('SMA_5'),
                            'SMA_20': current_row.get('SMA_20'),
                        })
                    elif signal_type.startswith('dmi'):
                        signal_data.update({
                            'DMP_14': current_row.get('DMP_14'),
                            'DMN_14': current_row.get('DMN_14'),
                            'ADX_14': current_row.get('ADX_14'),
                        })
                    elif signal_type.startswith('cci'):
                        signal_data.update({'CCI_20': current_row.get('CCI_20')})
                    elif signal_type.startswith('roc'):
                        signal_data.update({'ROC_12': current_row.get('ROC_12')})
                    recent_signals.append(signal_data)

    valuation_info = {
        'checked': valuation_checked,
        'blocked': valuation_blocked,
        'missing': valuation_missing,
        'candidates': valuation_candidates,
    }
    return {
        'signal_stats': success_rates,
        'overall_success_rate': overall_success_rate,
        'total_signals': total_signals,
        'total_success': total_success,
        'signals': signals,
        'recent_signals': recent_signals,
        'valuation_info': valuation_info,
    }


# ---------------------------------------------------------------------------
# 信号检测（KDJ / MACD / RSI / BOLL / MA / DMI / CCI / ROC）
# ---------------------------------------------------------------------------

def _detect_signals(df, current_row, prev_row, i, is_recent=False):
    """返回 [(signal_name, signal_type), ...] 列表。"""
    signals_for_day = []

    # KDJ
    if (current_row.get('K_9_3') is not None and current_row.get('D_9_3') is not None
            and current_row['K_9_3'] < 20 and current_row['D_9_3'] < 20):
        signals_for_day.append(('KDJ超卖', 'kdj_oversold'))
    if (prev_row.get('K_9_3') is not None and prev_row.get('D_9_3') is not None
            and current_row.get('K_9_3') is not None and current_row.get('D_9_3') is not None
            and prev_row['K_9_3'] < prev_row['D_9_3']
            and current_row['K_9_3'] > current_row['D_9_3']):
        signals_for_day.append(('KDJ金叉', 'kdj_golden_cross'))
    if current_row.get('K_9_3') is not None:
        if is_recent:
            if (current_row['close'] < df.iloc[-3:].iloc[:i + 1]['close'].min()
                    and current_row['K_9_3'] > df.iloc[-3:].iloc[:i + 1]['K_9_3'].min()):
                signals_for_day.append(('KDJ底背离', 'kdj_divergence'))
        else:
            if (current_row['close'] < df.iloc[i - 5:i]['close'].min()
                    and current_row['K_9_3'] > df.iloc[i - 5:i]['K_9_3'].min()):
                signals_for_day.append(('KDJ底背离', 'kdj_divergence'))

    # MACD
    if (prev_row.get('MACD_12_26_9') is not None and prev_row.get('MACDs_12_26_9') is not None
            and current_row.get('MACD_12_26_9') is not None and current_row.get('MACDs_12_26_9') is not None
            and prev_row['MACD_12_26_9'] < prev_row['MACDs_12_26_9']
            and current_row['MACD_12_26_9'] > current_row['MACDs_12_26_9']):
        signals_for_day.append(('MACD金叉', 'macd_golden_cross'))
    if (prev_row.get('MACD_12_26_9') is not None and prev_row['MACD_12_26_9'] < 0
            and current_row.get('MACD_12_26_9') is not None and current_row['MACD_12_26_9'] > 0):
        signals_for_day.append(('MACD零轴上穿', 'macd_zero_cross'))
    if current_row.get('MACD_12_26_9') is not None:
        if is_recent:
            if (current_row['close'] < df.iloc[-3:].iloc[:i + 1]['close'].min()
                    and current_row['MACD_12_26_9'] > df.iloc[-3:].iloc[:i + 1]['MACD_12_26_9'].min()):
                signals_for_day.append(('MACD底背离', 'macd_divergence'))
        else:
            if (current_row['close'] < df.iloc[i - 5:i]['close'].min()
                    and current_row['MACD_12_26_9'] > df.iloc[i - 5:i]['MACD_12_26_9'].min()):
                signals_for_day.append(('MACD底背离', 'macd_divergence'))

    # RSI
    if current_row.get('RSI_6') is not None and current_row['RSI_6'] < 20:
        signals_for_day.append(('RSI超卖', 'rsi_oversold'))
    if (prev_row.get('RSI_6') is not None and prev_row.get('RSI_12') is not None
            and current_row.get('RSI_6') is not None and current_row.get('RSI_12') is not None
            and prev_row['RSI_6'] < prev_row['RSI_12']
            and current_row['RSI_6'] > current_row['RSI_12']):
        signals_for_day.append(('RSI金叉', 'rsi_golden_cross'))

    # BOLL
    if (current_row.get('BBL_20_2.0') is not None
            and current_row['close'] <= current_row['BBL_20_2.0'] * 1.01):
        signals_for_day.append(('BOLL下轨支撑', 'boll_bottom_touch'))
    if (current_row.get('BBB_20_2.0') is not None and prev_row.get('BBB_20_2.0') is not None
            and current_row['BBB_20_2.0'] > prev_row['BBB_20_2.0'] * 1.1):
        signals_for_day.append(('BOLL带宽扩张', 'boll_width_expand'))

    # MA
    if (prev_row.get('SMA_5') is not None and prev_row.get('SMA_20') is not None
            and current_row.get('SMA_5') is not None and current_row.get('SMA_20') is not None
            and prev_row['SMA_5'] < prev_row['SMA_20']
            and current_row['SMA_5'] > current_row['SMA_20']):
        signals_for_day.append(('MA5上穿MA20', 'ma_golden_cross'))
    if (current_row.get('SMA_20') is not None
            and current_row['close'] > current_row['SMA_20'] * 0.99
            and current_row['close'] < current_row['SMA_20'] * 1.01):
        signals_for_day.append(('MA20支撑', 'ma_support'))

    # DMI
    if (prev_row.get('DMP_14') is not None and prev_row.get('DMN_14') is not None
            and current_row.get('DMP_14') is not None and current_row.get('DMN_14') is not None
            and current_row.get('ADX_14') is not None
            and prev_row['DMP_14'] < prev_row['DMN_14']
            and current_row['DMP_14'] > current_row['DMN_14']
            and current_row['ADX_14'] > 20):
        signals_for_day.append(('DMI金叉', 'dmi_golden_cross'))
    if current_row.get('ADX_14') is not None and current_row['ADX_14'] > 30:
        signals_for_day.append(('ADX强势', 'dmi_adx_strong'))

    # CCI
    if current_row.get('CCI_20') is not None and current_row['CCI_20'] < -100:
        signals_for_day.append(('CCI超卖', 'cci_oversold'))
    if (prev_row.get('CCI_20') is not None and current_row.get('CCI_20') is not None
            and prev_row['CCI_20'] < 0 and current_row['CCI_20'] > 0):
        signals_for_day.append(('CCI零轴上穿', 'cci_zero_cross'))

    # ROC
    if (prev_row.get('ROC_12') is not None and current_row.get('ROC_12') is not None
            and prev_row['ROC_12'] < 0 and current_row['ROC_12'] > 0):
        signals_for_day.append(('ROC零轴上穿', 'roc_zero_cross'))
    if current_row.get('ROC_12') is not None:
        if is_recent:
            if (current_row['close'] < df.iloc[-3:].iloc[:i + 1]['close'].min()
                    and current_row['ROC_12'] > df.iloc[-3:].iloc[:i + 1]['ROC_12'].min()):
                signals_for_day.append(('ROC底背离', 'roc_divergence'))
        else:
            if (current_row['close'] < df.iloc[i - 5:i]['close'].min()
                    and current_row['ROC_12'] > df.iloc[i - 5:i]['ROC_12'].min()):
                signals_for_day.append(('ROC底背离', 'roc_divergence'))

    return signals_for_day


# ---------------------------------------------------------------------------
# 顶层 worker 入口 —— ProcessPoolExecutor 调用此函数
# ---------------------------------------------------------------------------

def compute_signals_for_stock(stock_code, stock_name, df, indicators_config, signal_filters, current_time):
    """
    在子进程中执行的 worker 函数。
    返回 dict:
      - stock_code, stock_name
      - kdj_analysis: analyze_signals 的完整返回
      - heat_score: float | None
      - last_close_price: float
      - df: 带指标的 DataFrame（update_price_extremes 需要）
      - error: str | None
    """
    try:
        min_history_days = signal_filters.get('min_history_days', 60)
        if len(df) < min_history_days:
            return {
                'stock_code': stock_code,
                'stock_name': stock_name,
                'skip': True,
                'reason': f'数据量不足{min_history_days}天',
            }

        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        last_close_price = df.iloc[-1]['close']

        df = TechnicalIndicators.calculate_all(df, indicators_config)
        kdj_analysis = _analyze_signals(df, stock_code, current_time, signal_filters)

        vh, _ = _compute_volume_heat_score(df, signal_filters)

        return {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'skip': False,
            'kdj_analysis': kdj_analysis,
            'heat_score': vh,
            'last_close_price': last_close_price,
            'df': df,
            'error': None,
        }
    except Exception as e:
        import traceback
        return {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'skip': False,
            'error': f"{e}\n{traceback.format_exc()}",
        }
