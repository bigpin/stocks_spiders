#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股实时/近期 K 线抓取 CLI（a-stock-realtime-kline skill 附带工具）v1.1

三源自动降级：东方财富 push2his（主）→ 腾讯 ifzq.gtimg.cn → 新浪 CN_MarketDataService。
分钟/日线均支持，盘中实时。核心抓取逻辑抽取自 gapup 流水线每日实战代码。

统一出口径（关键）：
- volume 统一"股"（东财/腾讯原始为手已×100，新浪原生股）
- amount 统一"元"（腾讯分钟线无 amount 置 NaN；新浪日线恒 0 置 NaN）
- 分钟 bar 时间统一"起始时刻"：东财原生起始；新浪/腾讯为结束时刻标注，
  本工具已自动减去周期换算（baostock 也是结束时刻，对照时注意）
- 价格：东财/腾讯前复权；新浪不复权序列（跨除权日慎用）
- 日线 pct_chg 百分数；turn 仅东财有，其余源 NaN

限速与熔断：东财 1.2s+0.5s 抖动+指数退避（实测参数）；腾讯/新浪 0.3s 间隔+短重试；
任一源连续失败 3 次即熔断，本次运行内跳过该源直达下一源。

安全约束：仅 https、仅白名单 host，拒绝环回/私有/保留地址。
"""
import argparse
import ipaddress
import json
import os
import random
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

try:
    import pandas as pd
except ImportError:
    print("需要 pandas：pip install pandas", file=sys.stderr)
    sys.exit(2)

# ---------------- 常量（与 Spiders/spiders/stock_config.py 及实测保持一致） ----------------
EM_KLINE_URL = 'https://push2his.eastmoney.com/api/qt/stock/kline/get'
EM_UT = 'fa5fd1943c7b386f172d6893dbfba10b'
EM_HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'),
    'Referer': 'https://quote.eastmoney.com/',
    'Accept': '*/*',
}
TX_URL = 'https://ifzq.gtimg.cn/appstock/app/kline/mkline'      # 分钟线
TX_DAILY_URL = 'https://ifzq.gtimg.cn/appstock/app/fqkline/get'  # 日线
TX_HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
              'Referer': 'https://gu.qq.com/'}
SINA_URL = 'https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData'
SINA_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Referer': 'https://finance.sina.com.cn',
}

ALLOWED_HOSTS = {'push2his.eastmoney.com', 'quotes.sina.cn', 'ifzq.gtimg.cn'}

EM_DELAY, EM_JITTER, EM_RETRIES = 1.2, 0.5, 4
LIGHT_DELAY, LIGHT_RETRIES, LIGHT_BACKOFF = 0.3, 2, 3.0
CIRCUIT_THRESHOLD = 3
SINA_DATALen_CAP = 1900   # 实测上限（≥2000 返回 null）
TX_DATALen_CAP = 320      # 实测上限（请求 2000 只回 320）
EM_KLT = {1: '1', 5: '5', 15: '15', 30: '30', 60: '60', 101: '101'}
TX_MK = {1: 'm1', 5: 'm5', 15: 'm15', 30: 'm30', 60: 'm60'}
SOURCE_ORDER = ['em', 'tx', 'sina']

# 熔断计数（进程内）
_circuit = {s: 0 for s in SOURCE_ORDER}


def _die(msg, code=2):
    print(f"[rt_kline] {msg}", file=sys.stderr)
    sys.exit(code)


def _note(msg):
    print(f"[rt_kline] {msg}", file=sys.stderr)


def _url_guard(url):
    """安全校验：仅 https + 白名单 host；host 为 IP 字面量时拒绝私有/环回/保留地址。"""
    p = urllib.parse.urlsplit(url)
    if p.scheme != 'https':
        raise ValueError(f"仅允许 https 请求，拒绝: {url}")
    host = (p.hostname or '').lower()
    if host not in ALLOWED_HOSTS:
        raise ValueError(f"host 不在白名单 {sorted(ALLOWED_HOSTS)}: {host or '(空)'}")
    try:
        ip = ipaddress.ip_address(host)
        if not ip.is_global:
            raise ValueError(f"拒绝非公网地址: {host}")
    except ValueError:
        if host in ALLOWED_HOSTS:
            pass
        else:
            raise
    return url


def _http_get_json(url, headers, timeout=30):
    with urllib.request.urlopen(_url_guard(url), timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _with_retries(fn, retries, backoff, what):
    last = None
    for attempt in range(retries + 1):
        try:
            return fn()
        except Exception as e:
            last = e
            if attempt < retries:
                time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"{what} 重试{retries}次仍失败: {last}")


# ---------------- 代码转换（抽取自 gapup/common.py） ----------------
def normalize_code(code):
    code = str(code).strip().lower()
    if '.' in code:
        pre, num = code.split('.')
        if pre in ('sh', 'sz', 'bj'):
            return f"{pre}{num}"
        if code[0].isdigit() and len(code) == 8:  # 东财 secid 形式 1.600000
            pre = {'1': 'sh', '0': 'sz'}.get(code[0])
            if pre:
                return f"{pre}{code[2:]}"
        raise ValueError(f'不支持的代码: {code}')
    if code[:2] in ('sh', 'sz', 'bj'):
        return f"{code[:2]}{code[2:]}"
    if code.isdigit() and len(code) == 6:
        if code.startswith(('60', '68')):
            return f"sh{code}"
        if code.startswith(('00', '30')):
            return f"sz{code}"
        if code.startswith('92') or code[0] in '48':
            return f"bj{code}"
        raise ValueError(f'无法识别 6 位代码归属: {code}')
    raise ValueError(f'不支持的代码: {code}')


def to_eastmoney_secid(code):
    code = normalize_code(code)
    return {'sh': '1', 'sz': '0', 'bj': '0'}[code[:2]] + '.' + code[2:]


def _label_to_bar_start(hhmm, freq_min):
    """结束时刻标注 -> 起始时刻（会话对齐 5 分钟栅格，直接减周期安全）。

    兼容 '09:35'（新浪）与 '0935'（腾讯）两种串法。
    """
    digits = ''.join(c for c in hhmm if c.isdigit())
    s = int(digits[:2]) * 60 + int(digits[2:4]) - freq_min
    return f"{s // 60:02d}:{s % 60:02d}"


def _fmt_date(d):
    d = str(d).replace('-', '')
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


# ---------------- 东方财富（主源，原生起始时刻标注） ----------------
def _eastmoney_kline(secid, klt, lmt, beg='0', end='20500101'):
    """东财K线请求（限速/抖动/指数退避），返回 klines 列表或 None。

    实测：请求过快（间隔<1s 连续数十次）触发临时限流 RemoteDisconnected
    （含周末，持续数分钟~更久），退避后自恢复。
    """
    params = {
        'secid': secid,
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': klt, 'fqt': '1', 'ut': EM_UT,
        'beg': beg, 'end': end, 'lmt': str(lmt),
    }
    url = EM_KLINE_URL + '?' + '&'.join(f"{k}={v}" for k, v in params.items())
    last_err = None
    for attempt in range(EM_RETRIES + 1):
        time.sleep(EM_DELAY + random.uniform(0, EM_JITTER))
        try:
            data = _http_get_json(url, EM_HEADERS)
            if not data.get('data'):
                return None
            return data['data'].get('klines') or None
        except Exception as e:
            last_err = e
            time.sleep(min(60, 5 * (2 ** attempt)))  # 5/10/20/40s
    raise RuntimeError(f'东财请求重试{EM_RETRIES}次仍失败: {last_err}')


def fetch_minute_em(code, beg_date, end_date, freq_min=5):
    """东财分钟线。f51~: 日期时间(起始)/开/收/高/低/量(手)/额(元)。volume 已手→股。"""
    klines = _eastmoney_kline(to_eastmoney_secid(code), EM_KLT[freq_min], lmt=100000,
                              beg=str(beg_date).replace('-', ''),
                              end=str(end_date).replace('-', ''))
    if not klines:
        return None
    recs = [{
        'date': p[0][:10], 'bar_start': p[0][11:16],
        'open': float(p[1]), 'close': float(p[2]),
        'high': float(p[3]), 'low': float(p[4]),
        'volume': float(p[5]) * 100,
        'amount': float(p[6]),
    } for p in (line.split(',') for line in klines)]
    df = pd.DataFrame(recs).sort_values(['date', 'bar_start']).reset_index(drop=True)
    return df[(df['date'] >= _fmt_date(beg_date)) & (df['date'] <= _fmt_date(end_date))]


def fetch_daily_em(code, days=60):
    """东财近 N 根前复权日线。preclose 用 close.shift 校正（涨跌额基准是真实昨收，复权下除权日失真）。"""
    klines = _eastmoney_kline(to_eastmoney_secid(code), EM_KLT[101], lmt=days)
    if not klines:
        return None
    recs = [{
        'date': p[0],
        'open': float(p[1]), 'close': float(p[2]),
        'high': float(p[3]), 'low': float(p[4]),
        'preclose': float(p[2]) - float(p[9]),
        'volume': float(p[5]) * 100,
        'amount': float(p[6]),
        'pct_chg': float(p[8]),        # 原生百分数
        'turn': float(p[10]),          # 换手率% 仅此源有
    } for p in (line.split(',') for line in klines)]
    df = pd.DataFrame(recs)
    df['preclose'] = df['close'].shift(1).fillna(df['preclose'])
    return df


# ---------------- 腾讯（第二源，结束时刻标注，量纲=手） ----------------
def _tx_get(path, param):
    url = f"{TX_URL if path == 'mkline' else TX_DAILY_URL}?param={param}"
    time.sleep(LIGHT_DELAY)
    data = _http_get_json(url, TX_HEADERS)
    if data.get('code') != 0:
        return None
    return data.get('data') or {}


def fetch_minute_tx(code, freq_min=5, datalen=TX_DATALen_CAP):
    """腾讯分钟线（最近 ≤320 根，盘中实时，支持 1 分钟）。

    注意字段序：[时间, open, close, high, low, volume(手), {}, ?]——close 在第 2 位！
    无 amount。时间标注为结束时刻，这里已换算为起始时刻。
    """
    code_n = normalize_code(code)
    raw = _with_retries(
        lambda: _tx_get('mkline',
                        f"{code_n},{TX_MK[freq_min]},,{min(int(datalen), TX_DATALen_CAP)}"),
        LIGHT_RETRIES, LIGHT_BACKOFF, '腾讯分钟线') or {}
    arr = (raw.get(code_n) or {}).get(TX_MK[freq_min]) or []
    if not arr:
        return None
    recs = [{
        'date': f"{x[0][:4]}-{x[0][4:6]}-{x[0][6:8]}",
        'bar_start': _label_to_bar_start(x[0][8:12], freq_min),
        'open': float(x[1]), 'close': float(x[2]),
        'high': float(x[3]), 'low': float(x[4]),
        'volume': float(x[5]) * 100,   # 手 -> 股
        'amount': float('nan'),        # 无成交额字段
    } for x in arr]
    return pd.DataFrame(recs).sort_values(['date', 'bar_start']).reset_index(drop=True)


def fetch_daily_tx(code, days=120):
    """腾讯前复权日线（qfqday；字段序同上，close 第 2 位，量=手，无 amount/turn）。

    pct_chg 由 close.shift 计算值（东财为接口原生值）。
    """
    code_n = normalize_code(code)
    raw = _with_retries(
        lambda: _tx_get('fqkline', f"{code_n},day,,,{int(days)},qfq"),
        LIGHT_RETRIES, LIGHT_BACKOFF, '腾讯日线') or {}
    arr = (raw.get(code_n) or {}).get('qfqday') or (raw.get(code_n) or {}).get('day') or []
    if not arr:
        return None
    df = pd.DataFrame([{
        'date': x[0],
        'open': float(x[1]), 'close': float(x[2]),
        'high': float(x[3]), 'low': float(x[4]),
        'volume': float(x[5]) * 100,   # 手 -> 股
        'amount': float('nan'),
        'turn': float('nan'),
    } for x in arr])
    df['preclose'] = df['close'].shift(1)
    df['pct_chg'] = (df['close'] / df['preclose'] - 1) * 100
    return df[['date', 'open', 'high', 'low', 'close', 'preclose',
               'volume', 'amount', 'pct_chg', 'turn']]


def fetch_quote_tx(code):
    """腾讯实时快照（复用 mkline 响应内的 qt 字段，零额外域名）。

    qt 数组经典位序：1=名称 3=现价 4=昨收 5=今开 6=量(手) 30=时间
    31=涨跌 32=涨跌% 33=最高 34=最低 35="现价/量(手)/额(元)"组合串。
    """
    code_n = normalize_code(code)
    raw = _with_retries(
        lambda: _tx_get('mkline', f"{code_n},m5,,1"),
        LIGHT_RETRIES, LIGHT_BACKOFF, '腾讯快照') or {}
    qt = (raw.get(code_n) or {}).get('qt', {}).get(code_n) or []
    if len(qt) < 35:
        return None

    def _f(i):
        try:
            return float(qt[i])
        except (ValueError, TypeError):
            return float('nan')

    combo = str(qt[35]).split('/')
    try:
        volume = float(combo[1]) * 100   # 手 -> 股
    except (ValueError, IndexError):
        volume = _f(6) * 100
    try:
        amount = float(combo[2])         # 元
    except (ValueError, IndexError):
        amount = float('nan')
    t = str(qt[30])
    ts = (f"{t[:4]}-{t[4:6]}-{t[6:8]} {t[8:10]}:{t[10:12]}:{t[12:14]}"
          if len(t) >= 14 and t.isdigit() else t)
    return {'code': code_n, 'name': str(qt[1]), 'time': ts,
            'price': _f(3), 'preclose': _f(4), 'open': _f(5),
            'high': _f(33), 'low': _f(34),
            'pct_chg': _f(32), 'change': _f(31),
            'volume': volume, 'amount': amount}


# ---------------- 新浪（第三源，结束时刻标注，量纲=股） ----------------
def _sina_kline(code, scale, datalen):
    url = (f"{SINA_URL}?symbol={normalize_code(code)}&scale={scale}&ma=no"
           f"&datalen={min(int(datalen), SINA_DATALen_CAP)}")
    time.sleep(LIGHT_DELAY)
    return _with_retries(
        lambda: _http_get_json(url, SINA_HEADERS) or None,
        LIGHT_RETRIES, LIGHT_BACKOFF, '新浪K线')


def fetch_minute_sina(code, freq_min=5, datalen=SINA_DATALen_CAP):
    """新浪分钟线（最近 ≤1900 根，最细 5 分钟；时间标注为结束时刻，已换算为起始）。

    实测口径与 baostock 一致：volume=股、amount=元。
    """
    if freq_min == 1:
        raise ValueError('新浪最细 5 分钟线')
    data = _sina_kline(code, freq_min, datalen)
    if not data:
        return None
    recs = []
    for b in data:
        day = b.get('day', '')
        if len(day) < 16:
            continue
        recs.append({
            'date': day[:10],
            'bar_start': _label_to_bar_start(day[11:16], freq_min),
            'open': float(b['open']), 'close': float(b['close']),
            'high': float(b['high']), 'low': float(b['low']),
            'volume': float(b.get('volume') or 0),
            'amount': float(b.get('amount') or 0),
        })
    if not recs:
        return None
    return pd.DataFrame(recs).sort_values(['date', 'bar_start']).reset_index(drop=True)


def fetch_daily_sina(code, datalen=60):
    """新浪日线（scale=240）。无 turn/pct_chg，amount 恒为 0（置 NaN），preclose 用 shift 补。"""
    data = _sina_kline(code, 240, datalen)
    if not data:
        return None
    df = pd.DataFrame([{
        'date': b['day'][:10],
        'open': float(b['open']), 'close': float(b['close']),
        'high': float(b['high']), 'low': float(b['low']),
        'volume': float(b.get('volume') or 0),
        'amount': float('nan'),       # 实测恒为 0，置 NaN 防误用
        'turn': float('nan'),
    } for b in data])
    df['preclose'] = df['close'].shift(1)
    df['pct_chg'] = (df['close'] / df['preclose'] - 1) * 100
    return df[['date', 'open', 'high', 'low', 'close', 'preclose',
               'volume', 'amount', 'pct_chg', 'turn']]


# ---------------- 统一入口：多源链降级 + 熔断 ----------------
def _parse_chain(source):
    if source == 'auto':
        return list(SOURCE_ORDER)
    chain = [s.strip() for s in source.split(',') if s.strip()]
    bad = [s for s in chain if s not in SOURCE_ORDER]
    if bad:
        raise ValueError(f"未知源 {bad}（可选: em/tx/sina/auto 或逗号链）")
    if not chain:
        raise ValueError('空的源降级链')
    return chain


def _run_chain(chain, attempts, code=''):
    """按链逐源尝试：熔断跳过、失败计数、成功归零。返回 (df|None, source|'none')。"""
    for src in chain:
        if src not in attempts:
            _note(f"未知源 {src}，跳过（可选: em/tx/sina/auto）")
            continue
        if _circuit[src] >= CIRCUIT_THRESHOLD:
            _note(f"{src} 熔断中（连续失败{CIRCUIT_THRESHOLD}次），跳过")
            continue
        try:
            df = attempts[src]()
            _circuit[src] = 0
            if df is None or (hasattr(df, 'empty') and df.empty):
                _note(f"{code} {src}: 返回空")
                continue
            return df, src
        except Exception as e:
            _circuit[src] += 1
            _note(f"{code} {src}: 失败（{e}），尝试下一源"
                  f"（{src} 连续失败 {_circuit[src]}/{CIRCUIT_THRESHOLD}）")
    return None, 'none'


def fetch_minute_any(code, beg_date, end_date, freq_min=5, source='auto'):
    """统一分钟线入口。返回 (df|None, source名)。

    日期过滤放进每个源的尝试闭包内：某源回溯深度不够导致过滤后为空时，
    自动降级到下一源继续尝试（如 tx 只有近320根，够不到 beg 会落给 sina）。
    """
    chain = _parse_chain(source)
    if freq_min == 1:
        chain = [s for s in chain if s != 'sina']

    def _filtered(df):
        if df is None or df.empty:
            return None
        out = df[(df['date'] >= _fmt_date(beg_date)) & (df['date'] <= _fmt_date(end_date))]
        if out.empty:
            return None
        if out.iloc[0]['date'] > _fmt_date(beg_date):
            _note(f"{code}: 该源仅覆盖自 {out.iloc[0]['date']} 起，未达 beg {_fmt_date(beg_date)}"
                  f"（头部可能缺失，更早历史请用 baostock-data skill）")
        return out

    attempts = {
        'em': lambda: _filtered(fetch_minute_em(code, beg_date, end_date, freq_min=freq_min)),
        'tx': lambda: _filtered(fetch_minute_tx(code, freq_min=freq_min)),
        'sina': lambda: _filtered(fetch_minute_sina(code, freq_min=freq_min)),
    }
    df, src = _run_chain(chain, attempts, code)
    return df, src


def fetch_daily_any(code, days=60, source='auto'):
    """统一日线入口。返回 (df|None, source名)。"""
    chain = _parse_chain(source)
    attempts = {
        'em': lambda: fetch_daily_em(code, days=days),
        'tx': lambda: fetch_daily_tx(code, days=days),
        'sina': lambda: fetch_daily_sina(code, datalen=days),
    }
    df, src = _run_chain(chain, attempts, code)
    if df is not None:
        df = df.tail(int(days)).reset_index(drop=True)
    return df, src


# ---------------- CLI ----------------
def _emit(df, source, out=None, fmt='table'):
    df = df.copy()
    df['source'] = source
    if out:
        df.to_csv(out, index=False, encoding='utf-8-sig')
        print(f"共 {len(df)} 行（source={source}）→ {out}")
        return
    if fmt == 'csv':
        df.to_csv(sys.stdout, index=False)
        return
    if fmt == 'json':
        print(df.to_json(orient='records', force_ascii=False))
        return
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 240)
    if len(df) > 26:
        print(df.head(8))
        print(f"... 中间省略 {len(df) - 18} 行 ...")
        print(df.tail(10))
    else:
        print(df)
    print(f"共 {len(df)} 行（source={source}）")


def _collect_codes(codes, codes_file):
    """合并位置参数与 --codes-file（每行一个，# 注释行忽略）。"""
    merged = list(codes or [])
    if codes_file:
        try:
            with open(codes_file, encoding='utf-8') as f:
                merged += [ln.strip() for ln in f
                           if ln.strip() and not ln.strip().startswith('#')]
        except OSError as e:
            _die(f'读取 --codes-file 失败: {e}')
    if not merged:
        _die('未提供代码（位置参数或 --codes-file）')
    return merged


def _filter_closed_by(df, closed_by, freq_min):
    """只保留 closed_by 时刻已收盘的 bar（bar_start + freq <= 截止），决策场景用。"""
    import re
    if not re.fullmatch(r'\d{1,2}:\d{2}', closed_by.strip()):
        _die(f'--closed-by 格式应为 HH:MM，收到: {closed_by}')
    h, m = closed_by.strip().split(':')
    cut = int(h) * 60 + int(m)
    df = df[df['bar_start'].map(
        lambda t: int(t[:2]) * 60 + int(t[3:5]) + freq_min <= cut)]
    return df


def cmd_check(args):
    """三源连通性探测：各发 1 个最小请求，批量前先跑选源。"""
    ok = {}
    try:
        k = _eastmoney_kline('1.600000', EM_KLT[101], lmt=1)
        ok['eastmoney'] = bool(k)
    except Exception as e:
        ok['eastmoney'] = f'失败: {e}'
    for name, fn in (('tencent', lambda: fetch_daily_tx('sh600000', days=1)),
                     ('sina', lambda: fetch_daily_sina('sh600000', datalen=1))):
        try:
            df = fn()
            ok[name] = df is not None and not df.empty
        except Exception as e:
            ok[name] = f'失败: {e}'
    for k, v in ok.items():
        print(f"{k:10s} {'OK' if v is True else v}")
    if not all(v is True for v in ok.values()):
        sys.exit(4)


def cmd_verify(args):
    """交叉验证：从各存活源拉最近日线，比对收盘价一致性。"""
    code = args.codes[0]
    rows = {}
    for src, fn in (('em', lambda: fetch_daily_em(code, days=5)),
                    ('tx', lambda: fetch_daily_tx(code, days=5)),
                    ('sina', lambda: fetch_daily_sina(code, datalen=5))):
        try:
            df = fn()
            if df is None or df.empty:
                rows[src] = None
                continue
            last = df.iloc[-1]
            rows[src] = (last['date'], float(last['close']), float(last['volume']))
        except Exception as e:
            rows[src] = ('错误', str(e)[:60], None)
    print(f"{normalize_code(code)} 最近日线收盘价交叉验证：")
    closes = []
    for src, r in rows.items():
        if r is None:
            print(f"  {src:6s} 无数据")
        elif r[0] == '错误':
            print(f"  {src:6s} {r[1]}")
        else:
            print(f"  {src:6s} {r[0]}  close={r[1]:.3f}  volume={r[2]:.0f}(股)")
            closes.append(r[1])
    if len(closes) >= 2:
        diff = (max(closes) - min(closes)) / max(abs(c) for c in closes) * 100
        print(f"  收盘价最大离散度: {diff:.3f}%（各源收盘竞价快照时刻不同，<0.2% 属正常）")
    else:
        print("  存活源不足两个，无法交叉验证")


def cmd_quote(args):
    """实时快照（腾讯 qt）：现价/涨跌幅/量额/高低价，支持指数（sh000001 等）。"""
    rows, failed = [], []
    for code in _collect_codes(args.codes, args.codes_file):
        try:
            q = fetch_quote_tx(code)
        except ValueError as e:
            _die(str(e))
        if q is None:
            failed.append(code)
            _note(f"{code}: 快照无数据")
            continue
        rows.append(q)
    if rows:
        df = pd.DataFrame(rows)
        if args.format == 'json':
            print(df.to_json(orient='records', force_ascii=False))
        elif args.format == 'csv':
            df.to_csv(sys.stdout, index=False)
        else:
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 240)
            print(df.to_string(index=False))
    if failed:
        _die(f"以下代码无快照: {', '.join(failed)}", code=4)


def cmd_minute(args):
    if len(args.codes) > 1 and args.out:
        _die('多只股票请用 --out-dir，--out 仅支持单只')
    if args.freq == 1 and args.source in ('sina',):
        _die('1 分钟线新浪不支持（可用 em/tx/auto）')
    beg = _fmt_date(args.beg or (datetime.now() - timedelta(days=5)).strftime('%Y%m%d'))
    end = _fmt_date(args.end or datetime.now().strftime('%Y%m%d'))
    failed = []
    for code in _collect_codes(args.codes, args.codes_file):
        normalize_code(code)  # 前置校验：格式错误立即报，不进降级链污染源健康计数
        df, src = fetch_minute_any(code, beg, end, freq_min=args.freq, source=args.source)
        if args.closed_by and df is not None:
            df = _filter_closed_by(df, args.closed_by, args.freq)
        if df is None or df.empty:
            _note(f"{code}: 无数据（source={src}；检查日期是否交易日/代码/源回溯深度"
                  f"{'/closed-by 过滤后为空' if args.closed_by else ''}）")
            failed.append(code)
            continue
        out = args.out
        if args.out_dir:
            out = os.path.join(args.out_dir, f"{normalize_code(code)}_f{args.freq}.csv")
            os.makedirs(args.out_dir, exist_ok=True)
        _emit(df, src, out, args.format)
    if failed:
        _die(f"以下代码无数据: {', '.join(failed)}", code=4)


def cmd_daily(args):
    if len(args.codes) > 1 and args.out:
        _die('多只股票请用 --out-dir，--out 仅支持单只')
    failed = []
    for code in _collect_codes(args.codes, args.codes_file):
        normalize_code(code)  # 前置校验：格式错误立即报，不进降级链污染源健康计数
        df, src = fetch_daily_any(code, days=args.days, source=args.source)
        if df is None or df.empty:
            _note(f"{code}: 无数据（source={src}）")
            failed.append(code)
            continue
        out = args.out
        if args.out_dir:
            out = os.path.join(args.out_dir, f"{normalize_code(code)}_daily.csv")
            os.makedirs(args.out_dir, exist_ok=True)
        _emit(df, src, out, args.format)
    if failed:
        _die(f"以下代码无数据: {', '.join(failed)}", code=4)


def build_parser():
    p = argparse.ArgumentParser(
        prog='rt_kline',
        description='A股实时/近期K线三源抓取（东财→腾讯→新浪自动降级，限速+熔断保护）')
    sub = p.add_subparsers(dest='cmd', required=True)

    sp = sub.add_parser('check', help='三源连通性探测（批量前先跑）')
    sp.set_defaults(fn=cmd_check)

    sp = sub.add_parser('verify', help='交叉验证：各源最近日线收盘价比对')
    sp.add_argument('codes', nargs=1)
    sp.set_defaults(fn=cmd_verify)

    sp = sub.add_parser('quote', help='实时快照：现价/涨跌幅/量额（腾讯qt，支持指数）')
    sp.add_argument('codes', nargs='*', default=[])
    sp.add_argument('--codes-file')
    sp.add_argument('--format', default='table', choices=['table', 'csv', 'json'])
    sp.set_defaults(fn=cmd_quote)

    sp = sub.add_parser('minute', help='分钟线（盘中实时，含当日进行中bar）')
    sp.add_argument('codes', nargs='+', help='sh600000 / sz000001 / 600519 均可；指数需显式前缀如 sh000001')
    sp.add_argument('--freq', type=int, default=5, choices=[1, 5, 15, 30, 60],
                    help='1=东财+腾讯支持')
    sp.add_argument('--beg', help='YYYY-MM-DD，默认近5天')
    sp.add_argument('--end', help='默认今天')
    sp.add_argument('--source', default='auto',
                    help='auto(=em,tx,sina) / em / tx / sina / 逗号链如 tx,sina')
    sp.add_argument('--closed-by', help='HH:MM，只保留该时刻已收盘的bar（决策截止场景）')
    sp.add_argument('--codes-file', help='每行一个代码，#为注释；与位置参数合并')
    sp.add_argument('--format', default='table', choices=['table', 'csv', 'json'])
    sp.add_argument('--out', help='单只时代入 CSV')
    sp.add_argument('--out-dir', help='多只时每只一个 CSV 目录')
    sp.set_defaults(fn=cmd_minute)

    sp = sub.add_parser('daily', help='近期日线（换手/涨幅东财最全；盘中含今日进行中bar）')
    sp.add_argument('codes', nargs='+')
    sp.add_argument('--days', type=int, default=60)
    sp.add_argument('--source', default='auto',
                    help='auto / em / tx / sina / 逗号链')
    sp.add_argument('--codes-file')
    sp.add_argument('--format', default='table', choices=['table', 'csv', 'json'])
    sp.add_argument('--out')
    sp.add_argument('--out-dir')
    sp.set_defaults(fn=cmd_daily)
    return p


def main():
    args = build_parser().parse_args()
    try:
        args.fn(args)
    except ValueError as e:
        _die(str(e))
    except KeyboardInterrupt:
        _die('用户中断', code=130)


if __name__ == '__main__':
    main()
