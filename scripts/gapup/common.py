#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
gapup 公共模块：数据库 schema、股票代码转换、baostock/东财拉取封装、单位与时间对齐。

数据口径约定（训练与推理必须一致）：
- 价格统一前复权：baostock adjustflag='2'，东财 fqt=1。
  高开标签用复权价计算，除权日的"假跳空"被自然抵消（现金分红部分视为收益）。
- 成交量统一为"股"：baostock 原生为股；东财分钟K线为"手"，入库前 ×100。
- 分钟 bar 统一记录"起始时刻"（HH:MM）：baostock 原生 time 为结束时刻（如 20260918150000000
  表示 14:55-15:00 这根），转换时减去 bar 周期；东财原生即起始时刻（如 "2026-09-21 10:30"
  表示 10:30-10:35）。
- "截至 14:30 决策" = 只取在该时刻已收盘的 bar：bar_start + freq <= 14:30，
  5 分钟线下最后一根为 14:25 起始的 bar。
"""

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta

# 向上查找包含 Spiders/ 的目录作为项目根（脚本可从任意 cwd 启动）
_p = os.path.dirname(os.path.abspath(__file__))
while _p and _p != os.path.dirname(_p) and not os.path.isdir(os.path.join(_p, 'Spiders')):
    _p = os.path.dirname(_p)
if not _p or not os.path.isdir(os.path.join(_p, 'Spiders')):
    raise RuntimeError('未找到项目根目录（应包含 Spiders/）')
PROJECT_ROOT = _p
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Spiders.spiders.stock_config import KLINE_API, STOCK_PREFIX_MAP, HEADERS

import pandas as pd

# ---------------------------------------------------------------------------
# 路径与全局配置
# ---------------------------------------------------------------------------
GAPUP_DIR = os.path.dirname(os.path.abspath(__file__))
GAPUP_DB_PATH = os.path.join(PROJECT_ROOT, 'gapup.db')
MODELS_DIR = os.path.join(PROJECT_ROOT, 'models')
LOG_FILE = os.path.join(PROJECT_ROOT, 'logs', 'gapup.log')
DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'gapup')  # parquet 等中间产物

CONFIG = {
    'gap_threshold_pct': 1.0,   # 高开定义：次日开盘涨幅 >= 此值（相对当日收盘，复权口径）
    'cutoff': '14:30',          # 盘中决策时点，特征按此截断
    'minute_freq_min': 5,       # 分钟 bar 周期
    'hist_start': '2020-01-01', # baostock 分钟线最早可用日期（实测）
    'eastmoney_delay': 1.2,     # 东财请求间隔（秒）——实测过快会触发临时限流
    'eastmoney_jitter': 0.5,    # 请求间隔随机抖动（秒）
    'eastmoney_retries': 4,     # 请求失败重试次数（指数退避）
    'train_end': None,          # 训练数据截止（None=库里有什么用什么）
    # baostock 配额与并发（账号级限制：每日 5 万次，gapup 侧自限 4 万留富余；
    # 主 KDJ 系统约 5-6K/日不走此计数器）
    'baostock_daily_limit': 40000,   # gapup 每日请求上限（含在账号 5 万之内）
    'baostock_workers': 2,           # gapup 的 baostock 并发进程数
    # 一级筛选（日线规则，全市场 -> 每日候选池）；训练样本按同一规则过滤以对齐分布
    'screener': {
        'min_avg_amount_20d': 1.5e8,  # 流动性：近20日均成交额（元）
        'min_turnover_prev': 2.0,     # 昨日换手率下限（%）
        'pos_vs_20d_high_min': -0.08, # 昨收距20日收盘高点下限（强势区）
        'ret5_min': 0.0,              # 近5日涨幅下限（有动量）
        'ret5_max': 0.25,             # 近5日涨幅上限（未透支）
        'vol_ma_ratio_min': 1.1,      # 5日均量/20日均量下限（量能放大）
        'max_candidates': 100,        # 每日候选池规模（按复合分排序截取）
    },
    'top_n_output': 5,          # 最终精选输出数量
}

FEATURE_COLUMNS = [
    # 当日盘中（截至 14:30）
    'ret_so_far',          # 现价/昨收-1
    'open_gap',            # 当日开盘涨幅（open/昨收-1，由首根bar近似）
    'morning_ret',         # 上午收益（昨收→11:30）
    'afternoon_ret',       # 午后收益（13:00→14:30）
    'vwap_pos',            # 现价相对当日VWAP
    'day_range_pos',       # 现价处于当日高低区间位置
    'pullback_from_high',  # 距盘中最高点回撤
    'intraday_range',      # 当日振幅（截至14:30）
    'vol_ratio_same',      # 当日截至14:30量 / 近20日同时段均量
    'late_surge',          # 13:00→14:30 涨幅（尾盘拉升迹象）
    'last30m_momentum',    # 14:00→14:30 涨幅
    'n_new_highs',         # 盘中创当日新高次数（bar收盘价新高）
    # 历史日线（T-1 及更早）
    'prev_ret_1',          # 昨日涨幅
    'prev_turn',           # 昨日换手率
    'prev_amplitude',      # 昨日振幅
    'ret_5', 'ret_10', 'ret_20',   # 近5/10/20日涨幅（T-1收盘口径）
    'up_days',             # 连涨天数（截至T-1）
    'vol_ma_ratio',        # 近5日均量/近20日均量（截至T-1）
    'pos_vs_20d_high',     # 昨收距近20日最高收盘位置
    'is_20d_close_high',   # 昨收是否创20日收盘新高
]


def get_gapup_logger():
    """独立日志文件，避免被 run.py 每日覆盖的 spiders.log 冲掉。"""
    from Spiders.common.log import get_logger
    return get_logger('gapup', LOG_FILE)


# ---------------------------------------------------------------------------
# 情景锚点：在 51.5 万样本（2020-2026）上挖掘的可解释条件经验高开率。
# 用途：预测报告中与模型概率并排展示，作为透明参照——
#       模型概率若与锚点严重背离，优先怀疑模型（树模型高档位常有过度自信）。
# 数字来源：训练样本的条件分组统计，跨年稳定（如封死+温和量 2020-22 为 71.8%、
#       2023-26 为 66.2%）。样本量：封死组 ~4.5K，温和量档 ~1.7K。
# ---------------------------------------------------------------------------
def scenario_anchor(feat):
    """按 14:30 可观测条件分类情景，返回 (情景名, 经验高开率|None)。

    情景判定优先级：今日涨停族 > 昨日深跌+今日反弹 > 一般强势/深跌 > 普通 Deep-drop
    家族高开率 19-27%（约 2 倍基础率）但次日收盘中位仅 0~+0.15%，属薄利边缘，
    锚点旁的情景名即提示这种差别。
    """
    ret = feat.get('ret_so_far')
    vr = feat.get('vol_ratio_same')
    ls = feat.get('late_surge')
    pd_ = feat.get('prev_ret_1')    # 昨日涨幅
    dp = feat.get('day_range_pos')  # 今日日内位置

    def _ok(v):
        return v is not None and v == v  # 非 None 且非 NaN

    if not _ok(ret):
        return '数据不足', None
    if ret >= 0.095:  # 14:30 已涨停
        sealed = _ok(ls) and abs(ls) < 0.002  # 尾盘无波动 = 封死
        if not sealed:
            if _ok(ls):
                return '涨停未封死(尾盘有异动)', 0.34
            return '涨停(封板状态未知)', 0.45
        if not _ok(vr):
            return '涨停封死', 0.59
        if vr < 1.5:
            return '涨停封死+温和量', 0.68
        if vr < 2.5:
            return '涨停封死+放量', 0.57
        return '涨停封死+巨量', 0.50
    if _ok(pd_) and pd_ <= -0.095:  # 跌停级：高开率不低但必须早盘了结（持有到收盘中位转负）
        if ret > 0 and _ok(dp) and dp > 0.7:
            return '昨跌停级+翻红高位(次日早盘了结)', 0.27
        return '昨跌停级(易冲高回落)', 0.20
    if _ok(pd_) and pd_ <= -0.05:  # 昨日深跌族（14:30 可正常买入）
        if ret > 0.03:
            return '昨深跌+今强反弹3%+', 0.26
        if ret > 0 and _ok(dp) and dp > 0.7:
            return '昨深跌+翻红收高位', 0.23
        if ret > 0:
            return '昨深跌+翻红', 0.20
        return '昨日深跌(未修复)', 0.19
    if ret >= 0.05:
        # 连续强势（昨日已涨+今日未封板收高位）：复合退出中位 +1.1%、胜率 65%、逐年皆正
        if _ok(pd_) and pd_ > 0.03 and _ok(dp) and dp > 0.85:
            return '连续强势未封板(高开竞价卖/否则10点卖)', 0.25
        return '强势大涨(5~9.5%)', 0.20
    if ret <= -0.03:
        return '深跌(<=-3%)', 0.16
    return '普通', 0.11


# ---------------------------------------------------------------------------
# 数据库
# ---------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_bars (
    stock_code TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL, high REAL, low REAL, close REAL, preclose REAL,
    volume REAL, amount REAL, turn REAL, pct_chg REAL,
    PRIMARY KEY (stock_code, date)
);

CREATE TABLE IF NOT EXISTS minute_bars (
    stock_code TEXT NOT NULL,
    date TEXT NOT NULL,
    bar_start TEXT NOT NULL,   -- 'HH:MM' bar起始时刻
    open REAL, high REAL, low REAL, close REAL,
    volume REAL,               -- 股
    amount REAL,               -- 元
    PRIMARY KEY (stock_code, date, bar_start)
);

CREATE TABLE IF NOT EXISTS pool_history (
    stock_code TEXT NOT NULL,
    date TEXT NOT NULL,        -- 池子触发日（KDJ产出日）
    stock_name TEXT,
    source TEXT DEFAULT 'kdj',
    PRIMARY KEY (stock_code, date)
);

CREATE TABLE IF NOT EXISTS fetch_progress (
    stock_code TEXT PRIMARY KEY,
    daily_fetched_until TEXT,  -- 日线已抓到哪天（含）
    minute_fetched_until TEXT, -- 分钟线已抓到哪天（含）
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS candidates (
    date TEXT NOT NULL,        -- 候选日（决策日T，由T-1收盘数据筛出）
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    screener_score REAL,       -- 一级筛选复合分（排序用）
    created_at TEXT,
    PRIMARY KEY (date, stock_code)
);

CREATE TABLE IF NOT EXISTS predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pred_date TEXT NOT NULL,   -- 预测运行日（决策日T）
    pool_date TEXT,            -- 池子来源日
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    prob REAL,
    rank INTEGER,
    created_at TEXT,
    UNIQUE (pred_date, stock_code)
);

CREATE TABLE IF NOT EXISTS outcomes (
    pred_date TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    t_close REAL,              -- 决策日收盘价
    t1_open REAL,              -- 次日开盘价
    t1_open_gap REAL,          -- 次日开盘涨幅%（复权口径）
    is_high_open INTEGER,      -- 1=命中高开定义
    checked_at TEXT,
    PRIMARY KEY (pred_date, stock_code)
);

CREATE INDEX IF NOT EXISTS idx_minute_code_date ON minute_bars(stock_code, date);
CREATE INDEX IF NOT EXISTS idx_pool_date ON pool_history(date);
CREATE INDEX IF NOT EXISTS idx_pred_date ON predictions(pred_date);
"""


def connect_db():
    import sqlite3
    conn = sqlite3.connect(GAPUP_DB_PATH)
    # WAL + 忙等待：允许多进程（抓取/筛选/预测）并发读写而不报 database is locked
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(_SCHEMA)
    return conn


def import_pool_history(source_db=None):
    """从 stock_signals.db 的 stock_signals 表导入历史每日池子（幂等）。

    返回本次新导入的行数。
    """
    import sqlite3
    source_db = source_db or os.path.join(PROJECT_ROOT, 'stock_signals.db')
    if not os.path.exists(source_db):
        raise FileNotFoundError(f'池子来源库不存在: {source_db}')
    src = sqlite3.connect(source_db)
    rows = src.execute(
        "SELECT DISTINCT stock_code, insert_date, stock_name FROM stock_signals"
    ).fetchall()
    src.close()
    conn = connect_db()
    before = conn.execute("SELECT COUNT(*) FROM pool_history").fetchone()[0]
    conn.executemany(
        "INSERT OR IGNORE INTO pool_history (stock_code, date, stock_name, source) VALUES (?,?,?,'kdj')",
        rows,
    )
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM pool_history").fetchone()[0]
    conn.close()
    return after - before


# ---------------------------------------------------------------------------
# 股票代码转换（项目格式 sh600000 为内部统一格式）
# ---------------------------------------------------------------------------
def normalize_code(code):
    """'sh.600000'/'1.600000'/'sh600000' -> 'sh600000'。"""
    code = code.strip().lower()
    if '.' in code:
        pre, num = code.split('.')
        pre = {'sh': 'sh', 'sz': 'sz', 'bj': 'bj'}.get(pre, pre)
    elif code[0].isdigit():
        pre = {'1': 'sh', '0': 'sz'}.get(code[0])
        num = code[2:]
    else:
        pre, num = code[:2], code[2:]
    return f"{pre}{num}"


def to_baostock_code(code):
    code = normalize_code(code)
    pre = {'sh': 'sh', 'sz': 'sz', 'bj': 'bj'}.get(code[:2])
    if pre is None:
        raise ValueError(f'不支持的股票代码: {code}')
    return f"{pre}.{code[2:]}"


def to_eastmoney_secid(code):
    code = normalize_code(code)
    prefix = {'sh': '1', 'sz': '0', 'bj': '0'}.get(code[:2])
    if prefix is None:
        raise ValueError(f'不支持的股票代码: {code}')
    return f"{prefix}.{code[2:]}"


def baostock_login_ok():
    """探测 baostock 登录状态。返回 (ok: bool, msg: str)。

    baostock 对高频抓取会临时拉黑（'黑名单用户，请与管理员联系'），
    批量任务开始前应先探测，避免黑名单期间继续打请求加重封禁。
    """
    import baostock as bs
    try:
        lg = bs.login()
        msg = f"{lg.error_code} {lg.error_msg}"
        if lg.error_code != '0':
            return False, msg
        return True, msg
    except Exception as e:
        return False, str(e)


def is_blacklist_msg(msg):
    return '黑名单' in str(msg or '')


# ---------------------------------------------------------------------------
# baostock 每日请求配额（账号级上限 5 万次/日；跨进程共享计数，文件 + flock）
# ---------------------------------------------------------------------------
class BaostockQuotaExceeded(RuntimeError):
    """当日 baostock 请求配额已用尽（限 gapup 侧配额 = 总上限 - 预留）。"""


def _quota_path():
    os.makedirs(DATA_DIR, exist_ok=True)
    return os.path.join(DATA_DIR, 'baostock_quota.json')


def baostock_quota_limit():
    return int(CONFIG['baostock_daily_limit'])


def baostock_quota_consume(n=1):
    """原子地消耗 n 次配额。返回 True=成功；False=配额不足（调用方应中止）。"""
    import fcntl
    path = _quota_path()
    today = datetime.now().strftime('%Y-%m-%d')
    with open(path + '.lock', 'w') as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            data = {'date': today, 'count': 0}
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        data = json.load(f)
                except Exception:
                    data = {'date': today, 'count': 0}
            if data.get('date') != today:  # 跨天自动归零
                data = {'date': today, 'count': 0}
            if int(data['count']) + n > baostock_quota_limit():
                return False
            data['count'] = int(data['count']) + n
            tmp = path + '.tmp'
            with open(tmp, 'w') as f:
                json.dump(data, f)
            os.replace(tmp, path)
            return True
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


def baostock_quota_remaining():
    import fcntl
    path = _quota_path()
    if not os.path.exists(path):
        return baostock_quota_limit()
    with open(path + '.lock', 'w') as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception:
            return baostock_quota_limit()
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
    today = datetime.now().strftime('%Y-%m-%d')
    if data.get('date') != today:
        return baostock_quota_limit()
    return max(0, baostock_quota_limit() - int(data.get('count', 0)))


# ---------------------------------------------------------------------------
# baostock 拉取（日线 + 5分钟线）
# ---------------------------------------------------------------------------
def fetch_daily_baostock(code, start_date, end_date):
    """前复权日线（含换手/涨幅），返回按日期升序的 DataFrame。"""
    if not baostock_quota_consume():
        raise BaostockQuotaExceeded(
            f'每日请求配额已用尽（每日上限 {baostock_quota_limit()}，'
            f'账号总限 50000，已留富余）')
    from Spiders.spiders.baostock_helper import fetch_kline_data_baostock
    df = fetch_kline_data_baostock(
        code, start_date=start_date, end_date=end_date,
        frequency='d', adjustflag='2', verbose=False,
    )
    if df is None or df.empty:
        return None
    return df


def fetch_minute_baostock(code, start_date, end_date, freq_min=5):
    """前复权 5 分钟线（字段仅 OHLCV+额），返回 bar_start 为起始时刻的 DataFrame。

    baostock 分钟线字段不含 peTTM/turn（实测报错 10004012），故这里单独查询。
    time 字段为 bar 结束时刻（如 20260918150000000），转换为起始时刻。
    """
    import baostock as bs
    import socket
    from Spiders.spiders.baostock_helper import (
        login_baostock, _maybe_relogin_every_n_kline_requests,
        _sleep_inter_request_if_configured, BAOSTOCK_SOCKET_TIMEOUT,
    )
    if not baostock_quota_consume():
        raise BaostockQuotaExceeded(
            f'每日请求配额已用尽（每日上限 {baostock_quota_limit()}，'
            f'账号总限 50000，已留富余）')
    freq = str(freq_min)
    _sleep_inter_request_if_configured()
    _maybe_relogin_every_n_kline_requests()
    login_baostock()

    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(BAOSTOCK_SOCKET_TIMEOUT)
    try:
        rs = bs.query_history_k_data_plus(
            to_baostock_code(code),
            "date,time,open,high,low,close,volume,amount",
            start_date=start_date, end_date=end_date,
            frequency=freq, adjustflag='2',
        )
    finally:
        socket.setdefaulttimeout(old_timeout)

    if rs.error_code != '0':
        raise RuntimeError(f'baostock分钟线查询失败: {rs.error_code} {rs.error_msg}')
    rows = []
    while rs.error_code == '0' and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=rs.fields)
    for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # time: yyyymmddHHMMSSsss（bar结束时刻） -> 起始时刻 HH:MM
    def _bar_start(t):
        try:
            hh, mm = int(t[8:10]), int(t[10:12])
        except (ValueError, IndexError):
            return None
        start = hh * 60 + mm - freq_min
        return f"{start // 60:02d}:{start % 60:02d}"
    df['bar_start'] = df['time'].map(_bar_start)
    df = df.dropna(subset=['bar_start', 'close'])
    df = df[df['volume'] > 0]  # 剔除停牌空bar
    return df[['date', 'bar_start', 'open', 'high', 'low', 'close', 'volume', 'amount']]


# ---------------------------------------------------------------------------
# 东方财富拉取（盘中实时分钟线 + 近期日线）
# ---------------------------------------------------------------------------
def _eastmoney_kline(secid, klt, lmt, beg='0', end='20500101'):
    """东财K线通用请求（带限速/抖动/指数退避重试），返回原始 klines 列表。

    实测：请求过快（间隔<1s 连续数十次）会触发临时限流
    （RemoteDisconnected），退避数十秒后自动恢复。
    """
    import random
    params = {
        'secid': secid,
        'fields1': 'f1,f2,f3,f4,f5,f6',
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
        'klt': klt,
        'fqt': KLINE_API['fqt']['forward'],
        'ut': KLINE_API['ut'],
        'beg': beg,
        'end': end,
        'lmt': str(lmt),
    }
    url = KLINE_API['base_url'] + '?' + '&'.join(f"{k}={v}" for k, v in params.items())
    retries = int(CONFIG['eastmoney_retries'])
    delay = float(CONFIG['eastmoney_delay'])
    jitter = float(CONFIG['eastmoney_jitter'])
    last_err = None
    for attempt in range(retries + 1):
        time.sleep(delay + random.uniform(0, jitter))
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))
            if not data.get('data'):
                return None
            return data['data'].get('klines') or None
        except Exception as e:
            last_err = e
            backoff = min(60, 5 * (2 ** attempt))  # 5s,10s,20s,40s
            time.sleep(backoff)
    raise RuntimeError(f'东财请求重试{retries}次仍失败: {last_err}')


def fetch_minute_window_eastmoney(code, beg_date, end_date, freq_min=5):
    """东财 5 分钟线（beg~end 日期区间，盘中实时，含 end 当日进行中 bar）。

    日期接受 YYYY-MM-DD 或 YYYYMMDD。返回与 baostock 分钟线同构的 DataFrame
    （bar_start 为起始时刻，成交量已换算为股）。限速/重试由 _eastmoney_kline 统一处理。
    f51=日期时间(起始), f52=open, f53=close, f54=high, f55=low, f56=volume(手), f57=amount(元)。
    """
    klt = {1: '1', 5: '5', 15: '15', 30: '30', 60: '60'}[freq_min]
    beg = beg_date.replace('-', '')
    end = end_date.replace('-', '')
    klines = _eastmoney_kline(to_eastmoney_secid(code), klt, lmt=100000, beg=beg, end=end)
    if not klines:
        return None
    recs = []
    for line in klines:
        parts = line.split(',')
        dt = parts[0]  # '2026-09-21 10:30'
        recs.append({
            'date': dt[:10], 'bar_start': dt[11:16],
            'open': float(parts[1]), 'close': float(parts[2]),
            'high': float(parts[3]), 'low': float(parts[4]),
            'volume': float(parts[5]) * 100,  # 手 -> 股
            'amount': float(parts[6]),
        })
    if not recs:
        return None
    df = pd.DataFrame(recs).sort_values(['date', 'bar_start']).reset_index(drop=True)
    return df[(df['date'] >= f"{beg[:4]}-{beg[4:6]}-{beg[6:8]}")
              & (df['date'] <= f"{end[:4]}-{end[4:6]}-{end[6:8]}")]


def fetch_intraday_eastmoney(code, target_date=None, freq_min=5):
    """东财当日分钟线（盘中实时）。返回与 baostock 分钟线同构的 DataFrame。"""
    target = (target_date or datetime.now().strftime('%Y-%m-%d')).replace('-', '')
    df = fetch_minute_window_eastmoney(code, target, target, freq_min=freq_min)
    return df


# ---------------------------------------------------------------------------
# 新浪备用源（东财被临时限流时自动切换；字段口径一致：volume 股 / amount 元）
# ---------------------------------------------------------------------------
def _sina_kline(code, scale, datalen):
    url = ("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
           f"?symbol={normalize_code(code)}&scale={scale}&ma=no&datalen={datalen}")
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'Referer': 'https://finance.sina.com.cn',
    })
    time.sleep(0.3)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode('utf-8'))
    if not data:
        return None
    return data


def fetch_minute_window_sina(code, freq_min=5, datalen=1900):
    """新浪分钟线（最近 datalen 根，盘中实时，bar 时间为起始时刻）。

    注意：datalen 上限实测约 1900（2000 及以上返回 null），1900 根 5 分钟线
    ≈ 39.5 个交易日，足够 20 日量比特征使用。
    """
    data = _sina_kline(code, freq_min, min(int(datalen), 1900))
    if not data:
        return None
    recs = []
    for b in data:
        day = b.get('day', '')
        if len(day) < 16:
            continue
        recs.append({
            'date': day[:10], 'bar_start': day[11:16],
            'open': float(b['open']), 'close': float(b['close']),
            'high': float(b['high']), 'low': float(b['low']),
            'volume': float(b.get('volume') or 0),
            'amount': float(b.get('amount') or 0),
        })
    if not recs:
        return None
    return pd.DataFrame(recs).sort_values(['date', 'bar_start']).reset_index(drop=True)


def fetch_daily_sina(code, datalen=60):
    """新浪日线备用（scale=240），返回与 fetch_recent_daily_eastmoney 同构的 DataFrame。

    注：新浪日线无换手率字段（turn 置 NaN，模型对 NaN 容错）。
    """
    data = _sina_kline(code, 240, datalen)
    if not data:
        return None
    df = pd.DataFrame([{
        'date': b['day'][:10],
        'open': float(b['open']), 'close': float(b['close']),
        'high': float(b['high']), 'low': float(b['low']),
        'preclose': None,
        'volume': float(b.get('volume') or 0),
        'amount': float(b.get('amount') or 0),
        'pct_chg': None, 'turn': None,
    } for b in data])
    df['preclose'] = df['close'].shift(1)
    return df


_EM_CONSECUTIVE_FAILURES = 0  # 东财连续失败计数（熔断用：连续3次失败后本次运行直接走新浪）


def fetch_minute_window(code, beg_date, end_date, freq_min=5):
    """统一分钟线入口：东财优先，失败自动切新浪（连续失败3次熔断直达新浪）。返回 (df|None, source名)。"""
    global _EM_CONSECUTIVE_FAILURES
    logger = get_gapup_logger()
    try:
        if _EM_CONSECUTIVE_FAILURES >= 3:
            raise RuntimeError('东财熔断中（连续失败已达3次）')
        df = fetch_minute_window_eastmoney(code, beg_date, end_date, freq_min=freq_min)
        _EM_CONSECUTIVE_FAILURES = 0
        return df, 'eastmoney'
    except Exception as e:
        _EM_CONSECUTIVE_FAILURES += 1
        logger.warning(f"{code}: 东财分钟线失败（{e}），切换新浪备用源")
        try:
            df = fetch_minute_window_sina(code, freq_min=freq_min, datalen=1900)
            if df is not None and beg_date:
                b = beg_date.replace('-', '')
                cutoff_date = f"{b[:4]}-{b[4:6]}-{b[6:8]}"
                df = df[df['date'] >= cutoff_date]
            return df, 'sina'
        except Exception as e2:
            logger.warning(f"{code}: 新浪备用源也失败: {e2}")
            return None, 'none'


def fetch_recent_daily_eastmoney(code, days=60):
    """东财近 N 根前复权日线（盘中实时，含今日进行中bar）。

    f51=date, f52=open, f53=close, f54=high, f55=low, f56=volume(手),
    f57=amount, f59=涨跌幅%, f60=涨跌额, f61=换手率。
    返回列: date/open/high/low/close/preclose/volume(股)/amount/turn/pct_chg。
    """
    klines = _eastmoney_kline(to_eastmoney_secid(code), KLINE_API['klt']['daily'], lmt=days)
    if not klines:
        return None
    recs = []
    for line in klines:
        p = line.split(',')
        recs.append({
            'date': p[0],
            'open': float(p[1]), 'close': float(p[2]),
            'high': float(p[3]), 'low': float(p[4]),
            'preclose': float(p[2]) - float(p[9]),  # close - 涨跌额
            'volume': float(p[5]) * 100,            # 手 -> 股
            'amount': float(p[6]),
            'pct_chg': float(p[8]),
            'turn': float(p[10]),
        })
    df = pd.DataFrame(recs)
    # 涨跌额是相对"真实昨收"（未复权基准），复权价下可能引入小误差；
    # 再用相邻close校正 preclose，避免除权日异常
    df['preclose'] = df['close'].shift(1).fillna(df['preclose'])
    return df


# ---------------------------------------------------------------------------
# 时间口径工具
# ---------------------------------------------------------------------------
def hhmm_to_minutes(hhmm):
    h, m = hhmm.split(':')
    return int(h) * 60 + int(m)


def bars_closed_by(df, cutoff=None, freq_min=None):
    """过滤出 cutoff 时刻已收盘的 bar（bar_start + freq <= cutoff）。"""
    cutoff = cutoff or CONFIG['cutoff']
    freq_min = freq_min or CONFIG['minute_freq_min']
    cut = hhmm_to_minutes(cutoff)
    keep = df['bar_start'].map(lambda t: hhmm_to_minutes(t) + freq_min <= cut)
    return df[keep]
