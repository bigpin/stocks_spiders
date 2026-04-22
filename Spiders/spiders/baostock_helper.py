#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
baostock 数据获取辅助函数
用于替代东方财富API，提供更稳定的股票数据获取
"""

import os
import baostock as bs
import pandas as pd
from datetime import datetime
import time

from .stock_config import BAOSTOCK_RELOGIN_EVERY_N_REQUESTS


# 模块级登录状态，整个进程内只登录一次
_BAOSTOCK_LOGGED_IN = False
# 当前进程内已执行的 K 线 history 请求次数（用于周期性重登）
_BAOSTOCK_KLINE_REQUEST_COUNT = 0


def _force_relogin_baostock():
    """logout 后重新 login，用于长连接被服务端掐断前的主动换会话。"""
    global _BAOSTOCK_LOGGED_IN
    _BAOSTOCK_LOGGED_IN = False
    try:
        bs.logout()
    except Exception:
        pass
    login_baostock()


def _maybe_relogin_every_n_kline_requests():
    """每 N 次 K 线拉取（query_history_k_data_plus）在本进程内强制重登一次。"""
    global _BAOSTOCK_KLINE_REQUEST_COUNT
    n = int(BAOSTOCK_RELOGIN_EVERY_N_REQUESTS or 0)
    if n <= 0:
        return
    _BAOSTOCK_KLINE_REQUEST_COUNT += 1
    if _BAOSTOCK_KLINE_REQUEST_COUNT % n == 0:
        _force_relogin_baostock()


def parse_stock_list_line(line):
    """解析 stock_list.txt 单行：兼容仅代码，或「代码\\t名称」（列表更新时写入）。"""
    if line is None:
        return None, None
    line = line.strip()
    if not line or line.startswith("#"):
        return None, None
    if "\t" in line:
        code, _, rest = line.partition("\t")
        code = code.strip()
        if not code:
            return None, None
        name = rest.strip()
        return code, (name or None)
    return line, None


def read_stock_list_txt(path):
    """
    读取 stock_list.txt。

    Returns:
        tuple[list[str], dict[str, str]]: (代码顺序列表, 代码->文件中的简称)
    """
    if not os.path.exists(path):
        return [], {}
    codes = []
    list_name_by_code = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            code, list_name = parse_stock_list_line(raw)
            if not code:
                continue
            codes.append(code)
            if list_name:
                list_name_by_code[code] = list_name
    return codes, list_name_by_code


def convert_stock_code_to_baostock(stock_code):
    """
    将股票代码转换为baostock格式
    例如：sh603288 -> sh.603288, sz000858 -> sz.000858, 920978 -> bj.920978
    """
    if len(stock_code) >= 2:
        prefix = stock_code[:2]
        code = stock_code[2:]
        
        # 映射前缀
        prefix_map = {
            'sh': 'sh',
            'sz': 'sz',
            '92': 'bj'  # 北交所
        }
        
        baostock_prefix = prefix_map.get(prefix, prefix)
        return f"{baostock_prefix}.{code}"
    
    return stock_code


def login_baostock():
    """登录baostock（全局只登录一次）"""
    global _BAOSTOCK_LOGGED_IN
    if _BAOSTOCK_LOGGED_IN:
        return True

    lg = bs.login()
    if lg.error_code != '0':
        raise Exception(f"baostock登录失败: {lg.error_msg}")
    _BAOSTOCK_LOGGED_IN = True
    return True


def logout_baostock():
    """登出baostock（如需要可在程序结束时手动调用）"""
    global _BAOSTOCK_LOGGED_IN
    if _BAOSTOCK_LOGGED_IN:
        bs.logout()
        _BAOSTOCK_LOGGED_IN = False


def _get_trade_days_baostock(before_date=None, back_days=30):
    """
    用 query_trade_dates 获取「最近若干个交易日」，从新到旧排序。
    before_date: 不晚于该日期，默认今天；格式 YYYY-MM-DD 或 datetime
    back_days: 向前查询的日历天数
    返回: list['YYYY-MM-DD']，空列表表示失败
    """
    from datetime import timedelta
    login_baostock()
    if before_date is None:
        end = datetime.now()
    elif isinstance(before_date, str):
        end = datetime.strptime(before_date[:10], "%Y-%m-%d")
    else:
        end = before_date
    start = end - timedelta(days=max(1, back_days))
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    rs = bs.query_trade_dates(start_date=start_str, end_date=end_str)
    if rs.error_code != "0":
        return []
    trading_dates = []
    while rs.next():
        row = rs.get_row_data()
        if len(row) >= 2 and row[1] == "1":
            trading_dates.append(row[0])
    trading_dates.sort(reverse=True)  # 从新到旧
    return trading_dates


def _rows_to_stock_list_entries(rows, a_share_only):
    """将 query_all_stock 行转为 (扁平代码, 简称)；简称来自接口 code_name 列。"""
    entries = []
    for row in rows:
        code = row[0]
        code_flat = code.replace(".", "")
        code_name = (row[2] or "").strip() if len(row) > 2 else ""
        if not a_share_only:
            entries.append((code_flat, code_name))
            continue
        if "." not in code:
            continue
        market, num = code.split(".", 1)
        if market == "sh" and (num.startswith("60") or num.startswith("68")):
            entries.append((code_flat, code_name))
        elif market == "sz" and (num.startswith("00") or num.startswith("30")):
            entries.append((code_flat, code_name))
        elif market == "bj" and (num[0] in ("4", "8")):
            entries.append((code_flat, code_name))
    return entries


def get_stock_list_baostock_entries(day=None, a_share_only=True, try_days=10):
    """
    使用 baostock query_all_stock 获取股票列表，带证券简称（接口字段 code_name）。

    参数:
        day: 交易日，格式 'YYYY-MM-DD' 或 'YYYYMMDD'；为空则用 query_trade_dates 取最近交易日
        a_share_only: 是否只保留 A 股（默认 True，过滤掉指数、B 股等）
        try_days: 当 day 为空且 query_trade_dates 不可用时，向前尝试的日历天数

    返回:
        list[tuple[str, str]]: (sh600000 形式代码, 简称)，简称可能为空字符串
    """
    from datetime import timedelta

    login_baostock()

    def query_one_day(day_str):
        if len(day_str) == 8:
            day_str = f"{day_str[:4]}-{day_str[4:6]}-{day_str[6:8]}"
        rs = bs.query_all_stock(day=day_str)
        if rs.error_code != "0":
            raise RuntimeError(f"baostock query_all_stock 失败: {rs.error_msg}")
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        return rows, day_str

    if day:
        day_str = day if isinstance(day, str) else day.strftime("%Y-%m-%d")
        rows, _ = query_one_day(day_str)
        return _rows_to_stock_list_entries(rows, a_share_only) if rows else []

    for day_str in _get_trade_days_baostock():
        rows, _ = query_one_day(day_str)
        if rows:
            return _rows_to_stock_list_entries(rows, a_share_only)

    for i in range(try_days):
        d = datetime.now() - timedelta(days=i)
        day_str = d.strftime("%Y-%m-%d")
        rows, _ = query_one_day(day_str)
        if rows:
            return _rows_to_stock_list_entries(rows, a_share_only)
    return []


def get_stock_list_baostock(day=None, a_share_only=True, try_days=10):
    """
    使用 baostock 获取股票列表（对应 query_all_stock）。

    参数:
        day: 交易日，格式 'YYYY-MM-DD' 或 'YYYYMMDD'；为空则用 query_trade_dates 取最近交易日
        a_share_only: 是否只保留 A 股（默认 True，过滤掉指数、B 股等）
        try_days: 当 day 为空且 query_trade_dates 不可用时，向前尝试的日历天数

    返回:
        list[str]: 股票代码列表，格式与项目一致，如 ['sh600000', 'sz000001']
    """
    return [c for c, _ in get_stock_list_baostock_entries(day, a_share_only, try_days)]


def fetch_kline_data_baostock(stock_code, start_date=None, end_date=None, 
                               frequency='d', adjustflag='3', verbose=False):
    """
    使用baostock获取K线数据
    
    参数:
        stock_code: 股票代码，如 'sh603288', 'sz000858', '920978'
        start_date: 开始日期，格式 'YYYY-MM-DD' 或 'YYYYMMDD'
        end_date: 结束日期，格式 'YYYY-MM-DD' 或 'YYYYMMDD'
        frequency: 数据类型，默认为'd'（日K线）
                  'd'=日K线, 'w'=周K线, 'm'=月K线, '5'=5分钟K线, '15'=15分钟K线, 
                  '30'=30分钟K线, '60'=60分钟K线
        adjustflag: 复权类型，默认为'3'（前复权）
                   '1'=后复权, '2'=前复权, '3'=不复权
        verbose: 是否输出详细信息
    
    返回:
        pandas.DataFrame: K线数据，包含 date, open, high, low, close, volume, amount 等列
    """
    try:
        # 转换股票代码格式
        bs_code = convert_stock_code_to_baostock(stock_code)
        
        # 格式化日期
        if start_date:
            if len(start_date) == 8:  # YYYYMMDD格式
                start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        else:
            # 默认获取最近一年的数据
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
            start_dt = end_dt.replace(year=end_dt.year - 1)
            start_date = start_dt.strftime("%Y-%m-%d")
        
        if end_date:
            if len(end_date) == 8:  # YYYYMMDD格式
                end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
        else:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        if verbose:
            print(f"    使用baostock获取数据: {bs_code}, {start_date} 到 {end_date}")
        
        _maybe_relogin_every_n_kline_requests()
        # 确保已登录（全局只登录一次；上面周期性重登时已登录则此处为 no-op）
        login_baostock()
        
        # 查询K线数据（含估值字段 peTTM/pbMRQ，省去单独的估值抓取阶段）
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST,peTTM,pbMRQ",
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjustflag
        )
        
        if rs.error_code != '0':
            error_msg = rs.error_msg
            if verbose:
                print(f"    baostock查询失败: {error_msg}")
            return None
        
        # 转换为DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            if verbose:
                print(f"    警告: 未获取到数据")
            return None
        
        # 创建DataFrame
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 数据类型转换
        numeric_columns = ['open', 'high', 'low', 'close', 'preclose', 'volume', 
                          'amount', 'turn', 'pctChg', 'peTTM', 'pbMRQ']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 设置日期索引
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)
        
        # 重命名列以匹配现有代码
        column_mapping = {
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume',
            'amount': 'amount',
            'pctChg': 'change_rate',  # 涨跌幅
            'turn': 'turnover',  # 换手率
            'tradestatus': 'trade_status',
            'isST': 'is_st',
            'peTTM': 'peTTM',
            'pbMRQ': 'pbMRQ',
        }
        
        # 只保留需要的列
        available_columns = [col for col in column_mapping.keys() if col in df.columns]
        df = df[available_columns]
        
        # 重命名列
        df.rename(columns=column_mapping, inplace=True)
        
        # 计算涨跌额（如果close和preclose都存在）
        if 'preclose' in df.columns and 'close' in df.columns:
            df['change_amount'] = df['close'] - df['preclose']
        
        # 计算振幅（如果high和low都存在）
        if 'high' in df.columns and 'low' in df.columns and 'preclose' in df.columns:
            df['amplitude'] = ((df['high'] - df['low']) / df['preclose'] * 100).round(2)
        
        if verbose:
            print(f"    获取到 {len(df)} 条K线数据")
        
        return df
        
    except Exception as e:
        if verbose:
            import traceback
            print(f"    获取K线数据失败: {str(e)}")
            print(f"    错误堆栈: {traceback.format_exc()}")
        return None


def fetch_kline_data_baostock_simple(stock_code, start_date=None, end_date=None, verbose=False):
    """
    简化版baostock数据获取函数，使用默认参数（日K线，前复权）
    
    参数:
        stock_code: 股票代码
        start_date: 开始日期，格式 'YYYY-MM-DD' 或 'YYYYMMDD'
        end_date: 结束日期，格式 'YYYY-MM-DD' 或 'YYYYMMDD'
        verbose: 是否输出详细信息
    
    返回:
        pandas.DataFrame: K线数据
    """
    return fetch_kline_data_baostock(
        stock_code=stock_code,
        start_date=start_date,
        end_date=end_date,
        frequency='d',  # 日K线
        adjustflag='2',  # 前复权
        verbose=verbose
    )


def get_stock_name_baostock(stock_code):
    """
    使用baostock获取股票名称
    
    参数:
        stock_code: 股票代码，如 'sh603288', 'sz000858', '920978'
    
    返回:
        str: 股票名称，如果获取失败返回 None
    """
    try:
        bs_code = convert_stock_code_to_baostock(stock_code)
        # 确保已登录（全局只登录一次）
        login_baostock()
        
        rs = bs.query_stock_basic(code=bs_code)
        if rs.error_code != '0':
            return None
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if data_list and len(data_list) > 0:
            # baostock返回的字段：code, code_name, ipoDate, outDate, type, status
            return data_list[0][1] if len(data_list[0]) > 1 else None
        
        return None
    except Exception:
        return None


def fetch_stock_fundamental_worker(stock_code, latest_date=None):
    """
    Worker for ProcessPoolExecutor: fetch latest K-line price + PE (via epsTTM) for one stock.
    Returns a dict with all stock_detail fields, or None on failure.
    Safe to use in spawned subprocesses (each process logs in independently).
    """
    import math
    from datetime import timedelta

    def safe_float(v, default=None):
        if v is None or str(v).strip() == '':
            return default
        try:
            f = float(v)
            return default if math.isnan(f) else f
        except (TypeError, ValueError):
            return default

    try:
        login_baostock()
        bs_code = convert_stock_code_to_baostock(stock_code)

        # Resolve end_date
        if latest_date is None:
            end_dt = datetime.now()
        elif len(str(latest_date)) == 8 and str(latest_date).isdigit():
            end_dt = datetime.strptime(str(latest_date), "%Y%m%d")
        else:
            end_dt = datetime.strptime(str(latest_date)[:10], "%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")
        start_date = (end_dt - timedelta(days=10)).strftime("%Y-%m-%d")

        # --- K-line: get the most recent trading day's data ---
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,preclose,volume,amount,turn,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency='d',
            adjustflag='3',
        )
        k_fields = rs.fields
        krows = []
        while rs.next():
            krows.append(dict(zip(k_fields, rs.get_row_data())))

        if not krows:
            return None

        last = krows[-1]
        close = safe_float(last.get('close'))
        if not close or close <= 0:
            return None

        preclose = safe_float(last.get('preclose')) or close
        price_change = round(close - preclose, 4)
        pct_chg    = safe_float(last.get('pctChg'))
        volume     = safe_float(last.get('volume'))
        amount     = safe_float(last.get('amount'))
        turn       = safe_float(last.get('turn'))
        open_      = safe_float(last.get('open'))
        high       = safe_float(last.get('high'))
        low        = safe_float(last.get('low'))

        # --- PE = close / epsTTM from quarterly profit data ---
        pe = None
        report_year = end_dt.year
        for yr, qt in [
            (report_year,     4),
            (report_year,     3),
            (report_year,     2),
            (report_year,     1),
            (report_year - 1, 4),
            (report_year - 1, 3),
            (report_year - 1, 2),
            (report_year - 1, 1),
        ]:
            rs_p = bs.query_profit_data(code=bs_code, year=yr, quarter=qt)
            p_fields = rs_p.fields
            prows = []
            while rs_p.next():
                prows.append(dict(zip(p_fields, rs_p.get_row_data())))
            if prows:
                eps_ttm = safe_float(prows[0].get('epsTTM'))
                if eps_ttm and eps_ttm > 0:
                    pe = round(close / eps_ttm, 2)
                break  # found the latest quarter, stop regardless of eps sign

        # --- Stock name ---
        name = get_stock_name_baostock(stock_code) or stock_code

        return {
            'stock_id':          stock_code,
            'stock_name':        name,
            'new_price':         close,
            'percentage_change': pct_chg,
            'price_change':      price_change,
            'trading_volume':    round(volume / 100, 2) if volume else None,   # 股→手
            'trading_value':     round(amount, 4) if amount else None,          # 元
            'highest_price':     high,
            'lowest_price':      low,
            'opening_price':     open_,
            'closing_price':     preclose,
            'turnover_rate':     turn,
            'pe':                pe,
            'pb':                None,
        }
    except Exception:
        return None


def fetch_one_baostock_worker(stock_code, start_date, end_date, max_retries=3, list_name=None):
    """
    供多进程调用的 worker：在独立进程中拉取单只股票 K 线 + 名称，避免 baostock SDK 线程安全问题。
    遇到 BrokenPipeError / 连接异常时自动重试（重新登录后再请求）。
    返回 (stock_code, stock_name, df)，df 为 None 表示拉取失败。
    list_name: stock_list.txt 中的简称，在 query_stock_basic 失败时作为兜底。
    """
    global _BAOSTOCK_LOGGED_IN

    for attempt in range(1, max_retries + 1):
        try:
            login_baostock()
            df = fetch_kline_data_baostock_simple(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                verbose=False,
            )
            if df is None or df.empty:
                return (stock_code, None, None)
            name = get_stock_name_baostock(stock_code) or (list_name if list_name else None) or stock_code
            return (stock_code, name, df)
        except (BrokenPipeError, ConnectionError, OSError) as e:
            if attempt < max_retries:
                _BAOSTOCK_LOGGED_IN = False
                try:
                    bs.logout()
                except Exception:
                    pass
                time.sleep(1 * attempt)
            else:
                return (stock_code, None, None)
        except Exception:
            return (stock_code, None, None)


def fetch_and_compute_one_baostock_worker(
    stock_code,
    start_date,
    end_date,
    indicators_config,
    signal_filters,
    current_time,
    max_retries=3,
    list_name=None,
):
    """
    并行流水线 worker：在同一个子进程中完成
      1) 拉取单只股票 K 线（baostock）
      2) 计算技术指标 + 信号分析（CPU）
    主进程只负责 I/O（写文件/SQLite），避免“先全量拉取再统一计算”的峰值与内存压力。

    返回：compute_signals_for_stock 的 dict（包含 df/heat_score/kdj_analysis 等）。
    """
    from .signal_compute_worker import compute_signals_for_stock

    code, name, df = fetch_one_baostock_worker(
        stock_code=stock_code,
        start_date=start_date,
        end_date=end_date,
        max_retries=max_retries,
        list_name=list_name,
    )

    if df is None or getattr(df, "empty", True):
        return {
            'stock_code': stock_code,
            'stock_name': name or stock_code,
            'skip': True,
            'reason': 'K线拉取失败',
        }

    return compute_signals_for_stock(
        stock_code=code,
        stock_name=name or code,
        df=df,
        indicators_config=indicators_config,
        signal_filters=signal_filters,
        current_time=current_time,
    )
