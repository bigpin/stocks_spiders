#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
baostock 数据获取辅助函数
用于替代东方财富API，提供更稳定的股票数据获取
"""

import baostock as bs
import pandas as pd
from datetime import datetime
import time


# 模块级登录状态，整个进程内只登录一次
_BAOSTOCK_LOGGED_IN = False


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
        
        # 确保已登录（全局只登录一次）
        login_baostock()
        
        # 查询K线数据
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
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
                          'amount', 'turn', 'pctChg']
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
            'turn': 'turnover'  # 换手率
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
