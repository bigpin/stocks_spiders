#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单测试 baostock 是否可用：
- 登录
- 查询一小段日K数据
- 打印前几行
"""

import baostock as bs
import sys as _sys, os as _os
_p = _os.path.dirname(_os.path.abspath(__file__))
while _p and _p != _os.path.dirname(_p) and not _os.path.isdir(_os.path.join(_p, 'Spiders')):
    _p = _os.path.dirname(_p)
if _p and _os.path.isdir(_os.path.join(_p, 'Spiders')) and _p not in _sys.path:
    _sys.path.insert(0, _p)
from Spiders.common.log import get_logger
logger = get_logger(__name__)
import pandas as pd


def main():
    logger.info("=== Baostock 可用性测试 ===")

    # 1. 登录
    lg = bs.login()
    logger.info("login:", lg.error_code, lg.error_msg)
    if lg.error_code != "0":
        logger.info("登录失败，测试结束。")
        return

    try:
        # 2. 查询一只股票的日K数据（五粮液 sz.000858）
        logger.info("开始查询 sz.000858 日K 数据 ...")
        rs = bs.query_history_k_data_plus(
            "sz.000858",
            "date,code,open,high,low,close,volume,amount,pctChg",
            start_date="2024-01-02",
            end_date="2024-01-10",
            frequency="d",
            adjustflag="2",  # 前复权
        )

        logger.info("query:", rs.error_code, rs.error_msg)
        if rs.error_code != "0":
            logger.info("查询失败，测试结束。")
            return

        # 3. 读取结果
        data_list = []
        while (rs.error_code == "0") and rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            logger.info("没有返回任何K线数据。")
            return

        df = pd.DataFrame(data_list, columns=rs.fields)

        logger.info("\n返回数据基本信息：")
        logger.info("行数:", len(df), "列数:", len(df.columns))
        logger.info("列名:", list(df.columns))

        logger.info("\n完整数据：")
        print(df.to_string(index=False))
    finally:
        # 4. 登出
        bs.logout()
        logger.info("\n已登出 baostock。")


if __name__ == "__main__":
    main()

