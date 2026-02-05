#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简单测试 baostock 是否可用：
- 登录
- 查询一小段日K数据
- 打印前几行
"""

import baostock as bs
import pandas as pd


def main():
    print("=== Baostock 可用性测试 ===")

    # 1. 登录
    lg = bs.login()
    print("login:", lg.error_code, lg.error_msg)
    if lg.error_code != "0":
        print("登录失败，测试结束。")
        return

    try:
        # 2. 查询一只股票的日K数据（五粮液 sz.000858）
        print("开始查询 sz.000858 日K 数据 ...")
        rs = bs.query_history_k_data_plus(
            "sz.000858",
            "date,code,open,high,low,close,volume,amount,pctChg",
            start_date="2024-01-02",
            end_date="2024-01-10",
            frequency="d",
            adjustflag="2",  # 前复权
        )

        print("query:", rs.error_code, rs.error_msg)
        if rs.error_code != "0":
            print("查询失败，测试结束。")
            return

        # 3. 读取结果
        data_list = []
        while (rs.error_code == "0") and rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            print("没有返回任何K线数据。")
            return

        df = pd.DataFrame(data_list, columns=rs.fields)

        print("\n返回数据基本信息：")
        print("行数:", len(df), "列数:", len(df.columns))
        print("列名:", list(df.columns))

        print("\n完整数据：")
        print(df.to_string(index=False))
    finally:
        # 4. 登出
        bs.logout()
        print("\n已登出 baostock。")


if __name__ == "__main__":
    main()

