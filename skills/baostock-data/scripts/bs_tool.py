#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
baostock 通用查询 CLI（baostock-data skill 附带工具）

覆盖：K线(d/w/m/5/15/30/60 + 复权)、交易日历、全量证券列表、
证券基本资料、行业、复权因子、季频财务六表、业绩快报/预告、宏观利率。

内置保护（实测经验，详见同目录 SKILL.md）：
- 登录即探测黑名单（10001011 黑名单用户 → 退出码 3，快速失败）
- 每日请求配额：文件锁跨进程计数，默认自限 4 万（BAOSTOCK_DAILY_LIMIT 可调；
  账号总上限 5 万/日，超限会被拉黑数小时~1天）
- socket 超时 120s，防服务端不响应时无限阻塞

推荐解释器：/Users/dingli/anaconda3/bin/python（已装 baostock 0.9.10 + pandas）
"""
import argparse
import fcntl
import json
import os
import socket
import sys
from datetime import datetime, timedelta

SOCKET_TIMEOUT = 120
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
QUOTA_FILE = os.environ.get('BAOSTOCK_QUOTA_FILE') or os.path.join(
    SCRIPT_DIR, '.baostock_quota.json')
DAILY_LIMIT = int(os.environ.get('BAOSTOCK_DAILY_LIMIT', '40000'))

# 日线含估值/换手/ST 等全字段；分钟线仅 OHLCV+amount（baostock 限制）
DAILY_FIELDS = ("date,code,open,high,low,close,preclose,volume,amount,adjustflag,"
                "turn,tradestatus,pctChg,isST,peTTM,pbMRQ,psTTM,pcfNcfTTM")
MINUTE_FIELDS = "date,time,code,open,high,low,close,volume,amount,adjustflag"
MINUTE_FREQS = ('5', '15', '30', '60')

FINANCE_FNS = {
    'profit': 'query_profit_data',
    'operation': 'query_operation_data',
    'growth': 'query_growth_data',
    'balance': 'query_balance_data',
    'cashflow': 'query_cash_flow_data',
    'dupont': 'query_dupont_data',
}
REPORT_FNS = {
    'express': 'query_performance_express_report',
    'forecast': 'query_forecast_report',  # 业绩预告（注意：不叫 query_performance_forecast_report）
}
INDEX_STOCKS_FNS = {
    'hs300': 'query_hs300_stocks',
    'sz50': 'query_sz50_stocks',
    'zz500': 'query_zz500_stocks',
}

_LOGGED = [False]


def _die(msg, code=2):
    print(f"[bs_tool] {msg}", file=sys.stderr)
    sys.exit(code)


def _today():
    return datetime.now().strftime('%Y-%m-%d')


def _days_ago(n):
    return (datetime.now() - timedelta(days=n)).strftime('%Y-%m-%d')


def _padded(d):
    """'20261015' → '2026-10-15'，其余原样。"""
    d = str(d)
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return d


def to_bs_code(code):
    """sh.600000 / sh600000 / 600000 → sh.600000；4/8/92 开头归北交所 bj。"""
    code = str(code).strip().lower()
    if '.' in code:
        return code
    if len(code) == 8 and code[:2] in ('sh', 'sz', 'bj'):
        return f"{code[:2]}.{code[2:]}"
    if code.isdigit() and len(code) == 6:
        if code.startswith(('60', '68')):
            return f"sh.{code}"
        if code.startswith(('00', '30')):
            return f"sz.{code}"
        if code.startswith('92') or code[0] in '48':
            return f"bj.{code}"
    _die(f"无法识别的证券代码: {code}（支持 sh.600000 / sh600000 / 600000 形式）")


def login():
    """登录（进程内单例）；黑名单立即退出码 3，避免加重封禁。"""
    import baostock as bs
    if _LOGGED[0]:
        return bs
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    try:
        lg = bs.login()
    except Exception as e:
        _die(f"baostock 登录异常: {e}")
        return None
    finally:
        socket.setdefaulttimeout(old)
    if lg.error_code != '0':
        if '黑名单' in str(lg.error_msg or ''):
            _die(f"baostock 黑名单中（{lg.error_code} {lg.error_msg}）。"
                 "立即停止请求，一般数小时~1天自动解除。", code=3)
        _die(f"baostock 登录失败: {lg.error_code} {lg.error_msg}")
    _LOGGED[0] = True
    return bs


def logout():
    if not _LOGGED[0]:
        return
    try:
        import baostock as bs
        bs.logout()
    except Exception:
        pass
    _LOGGED[0] = False


# ---------------- 每日配额（文件 + flock，跨进程共享） ----------------
def quota_status():
    try:
        with open(QUOTA_FILE) as f:
            data = json.load(f)
    except Exception:
        return 0
    if data.get('date') != _today():
        return 0
    return int(data.get('count', 0))


def quota_consume(n=1):
    """原子消耗 n 次配额；不足则退出。"""
    d = os.path.dirname(QUOTA_FILE)
    if d:
        os.makedirs(d, exist_ok=True)
    today = _today()
    with open(QUOTA_FILE + '.lock', 'w') as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            used = quota_status()
            if used + n > DAILY_LIMIT:
                _die(f"当日配额已用尽（{used}/{DAILY_LIMIT}）：{QUOTA_FILE}。"
                     "账号总上限 5 万/日，继续会触发拉黑，明日再跑。")
            tmp = QUOTA_FILE + '.tmp'
            with open(tmp, 'w') as f:
                json.dump({'date': today, 'count': used + n}, f)
            os.replace(tmp, QUOTA_FILE)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


# ---------------- 查询与输出 ----------------
def fetch_result(rs):
    """ResultData → (DataFrame|None, err|None)。黑名单直接退出码 3。"""
    if '黑名单' in str(getattr(rs, 'error_msg', '') or ''):
        _die(f"查询被拒：黑名单（{rs.error_code} {rs.error_msg}）", code=3)
    if rs.error_code != '0':
        return None, f"{rs.error_code} {rs.error_msg}"
    import pandas as pd
    rows, fields = [], rs.fields
    while rs.next():
        rows.append(rs.get_row_data())
    return pd.DataFrame(rows, columns=fields), None


def run_query(rs):
    df, err = fetch_result(rs)
    if err:
        _die(f"查询失败: {err}")
    return df


def coerce_numeric(df):
    """字符串列转数值；转换后全空的列保留原值（日期/代码列不受影响）。"""
    import pandas as pd
    for col in df.columns:
        conv = pd.to_numeric(df[col], errors='coerce')
        if conv.notna().any():
            df[col] = conv
    return df


def emit(df, out=None, max_head=30, max_tail=10):
    df = coerce_numeric(df)
    if df is None or len(df) == 0:
        print("(无数据)")
        return
    if out:
        df.to_csv(out, index=False, encoding='utf-8-sig')
        print(f"共 {len(df)} 行 → {out}")
        return
    import pandas as pd
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 240)
    if len(df) > max_head + max_tail:
        print(df.head(max_head))
        print(f"... 中间省略 {len(df) - max_head - max_tail} 行 ...")
        print(df.tail(max_tail))
    else:
        print(df)
    print(f"共 {len(df)} 行")


# ---------------- 子命令 ----------------
def cmd_check(args):
    login()
    used = quota_status()
    print("登录 OK（黑名单探测通过）")
    print(f"本工具当日配额: {used}/{DAILY_LIMIT}，余 {DAILY_LIMIT - used}（账号总上限 5 万/日）")
    print(f"配额文件: {QUOTA_FILE}")


def cmd_quota(args):
    used = quota_status()
    print(f"{used}/{DAILY_LIMIT}（余 {DAILY_LIMIT - used}）  配额文件: {QUOTA_FILE}")


def cmd_kline(args):
    bs = login()
    quota_consume()
    fields = args.fields or (MINUTE_FIELDS if args.freq in MINUTE_FREQS else DAILY_FIELDS)
    end = _padded(args.end or _today())
    start = _padded(args.start or _days_ago(30))
    rs = bs.query_history_k_data_plus(
        to_bs_code(args.code), fields,
        start_date=start, end_date=end,
        frequency=args.freq, adjustflag=str(args.adjust))
    emit(run_query(rs), args.out)


def cmd_trades(args):
    bs = login()
    quota_consume()
    end = _padded(args.end or _today())
    start = _padded(args.start or _days_ago(90))
    df = run_query(bs.query_trade_dates(start_date=start, end_date=end))
    if args.trading_only and 'is_trading_day' in df.columns:
        df = df[df['is_trading_day'] == '1']
    emit(df, args.out)


def _is_a_share(code):
    market, _, num = str(code).partition('.')
    if market == 'sh':
        return num[:2] in ('60', '68')
    if market == 'sz':
        return num[:2] in ('00', '30')
    if market == 'bj':
        return num.startswith('92') or num[:1] in ('4', '8')
    return False


def _recent_trading_days(bs, n=10):
    """最近 n 个交易日（新→旧）；交易日历失败回退近 n 天日历日。"""
    df, _ = fetch_result(bs.query_trade_dates(start_date=_days_ago(40), end_date=_today()))
    days = []
    if df is not None and not df.empty and 'is_trading_day' in df.columns:
        days = df[df['is_trading_day'] == '1']['calendar_date'].tolist()
        days.sort(reverse=True)
    return (days + [_days_ago(i) for i in range(n)])[:n]


def cmd_allstock(args):
    bs = login()
    candidates = [_padded(args.day)] if args.day else _recent_trading_days(bs)
    df = None
    for d in candidates:
        quota_consume()
        got, _ = fetch_result(bs.query_all_stock(day=d))
        if got is not None and not got.empty:
            df = got
            print(f"[列表日期: {d}]", file=sys.stderr)
            break
    if df is None:
        _die("近 10 日均未取到证券列表")
    if args.a_share and 'code' in df.columns:
        df = df[df['code'].map(_is_a_share)]
    emit(df, args.out)


def cmd_basic(args):
    bs = login()
    quota_consume()
    df = run_query(bs.query_stock_basic(code=to_bs_code(args.code),
                                        code_name=args.name or ''))
    emit(df, args.out)


def cmd_industry(args):
    bs = login()
    quota_consume()
    code = to_bs_code(args.code) if args.code else ''
    df = run_query(bs.query_stock_industry(code=code))
    emit(df, args.out)


def cmd_adjust(args):
    bs = login()
    quota_consume()
    end = _padded(args.end or _today())
    start = _padded(args.start or _days_ago(365 * 5))
    df = run_query(bs.query_adjust_factor(code=to_bs_code(args.code),
                                          start_date=start, end_date=end))
    emit(df, args.out)


def _latest_quarter():
    """推算最近大概率已披露的季报（年报4月底/一季报4月底/中报8月底/三季报10月底）。"""
    now = datetime.now()
    if now.month <= 4:
        return now.year - 1, 3
    if now.month <= 8:
        return now.year, 1
    if now.month <= 10:
        return now.year, 2
    return now.year, 3


def cmd_finance(args):
    bs = login()
    quota_consume()
    year, quarter = args.year, args.quarter
    if not year or not quarter:
        dy, dq = _latest_quarter()
        year, quarter = year or dy, quarter or dq
        print(f"[未指定报告期，使用 {year}Q{quarter}]", file=sys.stderr)
    df = run_query(getattr(bs, FINANCE_FNS[args.type])(
        code=to_bs_code(args.code), year=int(year), quarter=int(quarter)))
    emit(df, args.out)


def cmd_report(args):
    bs = login()
    quota_consume()
    end = _padded(args.end or _today())
    start = _padded(args.start or _days_ago(180))
    df = run_query(getattr(bs, REPORT_FNS[args.type])(
        code=to_bs_code(args.code), start_date=start, end_date=end))
    emit(df, args.out)


def cmd_macro(args):
    bs = login()
    quota_consume()
    end = _padded(args.end or _today())
    start = _padded(args.start or _days_ago(365))
    if args.type == 'deposit':
        rs = bs.query_deposit_rate_data(start_date=start, end_date=end,
                                        currency=args.currency, rowNum=args.rows)
    elif args.type == 'loan':
        rs = bs.query_loan_rate_data(start_date=start, end_date=end,
                                     currency=args.currency, rowNum=args.rows)
    elif args.type == 'rrr':
        rs = bs.query_required_reserve_ratio_data(start_date=start, end_date=end,
                                                  currency=args.currency, rowNum=args.rows)
    elif args.type == 'money-month':
        # 货币供应量接口日期参数要求 yyyy-mm 格式（与利率类 yyyy-mm-dd 不同）
        rs = bs.query_money_supply_data_month(start_date=start[:7], end_date=end[:7])
    elif args.type == 'money-year':
        rs = bs.query_money_supply_data_year(start_date=start[:7], end_date=end[:7])
    else:  # rrr
        rs = bs.query_required_reserve_ratio_data(start_date=start, end_date=end,
                                                  currency=args.currency, rowNum=args.rows)
    emit(run_query(rs), args.out)


def cmd_dividend(args):
    """分红除权除息（baostock: query_dividend_data）。"""
    bs = login()
    quota_consume()
    year = str(args.year or (datetime.now().year - 1))
    df = run_query(bs.query_dividend_data(code=to_bs_code(args.code),
                                          year=year, yearType=args.year_type))
    emit(df, args.out)


def cmd_index_stocks(args):
    """指数成分股（无 date 参数，返回最新成分）。"""
    bs = login()
    quota_consume()
    df = run_query(getattr(bs, INDEX_STOCKS_FNS[args.type])())
    emit(df, args.out)


def build_parser():
    p = argparse.ArgumentParser(
        prog='bs_tool',
        description='baostock 通用查询 CLI（黑名单探测 + 每日配额保护）')
    sub = p.add_subparsers(dest='cmd', required=True)

    sp = sub.add_parser('check', help='登录/黑名单探测（批量任务前必跑）')
    sp.set_defaults(fn=cmd_check)

    sp = sub.add_parser('quota', help='查看本工具当日配额余量')
    sp.set_defaults(fn=cmd_quota)

    sp = sub.add_parser('kline', help='K线（freq: d/w/m/5/15/30/60）')
    sp.add_argument('code')
    sp.add_argument('--freq', default='d',
                    choices=['d', 'w', 'm', '5', '15', '30', '60'])
    sp.add_argument('--adjust', default='3', choices=['1', '2', '3'],
                    help='1=后复权 2=前复权 3=不复权(默认)')
    sp.add_argument('--start', help='YYYY-MM-DD，默认近30天')
    sp.add_argument('--end', help='默认今天')
    sp.add_argument('--fields', help='自定义字段(逗号分隔)，默认按 freq 自动选择')
    sp.add_argument('--out', help='写入 CSV')
    sp.set_defaults(fn=cmd_kline)

    sp = sub.add_parser('trades', help='交易日历')
    sp.add_argument('--start', help='默认近90天')
    sp.add_argument('--end', help='默认今天')
    sp.add_argument('--trading-only', action='store_true', help='只保留交易日')
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_trades)

    sp = sub.add_parser('allstock', help='某交易日全部证券列表')
    sp.add_argument('--day', help='交易日，默认自动取最近交易日')
    sp.add_argument('--a-share', action='store_true', help='只保留A股')
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_allstock)

    sp = sub.add_parser('basic', help='证券基本资料（名称/IPO/退市/状态）')
    sp.add_argument('code')
    sp.add_argument('--name', help='按名称模糊查（代替按代码）')
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_basic)

    sp = sub.add_parser('industry', help='证监会行业分类')
    sp.add_argument('code', nargs='?', default='', help='留空查全市场')
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_industry)

    sp = sub.add_parser('adjust', help='复权因子')
    sp.add_argument('code')
    sp.add_argument('--start', help='默认近5年')
    sp.add_argument('--end', help='默认今天')
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_adjust)

    sp = sub.add_parser('finance', help='季频财务六表')
    sp.add_argument('type', choices=sorted(FINANCE_FNS),
                    help='profit|operation|growth|balance|cashflow|dupont')
    sp.add_argument('code')
    sp.add_argument('--year', type=int, help='默认最近已披露报告期')
    sp.add_argument('--quarter', type=int)
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_finance)

    sp = sub.add_parser('report', help='业绩快报/预告')
    sp.add_argument('type', choices=sorted(REPORT_FNS), help='express|forecast')
    sp.add_argument('code')
    sp.add_argument('--start', help='默认近180天')
    sp.add_argument('--end', help='默认今天')
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_report)

    sp = sub.add_parser('macro', help='宏观数据（无 Shibor 接口，见 Pitfalls）')
    sp.add_argument('type',
                    choices=['deposit', 'loan', 'rrr', 'money-month', 'money-year'])
    sp.add_argument('--start', help='默认近1年')
    sp.add_argument('--end', help='默认今天')
    sp.add_argument('--currency', help='币种如 CNY（仅利率类 deposit/loan/rrr）')
    sp.add_argument('--rows', type=int, help='行数上限 rowNum（仅利率类）')
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_macro)

    sp = sub.add_parser('dividend', help='分红除权除息')
    sp.add_argument('code')
    sp.add_argument('--year', type=int, help='默认去年')
    sp.add_argument('--year-type', default='operate', choices=['report', 'operate'],
                    help='report=按报告期, operate=按除权除息日(默认)')
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_dividend)

    sp = sub.add_parser('index-stocks', help='指数成分股')
    sp.add_argument('type', choices=sorted(INDEX_STOCKS_FNS),
                    help='hs300=沪深300, sz50=上证50, zz500=中证500')
    sp.add_argument('--out')
    sp.set_defaults(fn=cmd_index_stocks)
    return p


def main():
    args = build_parser().parse_args()
    try:
        args.fn(args)
    finally:
        logout()


if __name__ == '__main__':
    main()
