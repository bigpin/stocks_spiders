---
name: baostock-data
description: A股历史数据源 baostock 全功能使用手册与通用 CLI。覆盖 K线（日/周/月/5-60分钟线，复权）、交易日历、复权因子、证券列表/基本资料/行业、季频财务六表、业绩快报/预告、宏观利率/Shibor。含实测踩坑清单（pctChg百分数、量单位=股、分钟time为bar结束时刻、当日数据约19:00才更新）、拉黑探测+每日5万次配额保护、新浪/东财兜底链。要实时行情/板块/资金流用 a-stock-market-data skill。
category: data-source
version: 1.0.0
---

# baostock A股历史数据源

免费 A 股历史数据库（Python SDK，证券代码格式 `sh.600000` / `sz.000001` / `bj.920978`）。
本机可用解释器：`/Users/dingli/anaconda3/bin/python`（python3.11，已装 baostock 0.9.10 + pandas）。

**定位分工**：baostock 只有历史数据（约 19:00 更新完当日）；要**盘中实时行情、板块、资金流、龙虎榜**用 `a-stock-market-data` skill（opencli 东财），兜底顺序见文末。

## 通用调用模式

所有查询接口返回 `ResultData`，统一这样取 DataFrame：

```python
import socket, baostock as bs
import pandas as pd

socket.setdefaulttimeout(120)   # 防服务端不响应时无限阻塞（必设）
lg = bs.login()
assert lg.error_code == '0', lg.error_msg

rs = bs.query_history_k_data_plus("sh.600000", "date,open,high,low,close,volume",
                                  start_date='2026-09-01', end_date='2026-09-30',
                                  frequency='d', adjustflag='2')
rows, fields = [], rs.fields
while rs.next():
    rows.append(rs.get_row_data())
df = pd.DataFrame(rows, columns=fields)
# 所有列都是字符串，需自行 to_numeric
bs.logout()
```

注意：**返回值全为字符串**，数值列必须 `pd.to_numeric(df[col], errors='coerce')`。字段名不用背——`rs.fields` 自带。

## 功能全景

### 1. K线 `bs.query_history_k_data_plus(code, fields, start_date, end_date, frequency, adjustflag)`

- `frequency`：`d` 日 / `w` 周 / `m` 月 / `5` `15` `30` `60` 分钟线。
- `adjustflag`：`1`=后复权，`2`=前复权，`3`=不复权（**默认 3**，不要凭直觉写成别的）。
- 指数同样走这个接口：`sh.000001` 上证、`sz.399001` 深成、`sh.000300` 沪深300、`sh.000688` 科创50、`sz.399006` 创业板指。

**日线可用字段**（分钟线只有前 9 个基础字段）：

| 字段 | 含义 | 分钟线 |
|---|---|---|
| date / time | 日期 / bar 时间 | 日线 date；分钟线 time |
| code | 证券代码 | ✓ |
| open/high/low/close | 价格 | ✓ |
| preclose | 前收盘 | ✗（分钟线无） |
| volume / amount | 成交量（**股**）/ 成交额（元） | ✓ |
| adjustflag | 复权标记 | ✓ |
| turn | 换手率% | ✗ |
| tradestatus | 交易状态 1正常 0停牌 | ✗ |
| pctChg | 涨跌幅 **百分数** | ✗ |
| peTTM / pbMRQ / psTTM / pcfNcfTTM | 估值（TTM市盈/市净/市销/市现） | ✗ |
| isST | 是否ST | ✗ |

### 2. 交易日历 `bs.query_trade_dates(start_date, end_date)`
返回 `(calendar_date, is_trading_day)`，`is_trading_day == '1'` 为交易日。**判断"下一个交易日"必须用它**，不要工作日推算（节假日会产出垃圾数据）。

### 3. 复权因子 `bs.query_adjust_factor(code, start_date, end_date)`
返回 foreAdjustFactor / backAdjustFactor。自己算复权价时用。

### 4. 证券基本资料 `bs.query_stock_basic(code, code_name)`
返回 code/code_name/ipoDate/outDate/type/status。查名称、IPO 日期、退市状态。

### 5. 全量证券列表 `bs.query_all_stock(day)`
`day` 必须是**交易日**（传非交易日会空/报错）。day 为空时先用交易日历找最近交易日。返回 code/tradeStatus/code_name，含指数与 B 股，需按市场前缀过滤（见 bs_tool.py 的 A 股过滤）。

### 6. 行业分类 `bs.query_stock_industry(code)`
证监会行业（industryClassification/industry/industryName）。

### 7. 季频财务（参数均为 `code, year, quarter`）

| 接口 | 内容 | 代表字段 |
|---|---|---|
| `bs.query_profit_data` | 盈利 | roeAvg、npMargin、epsTTM、MBRevenue |
| `bs.query_operation_data` | 营运 | NRTurnRatio、INVTurnRatio、CATurnRatio |
| `bs.query_growth_data` | 成长 | YOYEquity、YOYAsset、YOYNI、YOYEPSBasic |
| `bs.query_balance_data` | 偿债 | currentRatio、quickRatio、cashRatio、YOYLiability |
| `bs.query_cash_flow_data` | 现金流 | CFOToOR、CFOToNP、CAToAsset |
| `bs.query_dupont_data` | 杜邦 | roeDupont 等杜邦拆解指标 |

字段以 `rs.fields` 实际返回为准。PE 可自算：`close / epsTTM`。

### 8. 业绩快报 / 预告
- `bs.query_performance_express_report(code, start_date, end_date)` 业绩快报
- `bs.query_forecast_report(code, start_date, end_date)` 业绩预告（profitForcastExpire、forecastType 预增/预减等）——**注意不叫 query_performance_forecast_report，旧文档名在 0.9.10 会 AttributeError**

### 9. 宏观数据

| 接口 | 内容 |
|---|---|
| `bs.query_deposit_rate_data(start_date, end_date, currency, rowNum)` | 存款利率 |
| `bs.query_loan_rate_data(start_date, end_date, currency, rowNum)` | 贷款利率 |
| `bs.query_required_reserve_ratio_data(start_date, end_date, currency, rowNum)` | 存款准备金率 |
| `bs.query_money_supply_data_month(start_date, end_date)` | 货币供应量（月） |
| `bs.query_money_supply_data_year(start_date, end_date)` | 货币供应量（年） |

（0.9.10 **没有 Shibor 接口**，网上旧文档的 `query_shibor_data` 不存在。）

### 10. 指数成分股与分红
- `bs.query_hs300_stocks()` / `bs.query_sz50_stocks()` / `bs.query_zz500_stocks()` — 沪深300 / 上证50 / 中证500 最新成分（无参数）
- `bs.query_dividend_data(code, year, yearType)` — 分红除权除息；`yearType`：`report` 按报告期、`operate` 按除权除息日（默认）

## 实测踩坑清单（每条都踩过，必读）

1. **`pctChg` 是百分数**（如 `-9.98` 表示跌停），不是小数。与自算 `close.pct_change()`（小数）混用会产生 ~100 倍筛选误差——gapup 项目实际踩过。
2. **成交量单位=股**。东财分钟线=手（×100 转股对齐），新浪=股。
3. **分钟线 `time` 是 bar 的结束时刻**（格式 `YYYYMMDDHHMMSSsss`），取 bar 起始时刻要减去周期。
4. **当日盘中查不到当日数据**，更新停在上一交易日，当日约 19:00 后才有。
5. **5 分钟线最早 2020-01-02**；不支持 1 分钟线（最细 5 分钟）。
6. **分钟线只有 OHLCV+amount**，无 preclose/turn/pctChg/估值字段。
7. **复权口径**：`adjustflag` 1=后 2=前 3=不复权（默认 3）。跨除权日算收益率务必用复权价或复权因子。
8. **preclose 稳妥做法**：主系统封装 `Spiders/spiders/baostock_helper.py` 的 column_mapping 曾把 preclose 列丢弃导致落库全 NULL。自建封装时：要么保留列，要么直接 `close.shift(1)`（gapup 的 `fetch_data.upsert_daily` 即此做法）。
9. **`query_all_stock` 的 day 必须是交易日**；取"最新列表"先查交易日历再回退逐日尝试。
10. **北交所**：`920978` → `bj.920978`（前缀 `92` 映射 bj）；`8`/`4` 开头也是 bj。
11. **SDK 非线程安全**：多进程并行时每个子进程独立 `login()`；长任务建议每 N 次请求主动 logout+login 换会话（主系统 `BAOSTOCK_RELOGIN_EVERY_N_REQUESTS` 模式）。
12. **日线 `volume` 停牌日为 0、`tradestatus=0`**，回测时记得过滤。
13. **API 名以 `dir(baostock)` 实测为准**：旧文档的 `query_performance_forecast_report`（实为 `query_forecast_report`）和 `query_shibor_data`（0.9.10 无此接口）会直接 AttributeError。bs_tool.py 已对齐实测版本（0.9.10）。
14. **货币供应量接口的日期参数格式是 `yyyy-mm`**（不是 `yyyy-mm-dd`，传错报"起始日期格式不正确"）；利率类接口仍是 `yyyy-mm-dd`。bs_tool.py 已处理。

## 风控保护（批量任务必做）

- **账号级限额：每日 5 万次请求**。建议自限 4 万/日，跨进程文件锁计数（实现见 `scripts/gapup/common.py` 的 `baostock_quota_consume`，或直接用 bs_tool.py 内置配额）。
- **高频抓取会临时拉黑**：实测 ~3.5 万次请求后报 `10001011 黑名单用户，请与管理员联系`，一般数小时~1 天自动解除。批量任务**开始前必须探测**（bs_tool.py `check` 子命令），探测到黑名单立即终止，别继续加重封禁。
- **socket 超时必设**（120s），否则服务端不响应时进程无限阻塞。
- **并发 ≤ 2 进程**，请求间隔留 jitter。

## 数据兜底链（baostock 不可用/不够实时时）

1. **新浪** `quotes.sina.cn` 的 `CN_MarketDataService.getKLineData`：5 分钟线盘中实时、免 key、量/额单位与 baostock 一致（股/元），最佳兜底。datalen 上限约 1900。
2. **东财 push2his**：分钟线实时但限流敏感，需 ~1.2s 间隔+抖动+指数退避；volume=手。
3. 实时快照类（盘口/资金流/龙虎榜）：`a-stock-market-data` skill 的 opencli eastmoney。

## 通用 CLI：scripts/bs_tool.py

内置登录探测、黑名单快速失败、每日配额（文件锁计数，默认自限 4 万，`BAOSTOCK_DAILY_LIMIT` 可调）。配额文件独立于 gapup（`BAOSTOCK_QUOTA_FILE` 可指向共享文件，注意 gapup 侧限额是 4 万，两边别叠加超账号上限）。

```bash
PY=/Users/dingli/anaconda3/bin/python
BS="$PY /Users/dingli/Documents/UGit/Spiders/skills/baostock-data/scripts/bs_tool.py"

# 登录探测（批量任务前必跑；黑名单退出码 3）
$BS check

# K线：日/周/月/分钟 + 复权
$BS kline sh.600000 --freq d --adjust 2 --start 2026-01-01 --end 2026-10-15
$BS kline sz.000001 --freq 5 --start 2026-10-09 --end 2026-10-15 --out /tmp/k5.csv

# 交易日历 / 全量证券列表 / 基本资料 / 行业 / 复权因子
$BS trades --start 2026-10-01 --end 2026-10-31
$BS allstock --a-share            # day 为空自动取最近交易日
$BS basic sh.600000
$BS industry sh.600000
$BS adjust sh.600000 --start 2020-01-01

# 季频财务：profit|operation|growth|balance|cashflow|dupont（报告期默认最近已披露）
$BS finance profit sh.600000 --year 2026 --quarter 2

# 业绩快报/预告：express|forecast
$BS report forecast sh.600000 --start 2026-01-01 --end 2026-10-15

# 分红除权除息 / 指数成分股
$BS dividend sh.600000 --year 2025
$BS index-stocks hs300            # hs300 | sz50 | zz500

# 宏观：deposit|loan|rrr|money-month|money-year（无 Shibor 接口）
$BS macro money-month --start 2026-01-01

# 配额余量
$BS quota
```

退出码：`0` 正常；`3` 黑名单（脚本/编排据此快速失败）；`2` 参数/网络错误。`--out FILE.csv` 落盘时同时打印行数摘要。

## 项目内参考实现

- `Spiders/spiders/baostock_helper.py` — 主系统封装：登录单例、socket 超时、周期重登、多进程 worker、复权因子、估值合成 PE。
- `scripts/gapup/common.py` — 黑名单探测 `baostock_login_ok()` + 每日配额文件锁 `baostock_quota_consume()`。
- `scripts/gapup/fetch_data.py` — 全市场增量落库（preclose 自动补 `close.shift(1)`）。
