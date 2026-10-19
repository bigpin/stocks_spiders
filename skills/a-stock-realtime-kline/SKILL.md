---
name: a-stock-realtime-kline
description: A股盘中实时/近期K线三源抓取：东方财富 push2his（主源）→ 腾讯 ifzq.gtimg.cn → 新浪 CN_MarketDataService 自动降级+熔断，免key。分钟线（1/5/15/30/60）+日线+实时快照（quote，支持指数），统一单位口径（volume=股、bar=起始时刻），含交叉验证命令与已收盘bar过滤。历史全量数据用 baostock-data skill；快照/榜单/资金流用 a-stock-market-data skill。
category: data-source
version: 1.2.0
---

# A股实时K线三源抓取

填的空白：**盘中实时分钟 K 线序列**——baostock 只有历史（当日约 19:00 后更新），
opencli 东财是快照/榜单。三个独立供应商自动降级，任一被限流不影响可用性。
核心抓取逻辑抽取自 gapup 流水线每个交易日 14:30 实战运行的代码，限速/熔断参数均为实测值。

**三件套分工**：本 skill = 盘中实时分钟/日线序列；`baostock-data` = 历史全量（K线/财务/宏观）；`a-stock-market-data` = 实时快照/板块/资金流/龙虎榜。

## 三源对比（全部实测 2026-10）

| | 东方财富 push2his | 腾讯 ifzq.gtimg.cn | 新浪 CN_MarketDataService |
|---|---|---|---|
| 链位 | 主源 | 第二源 | 兜底 |
| 分钟频率 | 1/5/15/30/60 | 1/5/15/30/60 | 5/15/30/60（**无 1 分钟**） |
| 分钟回溯 | 任意日期区间 | **仅最近 320 根**（实测上限） | 仅最近 ~1900 根（≥2000 返回 null） |
| 日线 | ✓ 含换手/原生涨幅 | ✓（涨幅为 shift 计算值） | ✓（无换手，amount 恒 0） |
| bar 时刻标注 | 起始时刻（原生） | **结束时刻**（已自动换算为起始） | **结束时刻**（已自动换算为起始） |
| volume 原始单位 | 手（已×100） | 手（已×100，精度=手） | 股 |
| 复权 | 前复权 fqt=1 | 前复权 qfq | 不复权序列（跨除权日慎用） |
| 认证 | 免 key | 免 key | 免 key |
| 已知限制 | 快速连发触发 IP 限流（含周末） | web.ifzq 子域不稳定，用 ifzq.gtimg.cn | 稳定，最佳兜底 |

## 统一出口径（rt_kline 已处理好，直接用即可）

- **volume 统一"股"**，**amount 统一"元"**（腾讯分钟线无 amount、新浪日线 amount 恒 0 → 均置 NaN）
- **分钟 bar 时间统一"起始时刻"**（`bar_start`）：东财原生起始；腾讯/新浪原始为**结束时刻**标注，工具内已减去周期换算。⚠️ 对照 baostock 时注意：baostock 也是结束时刻，如需与 baostock 历史拼接，把 baostock 的 time 减去周期即可对齐
- 日线 `pct_chg` 百分数（东财为接口原生值；腾讯/新浪为 `close/close.shift(1)-1` 计算值）；`turn` 换手率**仅东财有**，其余源 NaN
- 盘中查询包含**当日进行中 bar**（东财/腾讯/新浪均如此）；要"已收盘 bar"过滤 `bar_start + freq_min <= 截止时刻`

## 实测踩坑清单

1. **东财限流**：间隔 <1s 快速连发数十次 → `RemoteDisconnected`，退避数十秒~更久自恢复（**周末也会触发，持续可达数小时**）。对策：1.2s 间隔+0.5s 抖动+指数退避 5/10/20/40s（内置）。
2. **熔断降级**：任一源连续失败 3 次，本次运行内跳过该源直达下一源（内置，链序 em→tx→sina）。
3. **腾讯字段序陷阱**：数组是 `[时间, open, close, high, low, volume, ...]`——**close 在第 2 位**，不是 OHLC 顺序！量纲=手且有手级舍入（与新浪/baostock 差 ~0.001%）。
4. **腾讯回溯仅 320 根**：请求 2000 只返回 320（实测硬上限）；5 分钟线 ≈ 6.7 个交易日，1 分钟线仅约 1.3 个交易日。更早历史用 baostock。
5. **新浪 datalen 上限 ~1900**：1900 根 5 分钟线 ≈ 39.5 个交易日。
6. **新浪日线 amount 恒为 0**（已置 NaN 防误用）；无 turn/preclose（shift 补）。
7. **东财 preclose 坑**：接口涨跌额基准是"真实昨收"，复权价下除权日算出异常昨收——用 `close.shift(1)` 校正（已内置）。
8. **收盘竞价 bar 各源有微小差异**：当日最后一根 bar（14:55-15:00）各源快照时刻不同，量差可达 ~0.1%，收盘价偶有 1 分钱差异——交叉验证 <0.2% 离散度属正常。
9. **1 分钟线**：东财+腾讯有，新浪无（auto 链自动剔除新浪）。
10. 周末/节假日查询分钟线返回上一交易日数据（非报错）；日期区间覆盖不到交易日返回空。
11. **bar 时刻标注差异**（最重要的一条）：东财=起始，腾讯/新浪/baostock=结束。混用多源数据时必须统一换算，否则特征错位一根 bar。
12. **指数必须显式前缀**：`sh000001` 上证指数、`sz399001` 深证成指。裸 `000001` 会被推断为深市个股平安银行！指数的量/额是全市场汇总口径，勿与个股混算。
13. **部分覆盖告警**：tx（仅 320 根）覆盖不到 `--beg` 时会返回能拿到的部分并在 stderr 告警起始日期——要完整长历史直接用 baostock-data；完全越界（区间早于该源全部数据）则自动降级到下一源。
14. **quote 快照时间戳**是最后成交/汇总时刻（收盘后显示 16:1x），盘中为实时。

## 候选源评估记录（为什么只有这三个）

- **网易 126.net**（img1.money.126.net 历史日线 JSON）：2026-10 实测已不可达（HTTP 000），弃。
- **雪球**：需 cookie/token（先访问页面取 xq_a_token），不符合免 key 定位，弃。
- **百度股市通**：需页面 token，弃。
- **akshare/tushare**：是聚合库非数据源，tushare 需积分，不符合零依赖定位，弃。

## CLI 用法：scripts/rt_kline.py

```bash
PY=/Users/dingli/anaconda3/bin/python   # 任意有 pandas 的 python 均可
RT="$PY /path/to/skills/a-stock-realtime-kline/scripts/rt_kline.py"

# 0) 三源连通探测（批量任务前先跑）
$RT check

# 0.5) 交叉验证：各源最近日线收盘价比对（对数据存疑时用）
$RT verify sh600000

# 0.8) 实时快照：现价/涨跌幅/量额（腾讯qt；支持指数 sh000001/sz399001）
$RT quote sh600000 sh000001
$RT quote --codes-file codes.txt --format csv     # 大批量从文件读，#为注释

# 1) 分钟线：默认近5天、5分钟、三源自动降级
$RT minute sh600000
$RT minute 600519 --freq 15 --beg 2026-10-12 --end 2026-10-16
$RT minute sz000001 --freq 1                      # 1分钟：em→tx
$RT minute sh600000 --source tx                   # 强制单源
$RT minute sh600000 --source tx,sina              # 自定义降级链
$RT minute sh600000 --closed-by 14:30             # 只保留14:30前已收盘的bar（决策截止）
$RT minute sh600000 --format json                 # json/csv 输出（程序化对接）

# 2) 日线：近 N 根（换手/涨幅东财最全）
$RT daily sh600000 --days 30

# 3) 批量 + 落盘（每只一个 CSV）
$RT minute sh600000 sz000001 bj920098 --freq 5 --out-dir /tmp/k5
$RT daily sh600000 600519 --days 120 --out-dir /tmp/daily
```

输出列：分钟线 `date/bar_start/open/high/low/close/volume(股)/amount(元|NaN)/source`；日线 `date/open/high/low/close/preclose/volume/amount/pct_chg/turn/source`。
退出码：`0` 成功；`2` 参数/用法错误；`4` 有代码全源无数据（部分成功也报 4，stderr 列明）。

## Python API

```python
from rt_kline import fetch_minute_any, fetch_daily_any
df, source = fetch_minute_any('sh600000', '2026-10-14', '2026-10-16', freq_min=5)  # 三源降级
df, source = fetch_daily_any('sh600000', days=60, source='tx')
```

安全约束：仅 https、仅白名单 host（push2his.eastmoney.com / ifzq.gtimg.cn / quotes.sina.cn），拒绝环回/私有/保留地址（`_url_guard`）。

## 出处与血缘

东财/新浪部分抽取自 `Spiders 项目 scripts/gapup/common.py`（该流水线每交易日 14:30 盘中运行）；腾讯源与三源降级架构为 2026-10-18 新增并实测验证（含真实东财限流场景下的降级演练）。
