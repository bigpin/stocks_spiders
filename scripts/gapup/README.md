# gapup — 次日高开预测流水线

从**全市场**分级过滤出每日 ~100 只候选池，再用盘中数据精选 top 5 只
"次日高开"（T+1 开盘涨幅 ≥ 1%，复权口径）概率最高的股票。

## 三级漏斗

| 级 | 时机 | 数据源 | 动作 |
|---|---|---|---|
| 一级筛选 | 每晚 22:00 | baostock 日线（全市场增量） | 硬规则（流动性/换手/动量/量能）→ 日线模型排序 → 次日 ~100 只候选入 `candidates` 表 |
| 二级特征+精选 | 次日 14:30 | 东财分钟线（盘中实时，每股 1 次请求） | 截至当日 14:30 的盘中特征 + 库内日线特征 → 完整模型打分 → TOP5 报告 |
| 验证 | 当晚 23:00（screener 之后） | 库内日线（新浪/东财兜底） | 回填次日实际开盘 → outcomes 表 + 命中率报告 |

## 文件

| 文件 | 作用 |
|---|---|
| `common.py` | 配置/建表/代码转换/baostock+东财拉取（限速/抖动/指数退避重试）/时间与单位对齐 |
| `fetch_data.py` | 训练宇宙分钟线+日线全历史抓取（断点续抓，2进程，受每日配额保护） |
| `screener.py` | 一级筛选：全市场日线增量 + 硬规则 + 日线模型 → 每日候选池 |
| `build_dataset.py` | 特征工程+标签（`compute_features` 与预测共用，保证口径一致） |
| `train_model.py` | 训练两个模型：`gapup_daily_model.pkl`（晚间筛选）+ `gapup_model.pkl`（盘中精选） |
| `predict.py` | 盘中 14:30 精选：候选池 → TOP5 报告 + predictions 表 |
| `evaluate.py` | 次日验证闭环 + 命中率统计 |

## 数据口径（训练=推理）

- 价格前复权（baostock adjustflag='2' / 东财 fqt=1），高开标签用复权价，除权失真自然抵消
- 成交量统一"股"（东财"手"×100）；分钟 bar 统一"起始时刻"
- 决策截止 14:30：只取 `bar_start + 5min <= 14:30` 的已收盘 bar

## 快速上手

```bash
# 一次性：抓训练宇宙分钟数据（池子成员股全历史，数小时）
python scripts/gapup/fetch_data.py --from-pool

# 训练（产出两个模型；样本自动按一级筛选规则对齐）
python scripts/gapup/build_dataset.py
python scripts/gapup/train_model.py

# 每日三个定时任务（外部调度，见下文"定时任务"）
python scripts/gapup/screener.py            # 晚：全市场日线增量 + 选出次日候选池
python scripts/gapup/predict.py             # 次日14:30：TOP5 精选报告
python scripts/gapup/evaluate.py            # 当晚：回填验证 + 命中率
```

## 定时任务（外部调度器）

每日三个任务由外部调度（cron / 云函数 / 其他机器均可），命令与建议时刻：

```bash
# 交易日 22:00 —— 全市场日线增量 + 选出次日候选池（~100只）
python scripts/gapup/screener.py

# 交易日 14:30 —— 盘中精选：候选池 -> TOP5 高开概率报告
python scripts/gapup/predict.py

# 交易日 23:00 —— 回填次日实际开盘 + 命中率报告（验证前一天14:30的预测；
#                  排在 screener 之后，库内日线已更新，通常无需外部源兜底）
python scripts/gapup/evaluate.py
```

工作目录需为项目根（脚本内部有向上定位项目根的逻辑，从任意 cwd 启动均可）。
日志统一写 `logs/gapup.log`，编排/调试日志另见 `logs/gapup_setup.log`。

## 调参入口

- 硬规则阈值：`common.py` → `CONFIG['screener']`（`screener.py --calibrate` 查看各规则通过数量，
  目标：硬规则通过 200~400 只，模型排序取 top 100）
- 高开阈值 / 决策时点 / TOP N：`CONFIG['gap_threshold_pct']` / `CONFIG['cutoff']` / `CONFIG['top_n_output']`
- 模型超参：`train_model.py` → `make_models()`

## 扩大训练宇宙（提精度路径）

初始宇宙 = 历史信号池 382 只（样本 51.5 万）。扩宇宙能改善模型对"没见过的票"的打分质量，
对日线筛选模型收益最大、分钟模型收益中等（隔夜高开本身噪声大，预期 AUC +0.02~0.05 量级）：

```bash
# 1. 抓全市场流动性 top1500 的分钟历史 —— 每天一批（300~400只，约2万次请求/批），
#    断点续抓，跑完自动从断点继续；不与 22:00 screener 重叠（如每天上午10点跑一次）
python scripts/gapup/fetch_data.py --top 1500 --max-stocks 350

# 2. 全部完成后，重建样本 + 重训双模型
python scripts/gapup/build_dataset.py
python scripts/gapup/train_model.py
```

判断是否抓完：日志出现"待抓取 0 只"即完成（约 4-5 个批次）。

日常维护：每晚 screener 自动积累全市场日线；建议**每月**执行一次上面的
build + train 重训（分钟宇宙不变时约 10 分钟）。

## baostock 配额与并发（重要）

账号级限制：**每日 5 万次请求**。gapup 侧自限 **4 万/日**（为 KDJ 主任务约 5-6K/日留富余）：

- 计数器：`data/gapup/baostock_quota.json`（跨进程文件锁，按天自动归零）
- 超限行为：每次请求前检查，耗尽即终止当日抓取（进度已保存，次日续抓）；screener 配额不足时跳过抓取、直接用库内数据筛选
- 并发：gapup 的 baostock 任务默认 **2 进程**（`CONFIG['baostock_workers']`，`fetch_data.py --workers` 可临时覆盖），叠加每请求 0.2-0.4s 间隔
- 典型用量：每晚 screener 约 5-6K 次；扩宇宙一次性约 19K 次（1302 只 × 14 次查询）

另注意高频会触发**临时黑名单**（报错"黑名单用户"，通常数小时~1天解除）。脚本已有
快速失败保护：启动探测登录 + 连续 3 只黑名单错误即终止。扩宇宙这类大批量任务
安排在白天跑、且不与 22:00 的 screener 重叠。

## 已知边界

- 训练宇宙目前是历史池子成员股（382 只 × 2020 至今）；晚间全市场日线数据积累后，
  建议**按月重训**并逐步扩大训练宇宙
- 隔夜高开预测上限天然偏低（AUC 0.52-0.58 属正常）；验收标准 = TOP5 命中率
  显著高于候选池基础高开率，以 `evaluate.py` 的滚动统计为准
- 东财请求有限速保护（1.2s+抖动+指数退避）；若批量失败多为临时限流，数十秒自动恢复
