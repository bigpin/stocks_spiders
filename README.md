# 股票信号分析系统

基于 Scrapy 的股票数据爬取和技术分析系统，支持 K 线数据采集、技术指标计算、信号生成和可视化展示。

## 功能特性

### 数据爬取
- 📈 **股票 K 线数据爬取**：从东方财富获取股票历史 K 线数据
- 📊 **技术指标计算**：自动计算 KDJ、MACD、RSI、布林带等技术指标
- 🔍 **股票列表获取**：支持批量获取股票代码列表
- 💾 **数据存储**：使用 SQLite 数据库存储股票信号和分析结果

### 信号分析
- 🎯 **KDJ 信号识别**：自动识别 KDJ 金叉、死叉等交易信号
- 📉 **价格极值追踪**：追踪买入后 30 天内的最高价和最低价
- 📅 **买入当天涨跌记录**：记录买入当天和第二天的涨跌幅
- 📊 **成功率统计**：计算信号的成功率和收益情况

### Web 可视化
- 🎨 **现代化界面**：单页多标签（时间轴 / 收益矩阵 / 数据列表），侧边栏可收起
- 📈 **时间轴视图**：垂直时间轴展示买入点、最高价线、最低价线；Ctrl + 滚轮缩放；点击高亮同股
- 📉 **统计双行**：**全库概览**（总信号、股票数、平均成功率、平均最高涨幅）+ **当前筛选后**（时间轴上实际展示的信号数、涉及股票、平均成功率、平均最高/最低涨幅）
- 🔍 **多条件筛选**：日期、股票代码、买入价区间、买入日/次日涨跌（可选滑块）、**交易热度评分**区间等；矩阵侧另有跟踪天数、最高涨幅、强制卖出天数等
- 📊 **止盈止损触达率矩阵**：按窗口内是否触及止盈/止损统计触达率、触发占比与均值；**触达率筛选阈值**（未达标格子置灰，且不参与 Top10 排名）；可选「未触达时强制持有后卖出」
- 📋 **数据列表**：分页、排序、多条件搜索（与 `/api/signals` 对接）

## 项目结构

```
Spiders/
├── spiders/              # Scrapy 爬虫
│   ├── stock_kline.py    # K 线数据爬虫（主要）
│   ├── stock_detail.py   # 股票详情爬虫
│   ├── get_stock_list.py # 股票列表获取
│   ├── technical_indicators.py  # 技术指标计算
│   └── stock_config.py   # 股票配置
├── web/                  # Web 应用
│   ├── app.py           # Flask 应用
│   ├── templates/       # HTML 模板
│   │   ├── calendar.html  # 主界面（时间轴 + 矩阵 + 数据列表）
│   │   └── index.html     # 列表视图（/list）
│   ├── static/          # CSS / JS
│   └── requirements.txt # Python 依赖
├── run.py               # 运行脚本
├── settings.py          # Scrapy 配置
└── stock_signals.db     # SQLite（位于仓库根目录，与 `Spiders/` 子目录同级；Web 默认路径见下文）
```

## 安装依赖

### Python 环境要求
- Python 3.11+

### 安装 Scrapy 相关依赖

```bash
pip install scrapy scrapy-splash pandas ta
```

（`sqlite3` 为 Python 标准库模块，无需通过 pip 安装。）

### 安装 Web 应用依赖

```bash
cd Spiders/web
pip install -r requirements.txt
```

## 使用方法

### 1. 爬取股票数据

#### 从文件读取股票列表

```bash
python Spiders/run.py
```

或使用 Scrapy 命令：

```bash
cd Spiders
scrapy crawl stock_kline -a use_file=true -a stock_file=../stock_list.txt -a calc_indicators=true
```

#### 指定股票代码

```bash
scrapy crawl stock_kline -a stock_codes=sh603288,sz000858 -a calc_indicators=true
```

#### 获取昨天的数据

```python
from Spiders.run import run_stock_kline_spider_with_yesterday
run_stock_kline_spider_with_yesterday('sh603288,sz000858')
```

### 2. 运行 Web 应用

```bash
cd Spiders/web
python app.py
```

然后在浏览器中访问：http://localhost:5001

### 3. 查看分析结果

- **主界面（时间轴 / 收益矩阵 / 数据列表）**：http://localhost:5001/ （默认）
- **列表视图**：http://localhost:5001/list

更细的 Web 端说明（筛选项、矩阵含义、CloudBase 上传脚本等）见 **`Spiders/web/README.md`**。

## 技术指标说明

系统支持以下技术指标的计算：

- **KDJ**：随机指标（9, 3 参数）
- **MACD**：指数平滑移动平均线（12, 26, 9 参数）
- **RSI**：相对强弱指标（6, 12, 24 周期）
- **布林带**：布林格带（20, 2 参数）

## 数据库结构

### stock_signals 表

主要字段：
- `stock_code`: 股票代码
- `stock_name`: 股票名称
- `insert_date`: 买入日期
- `insert_price`: 买入价格
- `highest_price`: 30 天内最高价
- `lowest_price`: 30 天内最低价
- `buy_day_change_rate`: 买入当天涨跌幅
- `next_day_change_rate`: 第二天涨跌幅
- `overall_success_rate`: 整体成功率
- `trade_heat_score`: 交易热度评分（部分数据源/报告中有值）

Web 时间轴接口 `GET /api/calendar/events` 会返回上述业务字段（含 `overall_success_rate`），并支持查询参数 `heat_min` / `heat_max` 做服务端热度筛选；时间轴上「筛选后」统计还会叠加前端买入价、买卖日涨跌等条件。

## 配置说明

### Scrapy 配置 (settings.py)

- `SPLASH_URL`: Splash 服务地址（用于 JavaScript 渲染）
- `DUPEFILTER_CLASS`: 自定义去重过滤器
- `REQUEST_FINGERPRINTER_IMPLEMENTATION`: 请求指纹实现版本

### Web 应用配置

- 默认端口：5001
- 数据库路径：`Spiders/web/app.py` 从 `web` 目录上溯三级，指向**仓库根目录**下的 `stock_signals.db`（与内含 `Spiders/` 源码子目录同级）。例如克隆在 `.../UGit/Spiders` 时，库文件为 `.../UGit/Spiders/stock_signals.db`

## 注意事项

1. **数据文件**：`.gitignore` 已配置忽略数据文件（`.csv`、`.db`、`kdj_signals_*.txt`），这些文件不会提交到 Git
2. **API Key**：使用股票列表 API 时需要配置 API Key
3. **Splash 服务**：如果使用 `scrapy-splash`，需要先启动 Splash 服务
4. **数据库迁移**：Web 应用启动时会自动执行数据库迁移，添加新字段

## 相关链接

- [GitHub 仓库](https://github.com/bigpin/stocks_spiders)
- [链家爬虫文章](https://mp.weixin.qq.com/s?__biz=MjM5Mzg5NDQ2MA==&mid=2257483733&idx=1&sn=09b33c1e252ae568d8ce173d3fd21784)
- [东方财富爬虫文章](https://mp.weixin.qq.com/s?__biz=MjM5Mzg5NDQ2MA==&mid=2257483733&idx=2&sn=2fdd627ce028b3b945c27658a0c1e06b)

## License

GPL-3.0

