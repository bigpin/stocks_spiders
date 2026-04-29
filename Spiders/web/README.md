# 股票信号分析系统 Web 客户端

用于展示股票信号分析结果的 Web 应用，提供数据可视化、筛选分析和收益计算功能。

## 功能特性

### 数据展示
- 📊 **统计信息面板**：**全库概览**一行 + **当前筛选后（时间轴数据）**一行（信号数、涉及股票、平均成功率、平均最高/最低涨幅）
- 📈 **时间轴视图**：可视化展示股票买入点和卖出点的时间线
- 📋 **数据列表**：表格分页展示信号，支持排序与筛选（主界面标签页之一）

### 筛选功能
- 🔍 **时间轴侧栏**：股票代码、插入日期范围、买入价区间、买卖日涨跌（可选滑块）、交易热度区间、显示开关等
- 🔍 **收益矩阵侧栏**：日期区间（最近 7/14/30…天）、跟踪天数、买入价与最高涨幅区间、买卖日涨跌（可选）、热度区间、**触达率筛选**（低于阈值的格子置灰且不参与 Top10）、强制卖出选项等

### 收益分析
- 💰 **止盈止损触达率矩阵**：
  - 按窗口内是否触及止盈/止损统计**触达率**、触发中止盈/止损占比、触发均值等
  - 止盈 2%～30%、止损 -2%～-30%，步长 2%
  - 综合得分 Top10 / Bottom10 着色；触达率未达筛选阈值的不参与 Top10
  - 可选「未触达时强制持有后卖出」及持有天数
  - 单元格悬停：明细与参与股票列表

### 时间轴功能
- 📅 **时间轴可视化**：
  - 垂直时间轴展示股票信号
  - 买入点颜色根据买入当天和第二天涨跌显示
  - **Ctrl + 滚轮**缩放时间间距；普通滚轮上下滚动时间轴
  - 点击选中/取消选中股票
  - 鼠标悬停显示详细信息

## 安装依赖

```bash
pip install -r requirements.txt
```

依赖包：
- Flask
- SQLite3（Python 内置）

## 运行应用

```bash
python app.py
```

应用将在 `http://0.0.0.0:5001` 启动。

然后在浏览器中访问：http://localhost:5001

## 使用说明

## 每日报告上传到腾讯云云数据库（CloudBase HTTP API）

本目录新增了两个脚本，用于把 `kdj_signals_YYYYMMDD.txt` 解析为结构化数据并上传到云数据库（单集合 `stock_signals`），并提供查询/删除/导出能力。

- **上传脚本**：`upload_daily_report_to_cloudbase.py`
- **管理脚本**：`manage_cloud_stock_signals.py`

### 数据结构（单集合，report_id 关联形成完整报告）

- `doc_type=stock_summary`：每个股票一条“报告头”（`_id == report_id == report_{report_date}_{stock_code}`）  
  含 `overall_success_rate`、`total_signal_count`、`total_success_count`，以及 **`trade_heat_score` / `trade_heat_max`**（来自 txt 中「最近交易热度评分: x/y」，旧报告无此行则为 `null`）
- `doc_type=signal_event`：每条“信号明细”（`report_id` 指向对应的 `stock_summary`，形成完整报告）

### 配置（不要把 AppSecret 提交到仓库）

请在 `Spiders/web/` 下创建 `.env`（可参考 `env.example`），内容为：

- `CLOUDBASE_ENV_ID`
- `WECHAT_APPID`
- `WECHAT_APPSECRET`

同时建议把 `.env` 与 `.cache/` 加入 `.gitignore`（可参考 `gitignore.snippet`）。

### 常用命令示例

- 上传某天报告（幂等，重复上传会覆盖同一 `_id`）：  
  `python3 upload_daily_report_to_cloudbase.py --file /Users/dingli/Documents/UGit/Spiders/kdj_signals_20260115.txt`

- 获取完整报告（报告头 + 明细）：  
  `python3 manage_cloud_stock_signals.py get-report --report_date 2026-01-15 --stock_code sh601231`

- 删除某天某股票报告（危险操作）：  
  `python3 manage_cloud_stock_signals.py delete-report --report_date 2026-01-15 --stock_code sh601231 --yes`

- 导出完整报告到 JSON：  
  `python3 manage_cloud_stock_signals.py export-report --report_date 2026-01-15 --stock_code sh601231 --out /tmp/report_sh601231_2026-01-15.json`

### 时间轴视图（默认页面）

1. **筛选股票**：
   - 选择股票代码（下拉选择）
   - 设置日期范围
   - 设置买入价格范围
   - 使用滑块设置买入当天和第二天涨跌范围（需勾选启用）

2. **查看时间轴**：
   - 鼠标滚轮上下滚动时间轴
   - Ctrl + 滚轮缩放时间间距
   - 点击买入点选中/取消选中股票
   - 鼠标悬停查看详细信息

3. **买入点颜色说明**：
   - 🔴 深红色：买入当天上涨 + 第二天上涨
   - 🟢 深绿色：买入当天下跌 + 第二天下跌
   - 🟠 橙色：买入当天上涨 + 第二天下跌
   - 🟢 浅绿色：买入当天下跌 + 第二天上涨
   - 🔴 红色：仅买入当天上涨
   - 🟢 绿色：仅买入当天下跌

### 收益矩阵分析

1. **设置筛选条件**：
   - 日期区间
   - 跟踪天数范围（默认最小10天）
   - 买入价格范围
   - 最高涨幅范围
   - 买入当天/第二天涨跌范围（滑块，需勾选启用）
   - 交易热度评分区间
   - **触达率筛选**（最小触达率%，默认 80；未达标格子置灰）
   - 未触达强制卖出及持有天数

2. **查看收益矩阵**：
   - 横列为止盈百分比（2% 到 30%）
   - 纵列为止损百分比（-2% 到 -30%）
   - 单元格展示触达率、触发均值及止盈/止损占比等（开启强卖时另有全量均值说明）
   - 黄色等规则：高质量组合、Top10、低触达参考色等（见页面内图例）

3. **查看详细信息**：
   - 鼠标悬停在单元格上查看参与计算的股票列表
   - 显示股票代码、名称、持有天数、收益和类型

### 列表视图

访问 http://localhost:5001/list 查看列表视图：

1. **筛选数据**：使用筛选条件区域设置筛选条件，点击"搜索"按钮
2. **排序数据**：点击表头可以按该列排序
3. **分页浏览**：使用底部分页控件浏览更多数据

## API 接口

### 获取信号数据
```
GET /api/signals
参数：
  - stock_code: 股票代码（可选）
  - stock_name: 股票名称（可选）
  - signal_type: 信号类型（可选）
  - min_success_rate: 最小成功率（可选）
  - date_from: 开始日期（可选）
  - date_to: 结束日期（可选）
  - page: 页码（默认1）
  - per_page: 每页数量（默认20）
```

### 获取统计数据
```
GET /api/stats
返回：
  - total_signals: 总信号数
  - total_stocks: 总股票数
  - avg_success_rate: 平均成功率
  - avg_highest_change: 平均最高涨幅
```

### 获取时间轴事件
```
GET /api/calendar/events
参数：
  - stock_code: 股票代码（可选）
  - date_from: 开始日期（可选）
  - date_to: 结束日期（可选）
  - heat_min / heat_max: 交易热度评分下限/上限（可选）
```

返回事件中包含 `overall_success_rate` 等字段，供前端「筛选后」统计使用。

### 获取股票代码列表
```
GET /api/stock-codes
返回所有股票代码和名称列表
```

## 数据库

Web 应用使用**仓库根目录**下的 `stock_signals.db`（与内含 `Spiders/` 源码子目录同级）。

启动时会自动执行数据库迁移，添加新字段（如 `buy_day_change_rate`、`next_day_change_rate`、`trade_heat_score`）。

## 注意事项

1. **端口占用**：默认使用 5001 端口，如果被占用可以修改 `app.py` 中的端口号
2. **数据库路径**：确保数据库文件存在于项目根目录
3. **筛选性能**：大量数据时筛选可能需要一些时间，请耐心等待
4. **浏览器兼容性**：建议使用现代浏览器（Chrome、Firefox、Safari、Edge）

## 技术栈

- **后端**：Flask
- **前端**：HTML5、CSS3、JavaScript（原生）
- **数据库**：SQLite3
- **可视化**：自定义时间轴实现

