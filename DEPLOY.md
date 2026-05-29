# Web 部署文档 — 微信云开发

## 架构

```
浏览器
  ↓
静态网站托管（HTML/CSS/JS）
  ↓ fetch
云函数 stockApi（HTTP 触发）
  ↓
云数据库（MongoDB）
  ↑ 同步
本地 SQLite（爬虫写入）
```

## 前置条件

- Node.js 18+
- 微信云开发环境已开通（envId: `cloudbase-4g6zx8vx290da64e`）
- 云开发控制台已开启 **HTTP 访问服务**

## 首次部署

### 1. 安装 CloudBase CLI

```bash
npm install -g @cloudbase/cli
tcb login
```

### 2. 创建云数据库集合

在 [云开发控制台](https://tcb.cloud.tencent.com/dev?envId=cloudbase-4g6zx8vx290da64e) 手动创建：

| 集合名 | 说明 | 建议索引 |
|--------|------|---------|
| `web_signals` | 信号数据 | `stock_code`, `insert_date` |
| `web_daily_prices` | 日线价格 | `signal_id`, `(stock_code, date)` |

### 3. 全量同步数据

```bash
python scripts/cloud/sync_sqlite_to_cloud.py --full --verbose
```

### 4. 部署云函数

```bash
cd cloudfunctions/stockApi
npm install --production
tcb fn deploy stockApi -e cloudbase-4g6zx8vx290da64e
```

### 5. 配置 HTTP 路由

在云开发控制台 → **HTTP 访问服务** → **添加路由**：

- 路径：`/api`
- 关联资源：云函数 `stockApi`
- 请求方法：GET

### 6. 部署静态网站

```bash
# 更新 public/js/calendar.js 和 public/list.html 中的 API_BASE
tcb hosting deploy ./public -e cloudbase-4g6zx8vx290da64e
```

静态网站地址：`https://cloudbase-4g6zx8vx290da64e-1323596446.tcloudbaseapp.com`

## 日常更新

爬虫跑完后自动同步（run.py 已集成），或手动：

```bash
# 增量同步（最近7天）
python scripts/cloud/sync_sqlite_to_cloud.py --incremental --verbose

# 全量同步
python scripts/cloud/sync_sqlite_to_cloud.py --full --verbose
```

## 一键部署脚本

```bash
bash scripts/cloud/deploy_web.sh
```

会自动部署云函数和静态网站，但 HTTP 路由需手动在控制台配置。

## 关键文件

| 文件 | 说明 |
|------|------|
| `cloudfunctions/stockApi/index.js` | 云函数（6个API端点） |
| `cloudfunctions/stockApi/config.json` | 触发器配置 |
| `scripts/cloud/sync_sqlite_to_cloud.py` | 数据同步脚本 |
| `scripts/cloud/deploy_web.sh` | 一键部署脚本 |
| `public/` | 静态文件目录（部署用） |
| `Spiders/web/` | 本地开发用（Flask） |

## API 端点

| 路径 | 说明 |
|------|------|
| `/api/stats` | 统计概览 |
| `/api/stock-codes` | 股票代码列表 |
| `/api/calendar/events` | 时间轴事件 |
| `/api/signals` | 分页信号列表 |
| `/api/filter-options` | 筛选选项 |
| `/api/signal-daily-prices` | 日线价格 |

## 注意事项

- 日线价格数据保留 30 天，过期自动清理
- 云数据库每次同步前会清理过期数据
- 本地 Flask 开发时 `API_BASE` 设为空字符串
- 云函数冷启动首次请求约 1-2 秒
