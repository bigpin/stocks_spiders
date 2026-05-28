#!/bin/bash
# 部署静态网站和云函数到微信云开发
# 用法: bash scripts/cloud/deploy_web.sh

set -e

ENV_ID="cloudbase-4g6zx8vx290da64e"
PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

echo "=== 1. 部署云函数 stockApi ==="
cd "$PROJECT_ROOT/cloudfunctions/stockApi"
# 安装依赖（如果 node_modules 不存在）
if [ ! -d "node_modules" ]; then
    echo "安装云函数依赖..."
    npm install --production
fi
tcb fn deploy stockApi -e "$ENV_ID"
echo "云函数部署完成。"

echo ""
echo "=== 2. 部署静态网站 ==="
cd "$PROJECT_ROOT"
tcb hosting deploy ./public -e "$ENV_ID"
echo "静态网站部署完成。"

echo ""
echo "=== 3. 获取云函数 HTTP 触发器地址 ==="
tcb fn trigger list stockApi -e "$ENV_ID" 2>/dev/null || echo "请手动查看云开发控制台获取触发器地址"

echo ""
echo "部署完成！"
echo "静态网站地址: https://${ENV_ID}.tcloudbaseapp.com/"
echo ""
echo "下一步: 更新 public/js/calendar.js 和 public/list.html 中的 API_BASE 为云函数触发器地址，然后重新部署静态网站。"
