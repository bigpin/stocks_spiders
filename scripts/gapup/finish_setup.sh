#!/bin/zsh
# gapup 一次性收尾编排：等全量分钟抓取完成后自动完成正式训练与部署。
# 手动运行：nohup zsh scripts/gapup/finish_setup.sh >> logs/gapup_setup.log 2>&1 &

set -u
cd /Users/dingli/Documents/UGit/Spiders
PY=/Users/dingli/anaconda3/bin/python

echo "=== [$(date '+%F %T')] 等待全量分钟抓取完成 ==="
while pgrep -f "gapup/fetch_data.py --from-pool" >/dev/null 2>&1; do
    sleep 120
done
echo "=== [$(date '+%F %T')] 分钟抓取已结束，开始重建样本 ==="

$PY scripts/gapup/build_dataset.py || { echo "build_dataset 失败"; exit 1; }

echo "=== [$(date '+%F %T')] 正式训练（日线模型 + 全量模型）==="
$PY scripts/gapup/train_model.py || { echo "train_model 失败"; exit 1; }

echo "=== [$(date '+%F %T')] 全市场日线首抓 + 首次真实筛选 ==="
# 候选日 = 下一个工作日（今晚运行即明天）
FOR_DATE=$($PY - <<'EOF'
from datetime import datetime, timedelta
d = datetime.now() + timedelta(days=1)
while d.weekday() >= 5:
    d += timedelta(days=1)
print(d.strftime('%Y-%m-%d'))
EOF
)
echo "目标候选日: $FOR_DATE"
$PY scripts/gapup/screener.py --for-date "$FOR_DATE" || echo "screener 失败（明日可手动重跑）"

echo "=== [$(date '+%F %T')] 全部收尾完成 ==="
echo "提示：每日定时任务（screener 22:00 / predict 14:30 / evaluate 23:00）请自行在外部调度器配置"
