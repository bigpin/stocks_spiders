#!/usr/bin/env bash
# 若用 `sh start.sh` 调用，shebang 不会生效，这里强制用 bash（需 [[、pipefail 等）
if [ -z "${BASH_VERSION:-}" ]; then
  exec /usr/bin/env bash "$0" "$@" || exit 1
fi
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PIDFILE="$SCRIPT_DIR/web.pid"
LOGFILE="$SCRIPT_DIR/web.log"
PORT="${WEB_PORT:-5001}"

if [[ -f "$PIDFILE" ]]; then
  old_pid="$(cat "$PIDFILE")"
  if kill -0 "$old_pid" 2>/dev/null; then
    echo "已在运行 (PID $old_pid)，如需重启请先执行 ./stop.sh"
    exit 1
  fi
  rm -f "$PIDFILE"
fi

if lsof -ti ":$PORT" >/dev/null 2>&1; then
  echo "端口 $PORT 已被占用，请先执行 ./stop.sh 或手动释放端口"
  exit 1
fi

nohup python3 app.py >>"$LOGFILE" 2>&1 &
echo $! >"$PIDFILE"
new_pid="$(cat "$PIDFILE")"
echo "已后台启动，PID ${new_pid}，日志: ${LOGFILE}，端口: ${PORT}"
