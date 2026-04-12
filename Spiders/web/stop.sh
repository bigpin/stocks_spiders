#!/usr/bin/env bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec /usr/bin/env bash "$0" "$@" || exit 1
fi
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="$SCRIPT_DIR/web.pid"
PORT="${WEB_PORT:-5001}"

stop_pid() {
  local pid="$1"
  kill "$pid" 2>/dev/null || true
  local i=0
  while kill -0 "$pid" 2>/dev/null && [[ $i -lt 20 ]]; do
    sleep 0.25
    i=$((i + 1))
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" 2>/dev/null || true
  fi
}

if [[ -f "$PIDFILE" ]]; then
  pid="$(cat "$PIDFILE")"
  if kill -0 "$pid" 2>/dev/null; then
    stop_pid "$pid"
    echo "已停止 PID $pid"
  else
    echo "PID 文件存在但进程已不存在，清理 PID 文件"
  fi
  rm -f "$PIDFILE"
else
  echo "未找到 ${PIDFILE}，将尝试按端口 ${PORT} 结束进程"
fi

pids_on_port="$(lsof -ti ":$PORT" 2>/dev/null || true)"
if [[ -n "$pids_on_port" ]]; then
  while read -r p; do
    [[ -z "$p" ]] && continue
    stop_pid "$p"
    echo "已结束占用端口 $PORT 的进程 $p"
  done <<<"$(echo "$pids_on_port" | tr ' ' '\n' | sort -u)"
fi

if lsof -ti ":$PORT" >/dev/null 2>&1; then
  echo "警告: 端口 $PORT 仍被占用，请手动检查: lsof -i :$PORT"
  exit 1
fi

echo "已停止（端口 $PORT 空闲）"
