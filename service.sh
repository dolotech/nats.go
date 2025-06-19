#!/usr/bin/env bash
#
# service.sh — 生产级服务管理脚本（
#
# 版本: 2024‑05‑18
# 修复：
#   * 处理二进制文件被删除后仍存在的残留进程检测
#   * 增强解压逻辑，确保彻底终止旧进程
#
set -Eeuo pipefail
IFS=$'\n\t'

### ——— 配置 ———————————————————————————————————————————
WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_NAME=${APP_NAME:-nats}
APP_BIN="${WORKDIR}/bin/${APP_NAME}"
CONF_FILE="your token"
LOG_DIR="${WORKDIR}/log"
ULIMIT_NOFILE=${ULIMIT_NOFILE:-65535}
LOCK_FILE="${WORKDIR}/${APP_NAME}.lock"

### ——— 工具检测 ——————————————————————————————————————————
log()  { printf "[%s] %s\n" "$(date +'%F %T')" "$*" >&2; }  # 输出到标准错误
die()  { log "错误: $*"; exit 1; }
need() { command -v "$1" &>/dev/null || die "缺少必需命令: $1"; }
need flock
need pgrep
need ps

### ——— 环境检查 ——————————————————————————————————————————
ensure_env() {
  [[ -f "${APP_BIN}" ]] || die "未找到可执行文件: ${APP_BIN}"
  [[ -x "${APP_BIN}" ]] || chmod +x "${APP_BIN}" || die "无法为 ${APP_BIN} 添加执行权限"
  mkdir -p "${LOG_DIR}"
  ulimit -n "${ULIMIT_NOFILE}" || die "设置 ulimit 失败"
}


### ——— 重复进程检测 ————————————————————————————————————
check_duplicates() {
  local expected=$(readlink -f "${APP_BIN}")
  local basename=$(basename "${APP_BIN}")
  local pids=() dup=()

  # 收集所有可能相关的 PID
  pids+=( $(pgrep -x "$basename" 2>/dev/null || true) )
  pids+=( $(pgrep -f -- "$expected" 2>/dev/null || true) )
  pids+=( $(ps -eo pid,args | awk -v ex="$expected" -v bn="$basename" '
    index($0, ex) || index($0, bn) {print $1}
  ' 2>/dev/null) )

  # 去重并过滤空值
  local unique_pids=($(printf "%s\n" "${pids[@]}" | grep -v '^$' | sort -u))

  for pid in "${unique_pids[@]}"; do
    [[ -z "$pid" ]] && continue
    local exe=$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)
    # 匹配二进制文件（含已删除情况）
    if [[ "$exe" == "$expected" || "$exe" == "${expected} (deleted)" ]]; then
      dup+=("$pid")
      continue
    fi
    # 安全获取命令行参数
        local cmdline=""
        if [[ -r "/proc/$pid/cmdline" ]]; then
          cmdline=$(xargs -0 < "/proc/$pid/cmdline" 2>/dev/null || true)
        fi

        # 匹配工作目录和配置文件
        local cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null || true)
        [[ "$cwd" == "$WORKDIR" && "$cmdline" == *"${CONF_FILE}"* ]] && dup+=("$pid")
  done

  if (( ${#dup[@]} > 0 )); then
    log "检测到同目录已运行实例 (PID: ${dup[*]}), 跳过启动。"
    printf "%s\n" "${dup[@]}"  # 输出 PID 列表到标准输出
    return 1
  fi
  return 0
}

### ——— 启动服务 ——————————————————————————————————————————
start_service() {
  ensure_env
  exec 200>"${LOCK_FILE}" && flock -n 200 || die "有其他操作正在进行，请稍后再试"

  local pids
  if pids=$(check_duplicates); then
    log "无冲突进程，继续启动..."
  else
    log "发现残留进程 (PID: $pids)，正在清理..."
    for pid in $pids; do
      kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 2
    check_duplicates && log "清理成功" || die "无法清理残留进程"
  fi
  ulimit -n 1048576
  local log_file="${LOG_DIR}/${APP_NAME}.$(date +%Y%m%d%H%M%S).log"
  log "正在启动 ${APP_NAME}…"
  "${APP_BIN}" --auth "${CONF_FILE}" >>"${log_file}" 2>&1 &
  local pid=$!
  flock -u 200; exec 200>&-
  sleep 0.5 && kill -0 "$pid" 2>/dev/null || { die "进程启动后立即退出"; }
  log "服务启动成功 (PID: $pid) | 日志: $log_file"
}

### ——— 增强版停止服务 ————————————————————————————————————
stop_service() {
    exec 200>"${LOCK_FILE}" && flock -n 200 || die "有其他操作正在进行，请稍后再试"
  # 不依赖 PID 文件，直接检测进程
  local pids
  if pids=$(check_duplicates); then
    log "服务未运行"
    return 0
  fi

  log "正在停止 ${APP_NAME} (PID: $pids)…"
  for pid in $pids; do
    kill -TERM "$pid" 2>/dev/null || true
  done

  # 等待进程终止
  for ((i=0;i<15;i++)); do
    if check_duplicates; then
      log "已停止"
      return 0
    fi
    sleep 1
  done

  log "超时，发送 SIGKILL"
  for pid in $pids; do
    kill -KILL "$pid" 2>/dev/null || true
  done
  check_duplicates && log "已停止" || die "无法停止服务"
}

### ——— 安全解压逻辑 ————————————————————————————————————
unzip_service() {
  local zip_file="${APP_BIN}.zip"
  [[ -f "$zip_file" ]] || die "升级包不存在: $zip_file"
  log "开始升级…"

  # 强制停止所有关联进程
  if pids=$(check_duplicates); then
    log "未找到运行实例"
  else
    log "发现残留进程 (PID: $pids)，正在清理..."
    stop_service  # 使用增强版停止方法
  fi

  exec 200>"${LOCK_FILE}" && flock -n 200 || die "有其他操作正在进行"
  local backup=""
  [[ -f "${APP_BIN}" ]] && backup="${APP_BIN}.$(date +%s).bak" && cp -fp "${APP_BIN}" "$backup" && log "备份 → $backup"

  unzip -oq "$zip_file" -d "$(dirname "${APP_BIN}")" || {
    flock -u 200; exec 200>&-
    [[ -n "$backup" ]] && mv -f "$backup" "${APP_BIN}"
    die "解压失败，已回滚"
  }

  chmod +x "${APP_BIN}" || log "chmod +x 失败，但继续执行"
  flock -u 200; exec 200>&-

  start_service || {
    log "升级后启动失败"
    [[ -n "$backup" && -f "$backup" ]] && mv -f "$backup" "${APP_BIN}" && start_service
    die "回滚失败，服务已停止"
  }
  log "升级完成"
  # [[ -n "$backup" ]] && rm -f "$backup"
}

### ——— CLI 入口 —————————————————————————————————————————
case "${1:-}" in
  start)   start_service ;;
  stop)    stop_service ;;
  restart) stop_service; start_service ;;
  unzip)   unzip_service ;;
  status)
    if pids=$(check_duplicates 2>/dev/null); then
        log "未运行"
        else
        log "运行中 (PID: ${pids})"
        fi
        ;;
  *) echo "用法: $0 {start|stop|restart|status|unzip}"; exit 1 ;;
esac
