#!/usr/bin/env bash
#
# bild.sh — 生产级 Go 项目构建 / 打包脚本
#
# 用法：
#   ./bild.sh [-n 二进制名] [-o linx|darwin|windows] [-a amd64|arm64] [-k] [-h]
#
#   -n  二进制/文件夹名称      (默认: accracy)
#   -o  目标操作系统 GOOS     (默认: linx)
#   -a  目标架构     GOARCH   (默认: amd64)
#   -s  源码目录        (默认: <repo_root>/cmd/{APP_NAME} )
#   -k  打包后保留原始可执行文件 (默认: 删除)
#   -h  显示帮助
#
# 生成产物位于 <repo_root>/bin/<GOOS>-<GOARCH>/目录下
#

set -eo pipefail  # 未定义变量、管道错误立即退出
export GOPROXY=https://goproxy.cn,direct
########################################
# ---------- 辅助函数 ----------
########################################
die()  { echo "ERROR: $1" >&2; exit 1; }
info() { echo "INFO: $1"; }

need() { command -v "$1" &>/dev/null || die "缺少依赖：$1"; }

usage() { grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0; }

########################################
# ---------- 解析参数 ----------
########################################
APP_NAME="nats"
GOOS="linux"
# GOOS="darwin"   
GOARCH="amd64"
KP_IN=false
SRC_DIR=""   # 可选：源码目录

while getopts ":n:o:a:s:kh" opt; do
  case "$opt" in
    n) APP_NAME="$OPTARG" ;;
    o) GOOS="$OPTARG" ;;
    a) GOARCH="$OPTARG" ;;
    s) SRC_DIR="$OPTARG" ;;
    k) KP_IN=false ;;
    h) sage ;;
    *) sage ;;
  esac
done

########################################
# ---------- 环境检查 ----------
########################################
need git
need go
need zip
[[ "$GOOS" == "windows" ]] && need powershell

########################################
# ---------- 路径解析 ----------
########################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/" && pwd)"

# 若未指定 -s，则默认在 cmd/APP_NAME
SRC_DIR="${SRC_DIR:-$REPO_ROOT/cmd/$APP_NAME}"

[[ -d "$SRC_DIR" ]] || die "找不到源码目录：$SRC_DIR（请确认目录存在，或用 -s 指定）"

########################################
# ---------- 版本元数据 ----------
########################################
if git -C "$REPO_ROOT" rev-parse --is-inside-work-tree &>/dev/null; then
  # 有 .git
  if git -C "$REPO_ROOT" rev-parse --verify HEAD &>/dev/null; then
    COMMIT_HASH=$(git -C "$REPO_ROOT" rev-parse --short HEAD)
  else
    COMMIT_HASH="empty"   # 无提交
  fi
else
  COMMIT_HASH="nogit"     # 不是 Git 仓库
fi
BUILD_DATE=$(date '+%Y-%m-%d %H:%M:%S %z')

# IN_DIR="$RPO_ROOT/bin/${GOOS}-${GOARCH}"
BIN_DIR="$REPO_ROOT/bin/"
BIN_PATH="${BIN_DIR}/${APP_NAME}"


########################################
# ---------- 编译 ----------
########################################
info "开始构建 ${APP_NAME} (${GOOS}/${GOARCH})..."
CGO_ENABLED=0 \
GOOS="$GOOS" \
GOARCH="$GOARCH" \
go build -mod=mod -tags=jsoniter \
  -ldflags "-s -w -X 'main.BuildVersion=${COMMIT_HASH}' -X 'main.BuildDate=${BUILD_DATE}'" \
  -o "$BIN_PATH" "$SRC_DIR"

info "已生成二进制：$BIN_PATH"
file "$BIN_PATH"

########################################
# ---------- 打包 ----------
########################################
cd  "$BIN_DIR"
PKG_NAME="${APP_NAME}.zip"

if [[ "$GOOS" == "windows" ]]; then
  info "使用 PowerShell 压缩 ➜ $PKG_NAME"
  powershell -NoProfile -Command \
    "Compress-Archive -Path '${APP_NAME}.exe' -DestinationPath '$PKG_NAME' -force"
  $KP_IN || rm -f "${APP_NAME}.exe"
else
  info "使用 zip 压缩 ➜ $PKG_NAME"
  zip -q "$PKG_NAME" "$APP_NAME"
  $KP_IN || rm -f "$APP_NAME"
fi
info "全部完成➜ $BIN_DIR"
