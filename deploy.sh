#!/usr/bin/env bash
# ------------------------------------------------------------
# 单/多服务一键部署脚本（兼容 macOS 自带 Bash 3.2）
# 1. 将本地压缩包上传到远程服务器指定目录
# 2. 在远程解压并执行重启命令
# ------------------------------------------------------------
set -euo pipefail              # 遇错退出、未定义变量报错、管道出错即失败

########## 可配置区域 ##########
RESTART_CMD="./service.sh unzip"   # 远端执行的重启／解压命令
DEFAULT_KEY="$HOME/.ssh/test"      # 默认私钥文件

# 服务名称列表
SERVICE_KEYS=(nats)
# 各服务对应的目标 <host>:<path> 列表（可写多个，用空格分隔）

TARGETS_nats=(test:~/nats/bin)

# TARGETS_proxy=(agent01:~/agent/bin agent02:~/agent/bin agent03:~/agent/bin agent04:~/agent/bin)
################################

# -------- 帮助信息函数 --------
usage() { cat <<EOF
用法: $(basename "$0") <key|all> [package.zip] [-k <keyfile>]

示例:
  ./deploy.sh botapi             # 上传 botapi.zip
  ./deploy.sh botapi api-v2.zip  # 上传自定义压缩包
  ./deploy.sh all release.zip -k ~/.ssh/id_rsa
EOF
exit 1; }

# -------- 解析命令行参数 --------
(( $# >= 1 )) || usage          # 至少要有一个参数
KEYFILE="$DEFAULT_KEY"
POSITIONAL=()                   # 存放非选项参数

while (( $# )); do
  case "$1" in
    -k) KEYFILE="$2"; shift 2 ;;    # -k 指定私钥
    --) shift; break ;;             # -- 之后全部按位置参数处理
    -*) echo "未知选项: $1" >&2; usage ;;
     *) POSITIONAL+=("$1"); shift ;; # 普通参数
  esac
done

set -- "${POSITIONAL[@]}"
(( $# == 1 || $# == 2 )) || usage   # 参数只能 1 或 2 个

KEYNAME="$1"                        # 服务名或 all
PKG="./bin/${2:-${KEYNAME}.zip}"          # 压缩包，默认 <服务名>.zip

# -------- 生成目标列表 --------
if [[ $KEYNAME == all ]]; then
  DEST_LIST=()
  for key in "${SERVICE_KEYS[@]}"; do
    # eval 取出对应数组
    eval "DEST_LIST+=(\"\${TARGETS_${key}[@]}\")"
  done
else
  # 检查服务名是否存在
  if ! printf '%s\n' "${SERVICE_KEYS[@]}" | grep -qx "$KEYNAME"; then
    echo "未知服务键: $KEYNAME" >&2; exit 2
  fi
  eval "DEST_LIST=(\"\${TARGETS_${KEYNAME}[@]}\")"
fi

[[ -f $PKG ]] || { echo "❌ 本地找不到文件: $PKG" >&2; exit 3; }

echo "► 部署文件 : $PKG"
echo "► 使用私钥 : $KEYFILE"
echo "► 目标列表 :"; printf '   - %s\n' "${DEST_LIST[@]}"
echo "---------------------------------------------"

# -------- ssh-agent：一次性解锁私钥 --------
if [[ -z ${SSH_AUTH_SOCK-} ]]; then
  eval "$(ssh-agent -s)"          # 启动 ssh-agent
  trap 'kill $SSH_AGENT_PID' EXIT # 脚本退出时关闭
fi

FP=$(ssh-keygen -lf "$KEYFILE" | awk '{print $2}')   # 私钥指纹
if ! ssh-add -l 2>/dev/null | grep -q "$FP"; then
  echo "► 请输入私钥口令（整个脚本期间仅需一次）"
  ssh-add "$KEYFILE"
fi

# -------- 主循环：上传并重启 --------
BASENAME=$(basename "$PKG")

for DEST in "${DEST_LIST[@]}"; do
  HOST=${DEST%%:*}            # 主机名
  REMOTE_PATH=${DEST#*:}      # 远程路径 ~/botapi/bin
  REMOTE_ROOT=${REMOTE_PATH%/bin*}   # 去掉 /bin* 得到根目录 ~/botapi

  echo -e "\n➡️  [$HOST] 上传 $BASENAME → $REMOTE_PATH/"
  scp -q "$PKG" "$HOST:$REMOTE_PATH/"

  # 若路径以 ~ 开头，用 \$HOME 让远端展开
  [[ $REMOTE_ROOT == ~* ]] && REMOTE_ROOT="\$HOME${REMOTE_ROOT:1}"

  echo "🔄 [$HOST] 解压并重启 ($RESTART_CMD)"
  ssh -T "$HOST" "set -euo pipefail; cd $REMOTE_ROOT && $RESTART_CMD"
done

echo -e "\n✅ 部署完成"
