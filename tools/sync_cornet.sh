#!/usr/bin/env bash
# 从自动打包服务同步 cornet 源码到本机。
# 只更新代码与配置，明确跳过本地产物，避免被整包覆盖：
#   .git/  cmake-build-*/  vcpkg_installed/  .vcpkg/  profile_results/  .idea/  .vscode/
#
# 用法（目标机）：
#   ./sync_cornet.sh            # 当前目录是 cornet/ 的父目录
#   ./sync_cornet.sh /path/to   # cornet/ 位于 /path/to/cornet
#   在 cornet 仓库内直接运行也可以，脚本自动找到父目录，
#   并先复制自身到临时位置再执行，防止 tar 覆盖正在运行的脚本。
#
# 注意：tar 只覆盖/新增，源端已删除的文件会残留。
# 目标机 .git 保留，可用 git status / git clean -n 查看差异。
# 覆盖默认地址：CORNET_SYNC_URL=http://.../path ./sync_cornet.sh

set -euo pipefail

URL="${CORNET_SYNC_URL:-http://10.41.52.25:12345/download/workspace/baidu/github/cornet}"

# 在 cornet 仓库内运行时，把自身复制到临时文件重启，避免解包覆盖自己
self_dir=$(cd "$(dirname "$(readlink -f "$0")")" && pwd)
if [[ -z "${CORNET_SYNC_REEXEC:-}" && $(basename "$(dirname "$self_dir")") == "cornet" ]]; then
    self=$(mktemp)
    cp "$(readlink -f "$0")" "$self"
    chmod +x "$self"
    CORNET_SYNC_REEXEC=1 DEST="$(cd "$self_dir/../.." && pwd)" exec "$self" "$@"
fi

dest=${1:-${DEST:-.}}
[[ -d $dest/cornet ]] || { echo "error: $dest/cornet 不存在，请在父目录运行或传入正确路径" >&2; exit 1; }

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
if [[ $URL == file://* ]]; then
    # 本地测试用：wget 不支持 file://，直接拷贝
    cp "${URL#file://}" "$tmp"
else
    wget -q -O "$tmp" "$URL" || { echo "error: 下载失败: $URL" >&2; exit 1; }
fi
[[ -s $tmp ]] || { echo "error: 下载得到空文件: $URL" >&2; exit 1; }

tar -xf "$tmp" -C "$dest" \
    --exclude='cornet/.git' \
    --exclude='cornet/cmake-build-*' \
    --exclude='cornet/vcpkg_installed' \
    --exclude='cornet/.vcpkg' \
    --exclude='cornet/profile_results' \
    --exclude='cornet/.idea' \
    --exclude='cornet/.vscode'

echo "ok: 已更新 $dest/cornet（保留 .git、cmake-build-*、vcpkg_installed 等本地文件）"
