#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

SUDO=""
if [ "$(id -u)" -ne 0 ]; then
    SUDO="sudo"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ─── 内核版本检查 ───
KVER=$(uname -r | cut -d. -f1-2)
KVER_MAJOR=$(echo "$KVER" | cut -d. -f1)
KVER_MINOR=$(echo "$KVER" | cut -d. -f2)
if [ "$KVER_MAJOR" -lt 5 ] || { [ "$KVER_MAJOR" -eq 5 ] && [ "$KVER_MINOR" -lt 11 ]; }; then
    error "内核版本 $(uname -r) 过低，cornet 需要 Linux 5.11+（io_uring 支持）"
fi
info "内核版本 $(uname -r) ✓"

# ─── 系统依赖 ───
info "安装系统依赖..."
if command -v apt-get &>/dev/null; then
    $SUDO apt-get update
    $SUDO apt-get install -y build-essential gcc g++ cmake ninja-build \
        git curl zip unzip tar pkg-config libssl-dev
elif command -v dnf &>/dev/null; then
    $SUDO dnf groupinstall -y "Development Tools"
    $SUDO dnf install -y gcc gcc-c++ cmake ninja-build \
        git curl zip unzip tar pkgconf-pkg-config openssl-devel
elif command -v yum &>/dev/null; then
    $SUDO yum groupinstall -y "Development Tools"
    $SUDO yum install -y gcc gcc-c++ cmake ninja-build \
        git curl zip unzip tar pkgconfig openssl-devel
else
    error "不支持的包管理器，请手动安装: gcc 11+, cmake 3.16+, ninja, git, pkg-config"
fi
info "系统依赖已就绪 ✓"

# ─── GCC 版本检查 ───
GCC_VER=$(gcc -dumpversion | cut -d. -f1)
if [ "$GCC_VER" -lt 11 ]; then
    error "GCC 版本 $(gcc -dumpversion) 过低，需要 GCC 11+（C++20 协程支持）"
fi
info "GCC $(gcc -dumpversion) ✓"

# ─── vcpkg ───
info "检查 vcpkg..."
VCPKG_DIR="$SCRIPT_DIR/.vcpkg"
if command -v vcpkg &>/dev/null; then
    info "vcpkg 已在 PATH 中 ✓"
    export VCPKG_ROOT="${VCPKG_ROOT:-$(dirname "$(command -v vcpkg)")}"
elif [ -n "$VCPKG_ROOT" ] && [ -x "$VCPKG_ROOT/vcpkg" ]; then
    info "vcpkg 已存在于 VCPKG_ROOT=$VCPKG_ROOT ✓"
    export PATH="$VCPKG_ROOT:$PATH"
elif [ -x "$VCPKG_DIR/vcpkg" ]; then
    info "vcpkg 已存在于 $VCPKG_DIR ✓"
    export VCPKG_ROOT="$VCPKG_DIR"
    export PATH="$VCPKG_ROOT:$PATH"
else
    warn "未检测到 vcpkg，正在安装到 $VCPKG_DIR..."
    git clone https://github.com/microsoft/vcpkg.git "$VCPKG_DIR"
    "$VCPKG_DIR/bootstrap-vcpkg.sh" -disableMetrics
    export VCPKG_ROOT="$VCPKG_DIR"
    export PATH="$VCPKG_ROOT:$PATH"
    info "vcpkg 安装完成 ✓"
fi

# ─── 构建 cornet ───
cd "$SCRIPT_DIR"

BUILD_TYPE="${1:-release}"
if [ "$BUILD_TYPE" != "debug" ] && [ "$BUILD_TYPE" != "release" ]; then
    error "用法: ./setup.sh [debug|release]"
fi

info "开始构建 cornet（$BUILD_TYPE）..."
cmake --preset "$BUILD_TYPE"
cmake --build --preset "$BUILD_TYPE"

info "========================================="
info " cornet 环境搭建完成！"
info " 构建产物位于: cmake-build-$BUILD_TYPE/"
info "========================================="
