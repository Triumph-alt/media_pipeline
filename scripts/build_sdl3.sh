#!/bin/bash
# ============================================================
# build_sdl3.sh — 为 media-pipeline 编译 SDL3 静态库
#
# 用法:
#   ./scripts/build_sdl3.sh <arch>
#
# 示例:
#   ./scripts/build_sdl3.sh x86_64
#   ./scripts/build_sdl3.sh aarch64
# ============================================================
set -euo pipefail

if [ $# -lt 1 ]; then
    echo "用法: $0 <arch>"
    echo "  支持的架构: x86_64, aarch64"
    exit 1
fi

ARCH="$1"

SDL_SRC="/home/thomasweide/SDL/SDL-release-3.2.28"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUTPUT_DIR="${PROJECT_DIR}/third_party/SDL3/${ARCH}"
BUILD_DIR="${PROJECT_DIR}/build/SDL3-${ARCH}"

if [ ! -d "${SDL_SRC}" ]; then
    echo "错误: SDL3 源码目录不存在: ${SDL_SRC}"
    exit 1
fi

# ============================================================
# 架构相关配置
# ============================================================
CMAKE_EXTRA_FLAGS=""

case "${ARCH}" in
    x86_64)
        ;;
    aarch64)
        CMAKE_EXTRA_FLAGS="-DCMAKE_TOOLCHAIN_FILE=${PROJECT_DIR}/cmake/toolchains/aarch64.cmake"
        ;;
    *)
        echo "错误: 不支持的架构 '${ARCH}'"
        exit 1
        ;;
esac

echo "============================================"
echo " 编译 SDL3 静态库"
echo " 架构:   ${ARCH}"
echo " 源码:   ${SDL_SRC}"
echo " 输出:   ${OUTPUT_DIR}"
echo "============================================"

# ============================================================
# CMake 配置
# ============================================================
cmake -B "${BUILD_DIR}" -S "${SDL_SRC}" \
    -DCMAKE_INSTALL_PREFIX="${OUTPUT_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DSDL_SHARED=OFF \
    -DSDL_STATIC=ON \
    -DSDL_TEST=OFF \
    -DSDL_EXAMPLES=OFF \
    -DSDL_TESTS=OFF \
    ${CMAKE_EXTRA_FLAGS}

# ============================================================
# 编译 & 安装
# ============================================================
cmake --build "${BUILD_DIR}" -j"$(nproc)"
cmake --install "${BUILD_DIR}"

echo ""
echo "============================================"
echo " 编译完成!"
echo " 输出目录: ${OUTPUT_DIR}"
echo "============================================"
