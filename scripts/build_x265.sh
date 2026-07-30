#!/bin/bash
# ============================================================
# build_x265.sh — 为 media-pipeline 编译 x265 静态库
#
# 用法:
#   ./scripts/build_x265.sh <arch>
#
# 示例:
#   ./scripts/build_x265.sh x86_64
#   ./scripts/build_x265.sh aarch64
# ============================================================
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "用法: $0 <arch>"
    echo "  支持的架构: x86_64, aarch64"
    exit 1
fi

ARCH="$1"
X265_SRC="/home/thomasweide/Encoder/H265/x265_git/source"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${PROJECT_DIR}/build/deps/x265-${ARCH}"
OUTPUT_DIR="${PROJECT_DIR}/third_party/encoders/${ARCH}"
JOBS="${JOBS:-$(nproc)}"

if [ ! -f "${X265_SRC}/CMakeLists.txt" ]; then
    echo "错误: x265 源码或 CMakeLists.txt 不存在: ${X265_SRC}"
    exit 1
fi

CMAKE_ARGS=(
    "-DCMAKE_INSTALL_PREFIX=${OUTPUT_DIR}"
    -DCMAKE_BUILD_TYPE=Release
    -DENABLE_SHARED=OFF
    -DENABLE_CLI=OFF
    -DENABLE_LIBNUMA=OFF
    -DENABLE_PIC=ON
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DHIGH_BIT_DEPTH=OFF
)

case "${ARCH}" in
    x86_64)
        ;;
    aarch64)
        TOOLCHAIN_FILE="${PROJECT_DIR}/cmake/toolchains/aarch64.cmake"
        if [ ! -f "${TOOLCHAIN_FILE}" ]; then
            echo "错误: aarch64 CMake 工具链文件不存在: ${TOOLCHAIN_FILE}"
            exit 1
        fi
        CMAKE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=${TOOLCHAIN_FILE}")
        ;;
    *)
        echo "错误: 不支持的架构 '${ARCH}'"
        echo "  支持的架构: x86_64, aarch64"
        exit 1
        ;;
esac

# 每个架构使用独立 CMake binary directory，外部 x265 源码树保持不变
rm -rf "${BUILD_DIR}"
mkdir -p "${OUTPUT_DIR}"

printf '%s\n' "============================================"
printf '%s\n' " 编译 x265 静态库"
printf '%s\n' " 架构:   ${ARCH}"
printf '%s\n' " 源码:   ${X265_SRC}"
printf '%s\n' " 构建:   ${BUILD_DIR}"
printf '%s\n' " 输出:   ${OUTPUT_DIR}"
printf '%s\n' "============================================"

cmake -S "${X265_SRC}" -B "${BUILD_DIR}" "${CMAKE_ARGS[@]}"
cmake --build "${BUILD_DIR}" -j"${JOBS}"
cmake --install "${BUILD_DIR}"

printf '\n%s\n' "============================================"
printf '%s\n' " x265 编译完成"
printf '%s\n' " 输出目录: ${OUTPUT_DIR}"
printf '%s\n' "============================================"
