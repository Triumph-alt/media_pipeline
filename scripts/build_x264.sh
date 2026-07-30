#!/bin/bash
# ============================================================
# build_x264.sh — 为 media-pipeline 编译 x264 静态库
#
# 用法:
#   ./scripts/build_x264.sh <arch>
#
# 示例:
#   ./scripts/build_x264.sh x86_64
#   ./scripts/build_x264.sh aarch64
# ============================================================
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "用法: $0 <arch>"
    echo "  支持的架构: x86_64, aarch64"
    exit 1
fi

ARCH="$1"
X264_SRC="/home/thomasweide/Encoder/H264/x264"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${PROJECT_DIR}/build/deps/x264-${ARCH}"
OUTPUT_DIR="${PROJECT_DIR}/third_party/encoders/${ARCH}"
JOBS="${JOBS:-$(nproc)}"

if [ ! -x "${X264_SRC}/configure" ]; then
    echo "错误: x264 源码或 configure 不存在: ${X264_SRC}"
    exit 1
fi

CONFIGURE_ARGS=(
    "--prefix=${OUTPUT_DIR}"
    --enable-static
    --disable-cli
    --disable-opencl
    --enable-pic
    --bit-depth=8
)

case "${ARCH}" in
    x86_64)
        # 当前主机未安装 nasm/yasm 时仍允许完成真实功能验收，只是不构建 x86 汇编优化路径
        if ! command -v nasm >/dev/null 2>&1; then
            CONFIGURE_ARGS+=(--disable-asm)
            echo "提示: 未找到 nasm，x264 将禁用 x86 汇编优化构建"
        fi
        ;;
    aarch64)
        CROSS_PREFIX="/opt/aarch64-linux-gnu-11.4.0-64/bin/aarch64-linux-gnu-"
        if [ ! -x "${CROSS_PREFIX}gcc" ]; then
            echo "错误: aarch64 C 编译器不存在: ${CROSS_PREFIX}gcc"
            exit 1
        fi
        CONFIGURE_ARGS+=(
            --host=aarch64-linux-gnu
            "--cross-prefix=${CROSS_PREFIX}"
        )
        ;;
    *)
        echo "错误: 不支持的架构 '${ARCH}'"
        echo "  支持的架构: x86_64, aarch64"
        exit 1
        ;;
esac

# 每个架构使用独立的中间目录，绝不在外部 x264 源码树内 configure 或编译
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}" "${OUTPUT_DIR}"

printf '%s\n' "============================================"
printf '%s\n' " 编译 x264 静态库"
printf '%s\n' " 架构:   ${ARCH}"
printf '%s\n' " 源码:   ${X264_SRC}"
printf '%s\n' " 构建:   ${BUILD_DIR}"
printf '%s\n' " 输出:   ${OUTPUT_DIR}"
printf '%s\n' "============================================"

cd "${BUILD_DIR}"
"${X264_SRC}/configure" "${CONFIGURE_ARGS[@]}"
make -j"${JOBS}"
make install

printf '\n%s\n' "============================================"
printf '%s\n' " x264 编译完成"
printf '%s\n' " 输出目录: ${OUTPUT_DIR}"
printf '%s\n' "============================================"
