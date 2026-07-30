#!/bin/bash
# ============================================================
# build_ffmpeg.sh — 为 media-pipeline 编译 FFmpeg 静态库
#
# 前置条件:
#   1. 先执行 scripts/build_x264.sh <arch>
#   2. 再执行 scripts/build_x265.sh <arch>
#   3. x264/x265 必须安装到 third_party/encoders/<arch>/
#
# 用法:
#   ./scripts/build_ffmpeg.sh <arch>
#
# 示例:
#   ./scripts/build_ffmpeg.sh x86_64
#   ./scripts/build_ffmpeg.sh aarch64
# ============================================================
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "用法: $0 <arch>"
    echo "  支持的架构: x86_64, aarch64"
    exit 1
fi

ARCH="$1"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FFMPEG_SRC="/home/thomasweide/ffmpeg/source/ffmpeg-8.0.1"
BUILD_DIR="/home/thomasweide/ffmpeg/build/ffmpeg-8.0.1/${ARCH}"
OUTPUT_DIR="${PROJECT_DIR}/third_party/ffmpeg/${ARCH}"
ENCODER_PREFIX="${PROJECT_DIR}/third_party/encoders/${ARCH}"
ENCODER_PKGCONFIG_DIR="${ENCODER_PREFIX}/lib/pkgconfig"
JOBS="${JOBS:-$(nproc)}"

if [ ! -x "${FFMPEG_SRC}/configure" ]; then
    echo "错误: 干净 FFmpeg 源码或 configure 不存在: ${FFMPEG_SRC}"
    exit 1
fi
if [ -e "${FFMPEG_SRC}/config.h" ]; then
    echo "错误: FFmpeg 源码树已被 configure，不能用于独立架构构建: ${FFMPEG_SRC}"
    exit 1
fi
if [ ! -f "${ENCODER_PKGCONFIG_DIR}/x264.pc" ] ||
   [ ! -f "${ENCODER_PKGCONFIG_DIR}/x265.pc" ]; then
    echo "错误: 找不到 ${ARCH} 的 x264/x265 pkg-config 元数据"
    echo "请先依次执行:"
    echo "  ./scripts/build_x264.sh ${ARCH}"
    echo "  ./scripts/build_x265.sh ${ARCH}"
    exit 1
fi

CONFIGURE_ARGS=(
    "--prefix=${OUTPUT_DIR}"
    --enable-static
    --disable-shared
    --disable-programs
    --disable-doc
    --disable-avdevice
    --enable-gpl
    --enable-libx264
    --enable-libx265
    --disable-encoders
    --enable-encoder=libx264
    --enable-encoder=libx265
    --enable-decoder=h264
    --enable-decoder=hevc
    --enable-decoder=mpeg4
    --enable-decoder=vp8
    --enable-decoder=vp9
    --enable-decoder=av1
    --enable-decoder=aac
    --enable-decoder=mp3
    --enable-decoder=pcm_s16le
    --enable-demuxer=mov
    --enable-demuxer=matroska
    --enable-demuxer=flv
    --enable-demuxer=mpegts
    --enable-demuxer=mp3
    --enable-demuxer=aac
    --enable-demuxer=rtsp
    --enable-demuxer=rtp
    --enable-demuxer=hls
    --enable-parser=h264
    --enable-parser=hevc
    --enable-parser=aac
    --enable-parser=mpegaudio
    --enable-protocol=file
    --enable-protocol=http
    --enable-protocol=tcp
    --enable-protocol=udp
    --enable-protocol=rtp
    --enable-protocol=rtmp
    --enable-avformat
    --enable-avcodec
    --enable-avutil
    --enable-swscale
    --enable-swresample
    --pkg-config-flags=--static
)

case "${ARCH}" in
    x86_64)
        # 当前开发机没有 nasm/yasm 时仍可完成功能验收，只是不启用 x86 汇编优化
        CONFIGURE_ARGS+=(--disable-x86asm)
        ;;
    aarch64)
        CROSS_PREFIX="/opt/aarch64-linux-gnu-11.4.0-64/bin/aarch64-linux-gnu-"
        if [ ! -x "${CROSS_PREFIX}gcc" ] || [ ! -x "${CROSS_PREFIX}g++" ]; then
            echo "错误: aarch64 交叉工具链不存在: ${CROSS_PREFIX}{gcc,g++}"
            exit 1
        fi
        CONFIGURE_ARGS+=(
            "--cross-prefix=${CROSS_PREFIX}"
            --arch=aarch64
            --target-os=linux
        )
        ;;
    *)
        echo "错误: 不支持的架构 '${ARCH}'"
        echo "  支持的架构: x86_64, aarch64"
        exit 1
        ;;
esac

# configure 产物和全部 object 只属于当前架构的独立目录，FFmpeg 源码树永远保持干净
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}" "${OUTPUT_DIR}"

printf '%s\n' "============================================"
printf '%s\n' " 编译 FFmpeg 静态库"
printf '%s\n' " 架构:   ${ARCH}"
printf '%s\n' " 源码:   ${FFMPEG_SRC}"
printf '%s\n' " 构建:   ${BUILD_DIR}"
printf '%s\n' " 编码器: ${ENCODER_PREFIX}"
printf '%s\n' " 输出:   ${OUTPUT_DIR}"
printf '%s\n' "============================================"

cd "${BUILD_DIR}"
PKG_CONFIG_PATH="" \
PKG_CONFIG_LIBDIR="${ENCODER_PKGCONFIG_DIR}" \
"${FFMPEG_SRC}/configure" "${CONFIGURE_ARGS[@]}"
make -j"${JOBS}"
make install

printf '\n%s\n' "============================================"
printf '%s\n' " FFmpeg 编译完成"
printf '%s\n' " 输出目录: ${OUTPUT_DIR}"
printf '%s\n' "============================================"
