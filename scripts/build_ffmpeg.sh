#!/bin/bash
# ============================================================
# build_ffmpeg.sh — 为 media-pipeline 编译 FFmpeg 静态库
#
# 用法:
#   ./scripts/build_ffmpeg.sh <arch>
#
# 示例:
#   ./scripts/build_ffmpeg.sh x86_64
#   ./scripts/build_ffmpeg.sh aarch64
# ============================================================
set -euo pipefail

# ============================================================
# 参数检查
# ============================================================
if [ $# -lt 1 ]; then
    echo "用法: $0 <arch>"
    echo "  支持的架构: x86_64, aarch64"
    exit 1
fi

ARCH="$1"

# ============================================================
# 路径配置（按需修改）
# ============================================================
FFMPEG_SRC="/home/thomasweide/ffmpeg/ffmpeg-8.0.1"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUTPUT_DIR="${PROJECT_DIR}/third_party/ffmpeg/${ARCH}"

# ============================================================
# 架构相关配置
# ============================================================
CROSS_PREFIX=""
EXTRA_FLAGS=""

case "${ARCH}" in
    x86_64)
        CC="gcc"
        EXTRA_FLAGS="--disable-x86asm"
        ;;
    aarch64)
        CC="/opt/aarch64-linux-gnu-11.4.0-64/bin/aarch64-linux-gnu-gcc"
        CROSS_PREFIX="/opt/aarch64-linux-gnu-11.4.0-64/bin/aarch64-linux-gnu-"
        EXTRA_FLAGS="--arch=aarch64 --target-os=linux"
        ;;
    *)
        echo "错误: 不支持的架构 '${ARCH}'"
        echo "  支持的架构: x86_64, aarch64"
        exit 1
        ;;
esac

# ============================================================
# 检查 FFmpeg 源码
# ============================================================
if [ ! -d "${FFMPEG_SRC}" ]; then
    echo "错误: FFmpeg 源码目录不存在: ${FFMPEG_SRC}"
    exit 1
fi

# ============================================================
# 编译
# ============================================================
echo "============================================"
echo " 编译 FFmpeg 静态库"
echo " 架构:   ${ARCH}"
echo " 源码:   ${FFMPEG_SRC}"
echo " 输出:   ${OUTPUT_DIR}"
echo "============================================"

mkdir -p "${OUTPUT_DIR}"
cd "${FFMPEG_SRC}"

CC="${CC}" ./configure \
    --prefix="${OUTPUT_DIR}" \
    --enable-static \
    --disable-shared \
    --disable-programs \
    --disable-doc \
    --disable-avdevice \
    --disable-encoders \
    --enable-decoder=h264 \
    --enable-decoder=hevc \
    --enable-decoder=mpeg4 \
    --enable-decoder=vp8 \
    --enable-decoder=vp9 \
    --enable-decoder=av1 \
    --enable-decoder=aac \
    --enable-decoder=mp3 \
    --enable-decoder=pcm_s16le \
    --enable-demuxer=mov \
    --enable-demuxer=matroska \
    --enable-demuxer=flv \
    --enable-demuxer=mpegts \
    --enable-demuxer=mp3 \
    --enable-demuxer=aac \
    --enable-demuxer=rtsp \
    --enable-demuxer=rtp \
    --enable-demuxer=rtmp \
    --enable-demuxer=hls \
    --enable-parser=h264 \
    --enable-parser=hevc \
    --enable-parser=aac \
    --enable-parser=mpegaudio \
    --enable-protocol=file \
    --enable-protocol=http \
    --enable-protocol=https \
    --enable-protocol=tcp \
    --enable-protocol=udp \
    --enable-protocol=rtp \
    --enable-protocol=rtmp \
    --enable-protocol=rtsp \
    --enable-avformat \
    --enable-avcodec \
    --enable-avutil \
    --enable-swscale \
    --enable-swresample \
    ${CROSS_PREFIX:+--cross-prefix="${CROSS_PREFIX}"} \
    ${EXTRA_FLAGS}

make -j"$(nproc)"
make install

echo ""
echo "============================================"
echo " 编译完成!"
echo " 输出目录: ${OUTPUT_DIR}"
echo "============================================"
