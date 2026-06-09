#pragma once

#include "Types.h"

extern "C" {
#include <libavutil/pixfmt.h>
#include <libavutil/samplefmt.h>
#include <libavcodec/avcodec.h>
}

namespace pipeline {

/* 流的元数据卡片，在节点之间传递这个流是什么格式的信息 */
struct StreamInfo {
    MediaType type = MediaType::UNKNOWN;

    // 视频格式参数
    int width = 0;
    int height = 0;
    AVRational frameRate = {0, 1};
    AVPixelFormat pixelFmt = AV_PIX_FMT_NONE;

    // 音频格式参数
    int sampleRate = 0;
    int channels = 0;
    AVSampleFormat sampleFmt = AV_SAMPLE_FMT_NONE;

    // 编码
    AVCodecID codecId = AV_CODEC_ID_NONE;
    const AVCodecParameters* codecpar = nullptr;

    // 时间
    AVRational time_base = {0, 1};
    int64_t duration = 0;  // 流总时长（微秒），未知为 0
};

} // namespace pipeline
