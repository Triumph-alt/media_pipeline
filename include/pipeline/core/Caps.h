#pragma once

#include "pipeline/core/Types.h"

#include <cstdint>
#include <vector>

extern "C" {
#include <libavutil/avutil.h>
#include <libavutil/pixfmt.h>
#include <libavutil/rational.h>
#include <libavutil/samplefmt.h>
#include <libavcodec/codec_id.h>
}

namespace pipeline {

// ===================================================================
// TemplateCaps: Pad 的静态能力描述
//
// 每个 Pad 在定义时声明支持的 MediaType 集合。
// Graph::link 时检查两端 TemplateCaps 是否有交集，无交集直接报错。
//
// 示例：
//   DecodeNode SrcPad  → {VIDEO_RAW, AUDIO_RAW}
//   VideoRender SinkPad → {VIDEO_RAW}
//   交集非空 → 允许连接
//
//   EncodeNode SrcPad  → {VIDEO_ENCODED, AUDIO_ENCODED}
//   VideoRender SinkPad → {VIDEO_RAW}
//   交集为空 → Build 阶段报错
// ===================================================================
struct TemplateCaps {
    std::vector<MediaType> supported_types;

    bool isCompatibleWith(const TemplateCaps& other) const {
        for (auto t : supported_types) {
            for (auto o : other.supported_types) {
                if (t == o) {
                    return true;
                }
            }
        }
        return false;
    }
};

// ===================================================================
// CapsEvent: 运行时动态参数事件
//
// Ready 阶段由 Source/DemuxNode 发出，顺流传递到所有下游。
// 下游节点（DecodeNode、VideoRenderNode 等）据此初始化处理器。
//
// 关键设计：DecodeNode 必须等 avcodec_open2() 完成后才能从
// AVCodecContext 读取输出参数（pix_fmt、width、height 等），
// 不能直接透传输入 CapsEvent 的值。
// ===================================================================
struct CapsEvent {
    MediaType media_type = MediaType::VIDEO_RAW;

    // 视频（VIDEO_RAW / VIDEO_ENCODED）
    int width = 0;
    int height = 0;
    AVPixelFormat pix_fmt = AV_PIX_FMT_NONE;
    AVRational framerate = {0, 1};

    // 音频（AUDIO_RAW / AUDIO_ENCODED）
    int sample_rate = 0;
    int channels = 0;
    AVSampleFormat sample_fmt = AV_SAMPLE_FMT_NONE;

    // 编码（VIDEO_ENCODED / AUDIO_ENCODED）
    AVCodecID codec_id = AV_CODEC_ID_NONE;
    std::vector<uint8_t> extradata;  // SPS/PPS 等
};

} // namespace pipeline
