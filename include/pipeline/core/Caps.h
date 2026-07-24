#pragma once

#include "pipeline/core/Types.h"

#include <cstdint>
#include <vector>

extern "C" {
#include <libavutil/avutil.h>
#include <libavutil/channel_layout.h>
#include <libavutil/pixfmt.h>
#include <libavutil/rational.h>
#include <libavutil/samplefmt.h>
#include <libavcodec/codec_id.h>
}

namespace pipeline {

// ===================================================================
// TemplateCaps 是 Pad 的静态能力描述，决定这条 Pad 理论上能不能连接
//
// Pad 定义时声明可承载的 MediaType 集合，它不承诺某条流最终的 codec、尺寸或采样格式
// Graph::link() 用两端集合是否存在交集，拒绝显然不兼容的拓扑
//
// 例：Decode 输出 {VIDEO_RAW, AUDIO_RAW} 连接 VideoRender 输入 {VIDEO_RAW} 合法
//    Encode 输出 {VIDEO_ENCODED, AUDIO_ENCODED} 连接 VideoRender 则无交集，link 失败
// ===================================================================
struct TemplateCaps {
    std::vector<MediaType> supported_types;

    // 两个 Pad 的静态 MediaType 集合是否有至少一个交集；Graph::link() 的预检查
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

    // 某个实际 MediaType 是否落在本 Pad 的静态能力范围内；Caps 发布/应用时复核
    bool contains(MediaType t) const {
        for (auto x : supported_types) {
            if (x == t) {
                return true;
            }
        }
        return false;
    }
};

// ===================================================================
// ChannelLayout: 框架自己的可复制声道布局值类型
//
// AVChannelLayout 的 CUSTOM 分支含有 FFmpeg 管理的堆指针，不能直接复制或
// 嵌入 CapsEvent。这里将它转换为普通值：native/ambisonic 保存 mask，custom
// 保存有序 channel id；需要调用 FFmpeg 时再临时重建 AVChannelLayout
// ===================================================================
struct ChannelLayout {
    AVChannelOrder order = AV_CHANNEL_ORDER_UNSPEC;
    int channels = 0;
    uint64_t mask = 0;
    std::vector<AVChannel> custom_order;

    // 把 FFmpeg AVChannelLayout 转化为框架 ChannelLayout
    static bool fromAV(const AVChannelLayout& source, ChannelLayout* out);
    // 把框架 ChannelLayout 转化为临时 FFmpeg AVChannelLayout
    bool toAV(AVChannelLayout* out) const;

    bool isValid() const;
    bool isNative() const { return order == AV_CHANNEL_ORDER_NATIVE; }
    bool operator==(const ChannelLayout& other) const;
    bool operator!=(const ChannelLayout& other) const { return !(*this == other); }

    static ChannelLayout stereo();
};

// ===================================================================
// CapsEvent: 运行时 Route 中有位置的完整流配置边界
//
// 每份 Caps 对其所属 MediaType、具体生产者/消费者和生效时间点而言，都必须完整、
// 准确地表达随后至下一份 Caps 前的 Buffer；不存在 best-effort Caps 或后发修正。
// “哪些字段必需”由节点在该边界要完成的职责决定：Decode 消费 encoded 视频普遍需要
// codec_id，VideoRender 消费 raw 视频则必须需要 width/height/pix_fmt。流级字段与
// BufferMeta 的逐帧字段刻意分离，Buffer 由最近一次成功应用的 Caps 完整解释。
// ===================================================================
struct CapsEvent {
    MediaType media_type = MediaType::VIDEO_RAW;

    // VIDEO_RAW / VIDEO_ENCODED 的 payload 格式字段。
    int width = 0;
    int height = 0;
    AVPixelFormat pix_fmt = AV_PIX_FMT_NONE;

    // VIDEO_ENCODED 的 nominal timing hint；Decode 仅用它为输出 Buffer 推导 duration，
    // 它不是 payload 格式，也不进入 hasSameFormat() 或传播到 VIDEO_RAW Caps。
    AVRational framerate = {0, 1};

    // AUDIO_RAW / AUDIO_ENCODED
    int sample_rate = 0;
    AVSampleFormat sample_fmt = AV_SAMPLE_FMT_NONE;
    ChannelLayout channel_layout;

    // VIDEO_ENCODED / AUDIO_ENCODED
    AVCodecID codec_id = AV_CODEC_ID_NONE;
    std::vector<uint8_t> extradata;

    // 用于生产者比较新旧实际格式是否发生变化
    bool hasSameFormat(const CapsEvent& other) const;
};

} // namespace pipeline
