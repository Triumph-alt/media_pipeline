#include "pipeline/core/Buffer.h"

#include <cstdlib>
#include <cstring>

extern "C" {
#include <libavutil/frame.h>
#include <libavutil/imgutils.h>
#include <libavutil/pixdesc.h>
}

namespace pipeline {

// ===================================================================
// Buffer::clone: 深拷贝，分叉时每路下游各自拥有一份独立副本。
// 后续优化方向：引用计数共享数据区，只增加引用不拷贝内存。
// ===================================================================
Buffer* Buffer::clone() const {
    auto* copy = new Buffer();
    copy->data = new uint8_t[size];
    copy->size = size;
    memcpy(copy->data, data, size);

    copy->pts = pts;
    copy->dts = dts;
    copy->duration = duration;
    copy->media_type = media_type;
    copy->meta = meta;

    return copy;
}

// ===================================================================
// Buffer::fromAVPacket: 从编码后的 AVPacket 拷入数据
//
// 调用者负责在调用后 av_packet_unref(pkt) 或 av_packet_free。
// 输出 Buffer 的 media_type 由调用者指定（VIDEO_ENCODED / AUDIO_ENCODED）。
// time_base 用于将 FFmpeg 时间戳转为微秒；codec_id 填入 EncodedMeta。
// ===================================================================
Buffer* Buffer::fromAVPacket(const AVPacket* pkt, MediaType type, AVRational time_base, AVCodecID codec_id) {
    if (type != MediaType::VIDEO_ENCODED && type != MediaType::AUDIO_ENCODED) {
        return nullptr;
    }
    if (!pkt || !pkt->data || pkt->size <= 0) {
        return nullptr;
    }

    auto* buf = new Buffer();
    buf->data = new uint8_t[static_cast<size_t>(pkt->size)];
    buf->size = static_cast<size_t>(pkt->size);
    memcpy(buf->data, pkt->data, pkt->size);

    // 时间戳转为微秒；无效时间戳保持 AV_NOPTS_VALUE，不在 Buffer 层推算
    bool valid_time_base = time_base.num > 0 && time_base.den > 0;
    buf->pts = (valid_time_base && pkt->pts != AV_NOPTS_VALUE)
             ? av_rescale_q(pkt->pts, time_base, {1, 1000000})
             : AV_NOPTS_VALUE;
    buf->dts = (valid_time_base && pkt->dts != AV_NOPTS_VALUE)
             ? av_rescale_q(pkt->dts, time_base, {1, 1000000})
             : AV_NOPTS_VALUE;
    buf->duration = (valid_time_base && pkt->duration > 0)
                  ? av_rescale_q(pkt->duration, time_base, {1, 1000000})
                  : 0;

    buf->media_type = type;

    // flags 是逐 packet 元数据，当前直接保留；codec 与流格式由 active Caps 描述。
    EncodedMeta meta;
    meta.flags = pkt->flags;
    buf->meta = meta;

    return buf;
}

// ===================================================================
// Buffer::fromAVFrame: 从解码后的 AVFrame 拷入数据
//
// 支持 planar 格式（YUV420P 等）和 packed 格式。
// 调用者负责在调用后 av_frame_unref(frame) 或 av_frame_free。
// 输出 Buffer 的 media_type 由调用者指定（VIDEO_RAW / AUDIO_RAW）。
// ===================================================================
Buffer* Buffer::fromAVFrame(const AVFrame* frame, MediaType type,
                             AVRational time_base, AVRational framerate) {
    if (!frame) {
        return nullptr;
    }

    bool valid_time_base = time_base.num > 0 && time_base.den > 0;

    if (type == MediaType::VIDEO_RAW) {
        // 视频帧
        int width = frame->width;
        int height = frame->height;
        AVPixelFormat pix_fmt = static_cast<AVPixelFormat>(frame->format);
        if (width <= 0 || height <= 0 || frame->format < 0 || !frame->data[0]) {
            return nullptr;
        }

        // 计算缓冲区大小
        int required = av_image_get_buffer_size(pix_fmt, width, height, 1);
        if (required <= 0) {
            return nullptr;
        }

        auto* buf = new Buffer();
        size_t totalSize = static_cast<size_t>(required);
        buf->data = new uint8_t[totalSize];
        buf->size = totalSize;

        // 拷贝
        int copied = av_image_copy_to_buffer(buf->data, required,
                                             frame->data, frame->linesize,
                                             pix_fmt, width, height, 1);
        if (copied < 0) {
            buf->unref();
            return nullptr;
        }

        buf->pts = (valid_time_base && frame->pts != AV_NOPTS_VALUE)
                 ? av_rescale_q(frame->pts, time_base, {1, 1000000})
                 : AV_NOPTS_VALUE;

        // 视频帧 duration = 1 / framerate
        if (framerate.num > 0 && framerate.den > 0) {
            buf->duration = av_rescale_q(1, {framerate.den, framerate.num}, {1, 1000000});
        } else {
            buf->duration = 0;
        }
        buf->media_type = MediaType::VIDEO_RAW;

        // 视频流格式由先于本帧发送的 CapsEvent 唯一描述；当前紧密连续存储没有逐帧 layout。
        buf->meta = VideoRawMeta{};
        return buf;
    }

    if (type == MediaType::AUDIO_RAW) {
        // 音频帧
        int sample_rate = frame->sample_rate;
        int channels = frame->ch_layout.nb_channels;
        AVSampleFormat sfmt = static_cast<AVSampleFormat>(frame->format);
        int nb_samples = frame->nb_samples;
        if (sample_rate <= 0 || channels <= 0 || nb_samples <= 0 || frame->format < 0) {
            return nullptr;
        }

        int bytes_per_sample = av_get_bytes_per_sample(sfmt);
        if (bytes_per_sample <= 0) {
            return nullptr;
        }

        bool is_planar_audio = av_sample_fmt_is_planar(sfmt);
        const uint8_t* const* audio_data = frame->extended_data
                                         ? frame->extended_data
                                         : const_cast<const uint8_t* const*>(frame->data);
        if (!audio_data) {
            return nullptr;
        }

        size_t bytes = static_cast<size_t>(bytes_per_sample);
        size_t samples = static_cast<size_t>(nb_samples);
        size_t channel_count = static_cast<size_t>(channels);
        if (samples > SIZE_MAX / bytes) {
            return nullptr;
        }
        size_t chSize = samples * bytes;
        if (channel_count > SIZE_MAX / chSize) {
            return nullptr;
        }
        size_t totalSize = channel_count * chSize;

        if (is_planar_audio) {
            for (int ch = 0; ch < channels; ch++) {
                if (!audio_data[ch]) {
                    return nullptr;
                }
            }
        } else if (!audio_data[0]) {
            return nullptr;
        }

        auto* buf = new Buffer();
        buf->data = new uint8_t[totalSize];
        buf->size = totalSize;

        size_t offset = 0;
        if (is_planar_audio) {
            for (int ch = 0; ch < channels; ch++) {
                memcpy(buf->data + offset, audio_data[ch], chSize);
                offset += chSize;
            }
        } else {
            memcpy(buf->data, audio_data[0], totalSize);
        }

        buf->pts = (valid_time_base && frame->pts != AV_NOPTS_VALUE)
                 ? av_rescale_q(frame->pts, time_base, {1, 1000000})
                 : AV_NOPTS_VALUE;

        // 音频帧 duration = nb_samples / sample_rate
        buf->duration = av_rescale_q(nb_samples, {1, sample_rate}, {1, 1000000});
        buf->media_type = MediaType::AUDIO_RAW;

        AudioRawMeta meta;
        // sample_rate/sample_fmt/layout 属于 active AudioRaw Caps；nb_samples 是逐帧属性。
        meta.nb_samples = nb_samples;
        buf->meta = meta;
        return buf;
    }

    // 不支持的类型
    return nullptr;
}

} // namespace pipeline
