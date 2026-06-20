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
    if (!pkt || !pkt->data || pkt->size <= 0) {
        return nullptr;
    }

    auto* buf = new Buffer();
    buf->data = new uint8_t[static_cast<size_t>(pkt->size)];
    buf->size = static_cast<size_t>(pkt->size);
    memcpy(buf->data, pkt->data, pkt->size);

    // 时间戳转为微秒
    buf->pts = av_rescale_q(pkt->pts, time_base, {1, 1000000});
    buf->dts = av_rescale_q(pkt->dts, time_base, {1, 1000000});
    buf->duration = av_rescale_q(pkt->duration, time_base, {1, 1000000});

    buf->media_type = type;

    // 运行时格式变化再补充完整字段
    EncodedMeta meta;
    meta.codec_id = codec_id;
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

    auto* buf = new Buffer();

    if (type == MediaType::VIDEO_RAW) {
        // 视频帧
        int width = frame->width;
        int height = frame->height;
        AVPixelFormat pix_fmt = static_cast<AVPixelFormat>(frame->format);

        // 计算缓冲区大小
        int required = av_image_get_buffer_size(pix_fmt, width, height, 1);
        if (required < 0) {
            delete buf;
            return nullptr;
        }

        size_t totalSize = static_cast<size_t>(required);
        buf->data = new uint8_t[totalSize];
        buf->size = totalSize;

        // 拷贝
        av_image_copy_to_buffer(buf->data, required,
                                frame->data, frame->linesize,
                                pix_fmt, width, height, 1);

        buf->pts = av_rescale_q(frame->pts, time_base, {1, 1000000});

        // 视频帧 duration = 1 / framerate
        if (framerate.num > 0 && framerate.den > 0) {
            buf->duration = av_rescale_q(1, {framerate.den, framerate.num}, {1, 1000000});
        } else {
            buf->duration = 0;
        }
        buf->media_type = MediaType::VIDEO_RAW;

        VideoRawMeta meta;
        meta.width = width;
        meta.height = height;
        meta.pix_fmt = pix_fmt;
        buf->meta = meta;
    } else if (type == MediaType::AUDIO_RAW) {
        // 音频帧
        int sample_rate = frame->sample_rate;
        int channels = frame->ch_layout.nb_channels;
        AVSampleFormat sfmt = static_cast<AVSampleFormat>(frame->format);
        int nb_samples = frame->nb_samples;

        int bytes_per_sample = av_get_bytes_per_sample(sfmt);
        bool is_planar_audio = av_sample_fmt_is_planar(sfmt);

        size_t totalSize = 0;
        if (is_planar_audio) {
            for (int ch = 0; ch < channels; ch++) {
                totalSize += static_cast<size_t>(bytes_per_sample * nb_samples);
            }
        } else {
            totalSize = static_cast<size_t>(bytes_per_sample * nb_samples * channels);
        }

        buf->data = new uint8_t[totalSize];
        buf->size = totalSize;

        size_t offset = 0;
        if (is_planar_audio) {
            for (int ch = 0; ch < channels; ch++) {
                size_t chSize = static_cast<size_t>(bytes_per_sample * nb_samples);
                memcpy(buf->data + offset, frame->data[ch], chSize);
                offset += chSize;
            }
        } else {
            memcpy(buf->data, frame->data[0], totalSize);
        }

        buf->pts = av_rescale_q(frame->pts, time_base, {1, 1000000});

        // 音频帧 duration = nb_samples / sample_rate
        if (sample_rate > 0 && nb_samples > 0) {
            buf->duration = av_rescale_q(nb_samples, {1, sample_rate}, {1, 1000000});
        } else {
            buf->duration = 0;
        }
        buf->media_type = MediaType::AUDIO_RAW;

        AudioRawMeta meta;
        meta.sample_rate = sample_rate;
        meta.channels = channels;
        meta.sample_fmt = sfmt;
        buf->meta = meta;
    } else {
        // 不支持的类型
        delete buf;
        return nullptr;
    }

    return buf;
}

} // namespace pipeline
