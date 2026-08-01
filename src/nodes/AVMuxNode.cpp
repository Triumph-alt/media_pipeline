#include "pipeline/nodes/AVMuxNode.h"

extern "C" {
#include <libavcodec/packet.h>
#include <libavformat/avformat.h>
#include <libavutil/error.h>
#include <libavutil/mathematics.h>
#include <libavutil/mem.h>
}

#include <cstring>
#include <limits>
#include <string>

namespace pipeline {
namespace {

constexpr AVRational kFrameworkTimeBase{1, 1000000};
constexpr int kAvioBufferSize = 4096;

std::string avErrorStr(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
}

int64_t rescaleFromFrameworkTimeBase(int64_t value, AVRational target_time_base) {
    if (value == AV_NOPTS_VALUE) {
        return AV_NOPTS_VALUE;
    }
    return av_rescale_q(value, kFrameworkTimeBase, target_time_base);
}

} // namespace

bool AVMuxNode::acceptsInputPad(MuxFormat format, MediaType hint_type) const {
    if (hint_type != MediaType::VIDEO_ENCODED && hint_type != MediaType::AUDIO_ENCODED) {
        return false;
    }

    switch (format) {
        case MuxFormat::MPEGTS:
        case MuxFormat::FLV:
            // 容器实例可服务任意数量 encoded 输入；视频/音频 codec 兼容性和 stream 参数
            // 在各 Pad 首份完整 Caps 到达时由 addStream() 做格式相关验证
            return true;
        case MuxFormat::MP4:
            // fragmented MP4 后端尚未实现，link 阶段直接拒绝任何输入
            return false;
    }
    return false;
}

int AVMuxNode::writeCallback(void* opaque, const uint8_t* data, int size) {
    auto* self = static_cast<AVMuxNode*>(opaque);
    if (!self || size < 0 || (size > 0 && !data)) {
        return AVERROR(EINVAL);
    }
    if (size == 0) {
        return 0;
    }

    // FFmpeg 只在本 callback 的调用期间拥有 data，必须立刻深拷贝到基类 pending 输出
    return self->appendContainerBytes(data, static_cast<size_t>(size))
        ? size
        : AVERROR_EXTERNAL;
}

bool AVMuxNode::allocateContext(MuxFormat format) {
    const char* muxer_name = nullptr;
    switch (format) {
        case MuxFormat::MPEGTS:
            muxer_name = "mpegts";
            break;
        case MuxFormat::FLV:
            muxer_name = "flv";
            break;
        case MuxFormat::MP4:
            postMessage(MessageType::ERROR,
                        "AVMuxNode: fragmented MP4 is not implemented yet");
            return false;
    }

    int result = avformat_alloc_output_context2(&fmt_ctx_, nullptr, muxer_name, nullptr);
    if (result < 0 || !fmt_ctx_) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: avformat_alloc_output_context2 failed: " + avErrorStr(result),
                    result);
        return false;
    }

    uint8_t* avio_buffer = static_cast<uint8_t*>(av_malloc(kAvioBufferSize));
    if (!avio_buffer) {
        postMessage(MessageType::ERROR, "AVMuxNode: av_malloc AVIO buffer failed");
        return false;
    }

    avio_ctx_ = avio_alloc_context(avio_buffer, kAvioBufferSize, 1, this,
                                   nullptr, &AVMuxNode::writeCallback, nullptr);
    if (!avio_ctx_) {
        av_free(avio_buffer);
        postMessage(MessageType::ERROR, "AVMuxNode: avio_alloc_context failed");
        return false;
    }

    // 自定义 AVIO 由节点回收，FFmpeg 不得在 avformat_free_context 中关闭或释放它
    fmt_ctx_->pb = avio_ctx_;
    fmt_ctx_->flags |= AVFMT_FLAG_CUSTOM_IO;
    backend_format_ = format;
    timestamp_origin_us_ = AV_NOPTS_VALUE;
    last_dts_us_ = AV_NOPTS_VALUE;
    return true;
}

bool AVMuxNode::addStream(const CapsEvent& caps, int* stream_index) {
    if (!fmt_ctx_ || !stream_index) {
        postMessage(MessageType::ERROR, "AVMuxNode: output context is unavailable while adding stream");
        return false;
    }

    const bool is_video = caps.media_type == MediaType::VIDEO_ENCODED;
    const bool is_audio = caps.media_type == MediaType::AUDIO_ENCODED;
    if (!is_video && !is_audio) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: input Caps must describe encoded video or audio");
        return false;
    }
    if (caps.extradata.empty()) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: encoded stream Caps require non-empty extradata");
        return false;
    }
    if (caps.extradata.size() >
        static_cast<size_t>(std::numeric_limits<int>::max() - AV_INPUT_BUFFER_PADDING_SIZE)) {
        postMessage(MessageType::ERROR, "AVMuxNode: extradata exceeds FFmpeg integer limits");
        return false;
    }

    if (is_video) {
        if ((caps.codec_id != AV_CODEC_ID_H264 && caps.codec_id != AV_CODEC_ID_HEVC) ||
            caps.width <= 0 || caps.height <= 0) {
            postMessage(MessageType::ERROR,
                        "AVMuxNode: video Caps require H.264/HEVC, positive dimensions and extradata");
            return false;
        }
        if (backend_format_ == MuxFormat::FLV && caps.codec_id != AV_CODEC_ID_H264) {
            postMessage(MessageType::ERROR,
                        "AVMuxNode: FLV accepts only H.264 video input");
            return false;
        }
    } else {
        if (caps.codec_id != AV_CODEC_ID_AAC || caps.sample_rate <= 0 ||
            !caps.channel_layout.isValid()) {
            postMessage(MessageType::ERROR,
                        "AVMuxNode: audio Caps require AAC, positive sample rate, valid channel layout and extradata");
            return false;
        }
    }

    AVStream* stream = avformat_new_stream(fmt_ctx_, nullptr);
    if (!stream) {
        postMessage(MessageType::ERROR, "AVMuxNode: avformat_new_stream failed");
        return false;
    }

    AVCodecParameters* parameters = stream->codecpar;
    parameters->codec_type = is_video ? AVMEDIA_TYPE_VIDEO : AVMEDIA_TYPE_AUDIO;
    parameters->codec_id = caps.codec_id;
    stream->time_base = kFrameworkTimeBase;

    if (is_video) {
        parameters->width = caps.width;
        parameters->height = caps.height;
    } else {
        parameters->sample_rate = caps.sample_rate;
        if (!caps.channel_layout.toAV(&parameters->ch_layout)) {
            postMessage(MessageType::ERROR,
                        "AVMuxNode: cannot materialize audio channel layout for stream parameters");
            return false;
        }
    }

    const size_t extradata_size = caps.extradata.size();
    parameters->extradata = static_cast<uint8_t*>(
        av_mallocz(extradata_size + AV_INPUT_BUFFER_PADDING_SIZE));
    if (!parameters->extradata) {
        postMessage(MessageType::ERROR, "AVMuxNode: av_mallocz extradata failed");
        return false;
    }

    // codecpar 接管这份 FFmpeg 分配的存储，Caps 保持自己的可复制 vector 所有权
    std::memcpy(parameters->extradata, caps.extradata.data(), extradata_size);
    parameters->extradata_size = static_cast<int>(extradata_size);
    *stream_index = stream->index;
    return true;
}

bool AVMuxNode::flushAvio() {
    if (!avio_ctx_) {
        postMessage(MessageType::ERROR, "AVMuxNode: AVIO context is unavailable while flushing output");
        return false;
    }

    // Header、每个 Packet 和 Trailer 返回基类前都强制交出 AVIO 内部小块缓存
    avio_flush(avio_ctx_);
    if (avio_ctx_->error < 0) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: custom AVIO write failed: " + avErrorStr(avio_ctx_->error),
                    avio_ctx_->error);
        return false;
    }
    return true;
}

bool AVMuxNode::writeHeader(MuxFormat format) {
    if ((format != MuxFormat::MPEGTS && format != MuxFormat::FLV) ||
        format != backend_format_ || !fmt_ctx_) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: output context is unavailable or does not match header format");
        return false;
    }

    AVDictionary* options = nullptr;
    if (format == MuxFormat::FLV) {
        // custom AVIO 是严格顺序输出，不能让 FLV muxer 在 Trailer 时 seek 回 Header 更新时长/文件大小
        av_dict_set(&options, "flvflags", "no_duration_filesize", 0);
    }
    const int result = avformat_write_header(fmt_ctx_, &options);
    av_dict_free(&options);
    if (result < 0) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: avformat_write_header failed: " + avErrorStr(result), result);
        return false;
    }
    return flushAvio();
}

bool AVMuxNode::writePacket(const Buffer* buf, int stream_index) {
    if (!fmt_ctx_ || !buf || stream_index < 0 ||
        stream_index >= static_cast<int>(fmt_ctx_->nb_streams)) {
        postMessage(MessageType::ERROR, "AVMuxNode: invalid packet write state");
        return false;
    }
    const AVMediaType stream_type = fmt_ctx_->streams[stream_index]->codecpar->codec_type;
    const MediaType expected_media_type = stream_type == AVMEDIA_TYPE_VIDEO
        ? MediaType::VIDEO_ENCODED
        : stream_type == AVMEDIA_TYPE_AUDIO
            ? MediaType::AUDIO_ENCODED
            : MediaType::CONTAINER;
    if (buf->media_type != expected_media_type || !buf->data || buf->size == 0 ||
        !std::holds_alternative<EncodedMeta>(buf->meta)) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: encoded Buffer does not match its output stream type");
        return false;
    }
    if (buf->dts == AV_NOPTS_VALUE) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: encoded Packet requires a valid DTS after framework interleaving");
        return false;
    }
    if (last_dts_us_ != AV_NOPTS_VALUE && buf->dts < last_dts_us_) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: encoded Packet DTS regressed after framework interleaving");
        return false;
    }
    if (timestamp_origin_us_ == AV_NOPTS_VALUE) {
        timestamp_origin_us_ = buf->dts;
    }
    const int64_t relative_dts_us = buf->dts - timestamp_origin_us_;
    const int64_t relative_pts_us = buf->pts == AV_NOPTS_VALUE
        ? AV_NOPTS_VALUE
        : buf->pts - timestamp_origin_us_;
    if (relative_dts_us < 0) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: normalized Packet DTS is negative");
        return false;
    }
    if (buf->size > static_cast<size_t>(std::numeric_limits<int>::max())) {
        postMessage(MessageType::ERROR, "AVMuxNode: encoded Buffer exceeds FFmpeg packet size limits");
        return false;
    }

    AVPacket* packet = av_packet_alloc();
    if (!packet) {
        postMessage(MessageType::ERROR, "AVMuxNode: av_packet_alloc failed");
        return false;
    }
    if (av_new_packet(packet, static_cast<int>(buf->size)) < 0) {
        av_packet_free(&packet);
        postMessage(MessageType::ERROR, "AVMuxNode: av_new_packet failed");
        return false;
    }

    // Packet 必须拥有 FFmpeg 分配且带 padding 的 payload，不能借用框架 Buffer 的 new[] 内存
    std::memcpy(packet->data, buf->data, buf->size);
    packet->stream_index = stream_index;
    packet->pts = rescaleFromFrameworkTimeBase(
        relative_pts_us, fmt_ctx_->streams[stream_index]->time_base);
    packet->dts = rescaleFromFrameworkTimeBase(
        relative_dts_us, fmt_ctx_->streams[stream_index]->time_base);
    packet->duration = buf->duration > 0
        ? av_rescale_q(buf->duration, kFrameworkTimeBase,
                       fmt_ctx_->streams[stream_index]->time_base)
        : 0;
    packet->flags = std::get<EncodedMeta>(buf->meta).flags;

    // MuxNode 已在 Route 层选出全局最小 DTS，这里直写而不建立 FFmpeg 通用 interleave queue
    const int result = av_write_frame(fmt_ctx_, packet);
    av_packet_free(&packet);
    if (result < 0) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: av_write_frame failed: " + avErrorStr(result), result);
        return false;
    }
    last_dts_us_ = buf->dts;
    return flushAvio();
}

bool AVMuxNode::writeTrailer() {
    if (!fmt_ctx_) {
        postMessage(MessageType::ERROR, "AVMuxNode: output context is unavailable for trailer");
        return false;
    }

    const int result = av_write_trailer(fmt_ctx_);
    if (result < 0) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: av_write_trailer failed: " + avErrorStr(result), result);
        return false;
    }
    return flushAvio();
}

void AVMuxNode::closeContext() {
    if (fmt_ctx_) {
        fmt_ctx_->pb = nullptr;
    }
    if (avio_ctx_) {
        // avio_context_free 只释放 context 本体，当前 buffer 可能被 FFmpeg 替换，必须单独释放
        av_freep(&avio_ctx_->buffer);
        avio_context_free(&avio_ctx_);
    }
    if (fmt_ctx_) {
        avformat_free_context(fmt_ctx_);
        fmt_ctx_ = nullptr;
    }
}

} // namespace pipeline
