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

SinkPad* AVMuxNode::requestSinkPad(const std::string& name, MediaType hint_type) {
    // 首版 MPEG-TS 只接受一路视频，音频和多视频交织留给后续明确的容器范围扩展
    if (hint_type != MediaType::VIDEO_ENCODED || !sink_pads_.empty()) {
        return nullptr;
    }
    return MuxNode::requestSinkPad(name, hint_type);
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
    if (format != MuxFormat::MPEGTS) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: only MPEG-TS is implemented in the first backend revision");
        return false;
    }

    int result = avformat_alloc_output_context2(&fmt_ctx_, nullptr, "mpegts", nullptr);
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
    return true;
}

bool AVMuxNode::addStream(const CapsEvent& caps, int* stream_index) {
    if (!fmt_ctx_ || !stream_index) {
        postMessage(MessageType::ERROR, "AVMuxNode: output context is unavailable while adding stream");
        return false;
    }
    if (caps.media_type != MediaType::VIDEO_ENCODED ||
        (caps.codec_id != AV_CODEC_ID_H264 && caps.codec_id != AV_CODEC_ID_HEVC)) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: MPEG-TS first revision accepts only H.264 or HEVC video input");
        return false;
    }
    if (caps.width <= 0 || caps.height <= 0 || caps.extradata.empty()) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: MPEG-TS video Caps require width, height and non-empty extradata");
        return false;
    }
    if (caps.extradata.size() >
        static_cast<size_t>(std::numeric_limits<int>::max() - AV_INPUT_BUFFER_PADDING_SIZE)) {
        postMessage(MessageType::ERROR, "AVMuxNode: extradata exceeds FFmpeg integer limits");
        return false;
    }

    AVStream* stream = avformat_new_stream(fmt_ctx_, nullptr);
    if (!stream) {
        postMessage(MessageType::ERROR, "AVMuxNode: avformat_new_stream failed");
        return false;
    }

    AVCodecParameters* parameters = stream->codecpar;
    parameters->codec_type = AVMEDIA_TYPE_VIDEO;
    parameters->codec_id = caps.codec_id;
    parameters->width = caps.width;
    parameters->height = caps.height;
    stream->time_base = kFrameworkTimeBase;

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
    if (format != MuxFormat::MPEGTS || !fmt_ctx_) {
        postMessage(MessageType::ERROR, "AVMuxNode: MPEG-TS output context is unavailable for header");
        return false;
    }

    const int result = avformat_write_header(fmt_ctx_, nullptr);
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
    if (buf->media_type != MediaType::VIDEO_ENCODED || !buf->data || buf->size == 0 ||
        !std::holds_alternative<EncodedMeta>(buf->meta)) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: expected a non-empty VIDEO_ENCODED Buffer with EncodedMeta");
        return false;
    }
    if (buf->dts == AV_NOPTS_VALUE) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: MPEG-TS Packet requires a valid DTS after framework interleaving");
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
    packet->pts = rescaleFromFrameworkTimeBase(buf->pts, fmt_ctx_->streams[stream_index]->time_base);
    packet->dts = rescaleFromFrameworkTimeBase(buf->dts, fmt_ctx_->streams[stream_index]->time_base);
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
