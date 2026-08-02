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
        case MuxFormat::MP4:
            // 容器实例可服务任意数量 encoded 输入；视频/音频 codec 兼容性和 stream 参数
            // 在各 Pad 首份完整 Caps 到达时由 addStream() 做格式相关验证
            // MuxFormat::MP4 固定 fMP4：视频 H.264/HEVC，音频 AAC
            return true;
    }
    return false;
}

void AVMuxNode::resetAvioTail() {
    avio_tail_seek_enabled_ = false;
    avio_committed_ = 0;
    avio_pos_ = 0;
    avio_end_ = 0;
    avio_tail_.clear();
}

int AVMuxNode::writeAvioTail(const uint8_t* data, int size) {
    if (size < 0 || (size > 0 && !data)) {
        return AVERROR(EINVAL);
    }
    if (size == 0) {
        return 0;
    }
    if (avio_pos_ < avio_committed_) {
        // 已交给 pending/Route 的前缀不能再改；说明 commit 过早或 seek 越界
        postMessage(MessageType::ERROR,
                    "AVMuxNode: fMP4 AVIO attempted to rewrite committed container prefix");
        return AVERROR(ESPIPE);
    }

    // 把绝对位置转化成 tail 内下标
    const int64_t offset_in_tail = avio_pos_ - avio_committed_;
    if (offset_in_tail > static_cast<int64_t>(avio_tail_.size())) {
        postMessage(MessageType::ERROR, "AVMuxNode: fMP4 AVIO write position past tail end");
        return AVERROR(EINVAL);
    }

    // 需要的 tail 容量
    const int64_t need = offset_in_tail + static_cast<int64_t>(size);
    if (need < 0 || need > static_cast<int64_t>(avio_tail_.max_size())) {
        postMessage(MessageType::ERROR, "AVMuxNode: fMP4 AVIO tail size overflow");
        return AVERROR(EINVAL);
    }
    if (need > static_cast<int64_t>(avio_tail_.size())) {
        avio_tail_.resize(static_cast<size_t>(need));
    }

    // FFmpeg 只在本 callback 期间拥有 data，立刻落入节点自有 tail
    std::memcpy(avio_tail_.data() + static_cast<size_t>(offset_in_tail), data,
                static_cast<size_t>(size));
    avio_pos_ += size;
    if (avio_pos_ > avio_end_) {
        avio_end_ = avio_pos_;
    }
    // tail 逻辑长度始终覆盖 [committed, end)
    if (avio_end_ - avio_committed_ != static_cast<int64_t>(avio_tail_.size())) {
        // 允许在 end 之前覆写；若 write 扩展了 end，size 已在上方 resize
        // 若 seek 回写未延伸 end，保持 tail.size 与 end-committed 一致
        const int64_t tail_len = avio_end_ - avio_committed_;
        if (tail_len >= 0 && tail_len != static_cast<int64_t>(avio_tail_.size())) {
            avio_tail_.resize(static_cast<size_t>(tail_len));
        }
    }
    return size;
}

int64_t AVMuxNode::seekAvioTail(int64_t offset, int whence) {
    if (whence == AVSEEK_SIZE) {
        return avio_end_;
    }

    int64_t target = 0;
    switch (whence) {
        case SEEK_SET:
            target = offset;
            break;
        case SEEK_CUR:
            target = avio_pos_ + offset;
            break;
        case SEEK_END:
            target = avio_end_ + offset;
            break;
        default:
            return AVERROR(EINVAL);
    }

    if (target < 0) {
        return AVERROR(EINVAL);
    }
    if (target < avio_committed_) {
        // 不能 seek 到已 commit 前缀；fMP4 只应回写当前未提交 fragment 元数据
        postMessage(MessageType::ERROR,
                    "AVMuxNode: fMP4 AVIO seek into committed container prefix");
        return AVERROR(ESPIPE);
    }
    if (target > avio_end_) {
        // 不支持在逻辑 EOF 之后制造稀疏空洞
        postMessage(MessageType::ERROR,
                    "AVMuxNode: fMP4 AVIO seek past current logical end");
        return AVERROR(EINVAL);
    }

    avio_pos_ = target;
    return avio_pos_;
}

bool AVMuxNode::commitAvioTail() {
    if (!avio_tail_seek_enabled_) {
        return true;
    }
    if (avio_pos_ != avio_end_) {
        // 仍停在 tail 中部时不能提交，否则后续 size 回填会打到已发出的前缀
        return true;
    }
    if (avio_tail_.empty()) {
        return true;
    }

    // 游标在逻辑末尾：此前 fragment 的 moof size 回填已完成，整段 tail 可顺序放出
    if (!appendContainerBytes(avio_tail_.data(), avio_tail_.size())) {
        return false;
    }
    avio_committed_ += static_cast<int64_t>(avio_tail_.size());
    avio_tail_.clear();
    return true;
}

int AVMuxNode::writeCallback(void* opaque, const uint8_t* data, int size) {
    auto* self = static_cast<AVMuxNode*>(opaque);
    if (!self || size < 0 || (size > 0 && !data)) {
        return AVERROR(EINVAL);
    }
    if (size == 0) {
        return 0;
    }

    if (self->avio_tail_seek_enabled_) {
        return self->writeAvioTail(data, size);
    }

    // MPEG-TS/FLV：FFmpeg 只在本 callback 期间拥有 data，必须立刻深拷贝到基类 pending
    return self->appendContainerBytes(data, static_cast<size_t>(size))
        ? size
        : AVERROR_EXTERNAL;
}

int64_t AVMuxNode::seekCallback(void* opaque, int64_t offset, int whence) {
    auto* self = static_cast<AVMuxNode*>(opaque);
    if (!self || !self->avio_tail_seek_enabled_) {
        return AVERROR(ESPIPE);
    }
    return self->seekAvioTail(offset, whence);
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
            // 项目中 MP4 固定表示 fragmented MP4；muxer 字符串仍为 "mp4"，由 movflags 强制 fMP4
            muxer_name = "mp4";
            break;
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

    resetAvioTail();
    // fMP4 需要 seek 回填 moof/traf size；其它格式保持无 seek 顺序写出
    const bool enable_tail_seek = (format == MuxFormat::MP4);
    avio_ctx_ = avio_alloc_context(
        avio_buffer, kAvioBufferSize, 1, this, nullptr, &AVMuxNode::writeCallback,
        enable_tail_seek ? &AVMuxNode::seekCallback : nullptr);
    if (!avio_ctx_) {
        av_free(avio_buffer);
        postMessage(MessageType::ERROR, "AVMuxNode: avio_alloc_context failed");
        return false;
    }

    if (enable_tail_seek) {
        // 仅声明对未 commit tail 可 seek；已发出的 CONTAINER 前缀仍不可改
        avio_ctx_->seekable = AVIO_SEEKABLE_NORMAL;
        avio_tail_seek_enabled_ = true;
    } else {
        avio_ctx_->seekable = 0;
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
        // MPEG-TS / fMP4 视频均接受 H.264 与 HEVC；尺寸与 extradata 已在上方校验
    } else {
        // FLV 与 fMP4 音频均要求 AAC；MPEG-TS 当前同样只声明 AAC 兼容性
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
    // fMP4：仅在逻辑写指针回到末尾时提交 tail，保证 moof size 回填已落在未提交区
    return commitAvioTail();
}

bool AVMuxNode::writeHeader(MuxFormat format) {
    if ((format != MuxFormat::MPEGTS && format != MuxFormat::FLV && format != MuxFormat::MP4) ||
        format != backend_format_ || !fmt_ctx_) {
        postMessage(MessageType::ERROR,
                    "AVMuxNode: output context is unavailable or does not match header format");
        return false;
    }

    AVDictionary* options = nullptr;
    if (format == MuxFormat::FLV) {
        // custom AVIO 是严格顺序输出，不能让 FLV muxer 在 Trailer 时 seek 回 Header 更新时长/文件大小
        av_dict_set(&options, "flvflags", "no_duration_filesize", 0);
    } else if (format == MuxFormat::MP4) {
        // empty_moov 先发可流式 moov；frag_keyframe 按关键帧切 fragment
        // default_base_moof 让 sample 偏移相对 moof，避免依赖全局绝对文件偏移回写
        av_dict_set(&options, "movflags",
                    "frag_keyframe+empty_moov+default_base_moof", 0);
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
    resetAvioTail();
}

} // namespace pipeline
