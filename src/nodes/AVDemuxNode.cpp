#include "pipeline/nodes/AVDemuxNode.h"
#include "pipeline/core/Buffer.h"

extern "C" {
#include <libavutil/error.h>
}

namespace pipeline {

// ===================================================================
// 辅助函数：将 codecpar->extradata 深拷贝到 vector
// ===================================================================
static void copyExtradata(const AVCodecParameters* par, std::vector<uint8_t>& out) {
    if (par->extradata && par->extradata_size > 0) {
        out.assign(par->extradata, par->extradata + par->extradata_size);
    }
}

// ===================================================================
// 辅助函数：FFmpeg 错误码转字符串
// ===================================================================
static std::string avErrorStr(int err) {
    char buf[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(err, buf, sizeof(buf));
    return std::string(buf);
}

// ===================================================================
// openInput: avformat_open_input
// ===================================================================
bool AVDemuxNode::openInput(const std::string& url) {
    fprintf(stderr, "[%s] openInput: %s\n", name_.c_str(), url.c_str());
    int ret = avformat_open_input(&fmt_ctx_, url.c_str(), nullptr, nullptr);
    if (ret < 0) {
        postMessage(MessageType::ERROR,
                    "AVDemuxNode: avformat_open_input failed: " + avErrorStr(ret), ret);
        return false;
    }
    fprintf(stderr, "[%s] openInput OK\n", name_.c_str());
    return true;
}

// ===================================================================
// probeStreams: avformat_find_stream_info + av_find_best_stream
//
// 按设计文档 §5.5.2，各取一路最佳视频和一路最佳音频，
// 通过 DemuxProbeResult 显式返回。无关流不暴露给基类。
// ===================================================================
bool AVDemuxNode::probeStreams(DemuxProbeResult* result) {
    int ret = avformat_find_stream_info(fmt_ctx_, nullptr);
    if (ret < 0) {
        postMessage(MessageType::ERROR,
                    "AVDemuxNode: avformat_find_stream_info failed: " + avErrorStr(ret), ret);
        return false;
    }

    // 只探测用户实际请求的流（通过已创建的 SrcPad 判断）
    bool need_video = false;
    bool need_audio = false;
    for (const auto& pad : srcPads()) {
        if (pad->templateCaps().contains(MediaType::VIDEO_ENCODED)) need_video = true;
        if (pad->templateCaps().contains(MediaType::AUDIO_ENCODED)) need_audio = true;
    }

    if (need_video) {
        video_stream_idx_ = av_find_best_stream(
            fmt_ctx_, AVMEDIA_TYPE_VIDEO, -1, -1, nullptr, 0);
        if (video_stream_idx_ >= 0) {
            AVStream* st = fmt_ctx_->streams[video_stream_idx_];
            CapsEvent caps;
            caps.media_type = MediaType::VIDEO_ENCODED;
            caps.codec_id   = st->codecpar->codec_id;
            caps.width      = st->codecpar->width;
            caps.height     = st->codecpar->height;
            caps.framerate  = av_guess_frame_rate(fmt_ctx_, st, nullptr);
            copyExtradata(st->codecpar, caps.extradata);
            result->video = std::move(caps);
            fprintf(stderr, "[%s] video stream #%d: %dx%d codec=%d\n",
                    name_.c_str(), video_stream_idx_,
                    st->codecpar->width, st->codecpar->height, st->codecpar->codec_id);
        }
    }

    if (need_audio) {
        audio_stream_idx_ = av_find_best_stream(
            fmt_ctx_, AVMEDIA_TYPE_AUDIO, -1, -1, nullptr, 0);
        if (audio_stream_idx_ >= 0) {
            AVStream* st = fmt_ctx_->streams[audio_stream_idx_];
            CapsEvent caps;
            caps.media_type  = MediaType::AUDIO_ENCODED;
            caps.codec_id    = st->codecpar->codec_id;
            caps.sample_rate = st->codecpar->sample_rate;
            caps.channels    = st->codecpar->ch_layout.nb_channels;
            copyExtradata(st->codecpar, caps.extradata);
            result->audio = std::move(caps);
            fprintf(stderr, "[%s] audio stream #%d: %dHz %dch codec=%d\n",
                    name_.c_str(), audio_stream_idx_,
                    st->codecpar->sample_rate, st->codecpar->ch_layout.nb_channels,
                    st->codecpar->codec_id);
        }
    }

    return true;
}

// ===================================================================
// readFrame: av_read_frame，按 stream_index 分发
//
// 无关流（subtitle、未选 track 等）在内部循环跳过，不返回 SKIP。
// 按设计文档 §5.5.2，循环直到找到匹配的流或遇到终止条件。
// ===================================================================
DemuxReadResult AVDemuxNode::readFrame() {
    AVPacket* pkt = av_packet_alloc();
    if (!pkt) {
        postMessage(MessageType::ERROR, "AVDemuxNode: av_packet_alloc failed");
        return {DemuxReadStatus::ERROR, BufferRef{nullptr}};
    }

    while (true) {
        int ret = av_read_frame(fmt_ctx_, pkt);
        if (ret == AVERROR_EOF) {
            av_packet_free(&pkt);
            return {DemuxReadStatus::END_OF_STREAM, BufferRef{nullptr}};
        }
        if (ret < 0) {
            av_packet_free(&pkt);
            // interrupt_requested_ 检查留给后续网络 I/O 中断实现
            postMessage(MessageType::ERROR,
                        "AVDemuxNode: av_read_frame failed: " + avErrorStr(ret), ret);
            return {DemuxReadStatus::ERROR, BufferRef{nullptr}};
        }

        // 按 stream_index 判断类型，无关流在内部跳过
        MediaType type;
        if (pkt->stream_index == video_stream_idx_) {
            type = MediaType::VIDEO_ENCODED;
        } else if (pkt->stream_index == audio_stream_idx_) {
            type = MediaType::AUDIO_ENCODED;
        } else {
            av_packet_unref(pkt);
            continue;
        }

        AVStream* st = fmt_ctx_->streams[pkt->stream_index];
        Buffer* buffer = Buffer::fromAVPacket(
            pkt, type, st->time_base, st->codecpar->codec_id);
        av_packet_unref(pkt);
        av_packet_free(&pkt);

        if (!buffer) {
            postMessage(MessageType::ERROR, "AVDemuxNode: Buffer::fromAVPacket failed");
            return {DemuxReadStatus::ERROR, BufferRef{nullptr}};
        }
        return {DemuxReadStatus::BUFFER, BufferRef{buffer}};
    }
}

// ===================================================================
// closeInput: avformat_close_input
//
// 必须支持部分初始化状态：avformat_close_input 接受 nullptr 调用。
// ===================================================================
void AVDemuxNode::closeInput() {
    if (fmt_ctx_) {
        avformat_close_input(&fmt_ctx_);
    }
}

} // namespace pipeline
