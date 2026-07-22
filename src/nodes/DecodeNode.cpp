#include "pipeline/nodes/DecodeNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Edge.h"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavutil/channel_layout.h>
#include <libavutil/error.h>
#include <libavutil/imgutils.h>
}

#include <climits>
#include <utility>

namespace pipeline {

// ===================================================================
// 构造函数
// ===================================================================
DecodeNode::DecodeNode(const std::string& name)
    : TransformNode(name) {
    addSinkPad("in", TemplateCaps{{MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED}});
}

// ===================================================================
// onStreamInfo: 严格按设计文档 §8.2 参考实现
//
//   1. receiveCapsEvent("in")
//   2. avcodec_find_decoder + avcodec_alloc_context3 + 填充 extradata + avcodec_open2
//   3. 从 ctx 读取输出参数
//   4. resize SrcPad Queue → sendCapsEvent
// ===================================================================
bool DecodeNode::onStreamInfo() {
    // 1. 用 receiveCapsEvent 从 SinkPad Queue 取 CapsEvent
    if (!receiveCapsEvent("in")) {
        return false;
    }
    const CapsEvent& in_caps = negotiated_caps_["in"];

    // 2. 打开解码器
    codec_ = avcodec_find_decoder(in_caps.codec_id);
    if (!codec_) {
        postMessage(MessageType::ERROR, "DecodeNode: decoder not found for codec_id");
        return false;
    }

    ctx_ = avcodec_alloc_context3(codec_);
    if (!ctx_) {
        postMessage(MessageType::ERROR, "DecodeNode: avcodec_alloc_context3 failed");
        return false;
    }

    // 将输入参数填入 ctx（avcodec_open2 需要这些输入提示）
    ctx_->codec_id = in_caps.codec_id;
    if (in_caps.media_type == MediaType::VIDEO_ENCODED) {
        ctx_->codec_type = AVMEDIA_TYPE_VIDEO;
        ctx_->width      = in_caps.width;
        ctx_->height     = in_caps.height;
    } else {
        ctx_->codec_type  = AVMEDIA_TYPE_AUDIO;
        ctx_->sample_rate = in_caps.sample_rate;
        av_channel_layout_default(&ctx_->ch_layout, in_caps.channels);
    }

    // 填充 extradata（SPS/PPS / AudioSpecificConfig 等）
    if (!in_caps.extradata.empty()) {
        ctx_->extradata = static_cast<uint8_t*>(
            av_mallocz(in_caps.extradata.size() + AV_INPUT_BUFFER_PADDING_SIZE));
        if (!ctx_->extradata) {
            postMessage(MessageType::ERROR, "DecodeNode: extradata alloc failed");
            return false;
        }
        memcpy(ctx_->extradata, in_caps.extradata.data(), in_caps.extradata.size());
        ctx_->extradata_size = static_cast<int>(in_caps.extradata.size());
    }

    if (avcodec_open2(ctx_, codec_, nullptr) < 0) {
        postMessage(MessageType::ERROR, "DecodeNode: avcodec_open2 failed");
        return false;
    }
    fprintf(stderr, "[%s] decoder opened: pix_fmt=%d w=%d h=%d sr=%d ch=%d\n",
            name_.c_str(), ctx_->pix_fmt, ctx_->width, ctx_->height,
            ctx_->sample_rate, ctx_->ch_layout.nb_channels);

    // 3. 从 ctx 读取输出参数（必须等 avcodec_open2 完成后）
    //    输出参数由解码器决定，不能直接透传输入 CapsEvent 的值
    is_video_ = (in_caps.media_type == MediaType::VIDEO_ENCODED);

    CapsEvent out_caps;
    if (is_video_) {
        out_caps.media_type = MediaType::VIDEO_RAW;
        out_caps.width      = ctx_->width;
        out_caps.height     = ctx_->height;
        out_caps.pix_fmt    = ctx_->pix_fmt;
        framerate_          = in_caps.framerate;
    } else {
        out_caps.media_type  = MediaType::AUDIO_RAW;
        out_caps.sample_rate = ctx_->sample_rate;
        out_caps.channels    = ctx_->ch_layout.nb_channels;
        out_caps.sample_fmt  = ctx_->sample_fmt;
    }

    // 4. 调整逻辑输出 Route 容量，再只发布一次 CapsEvent。
    auto* output_pad = getSrcPad("out_0");
    if (!output_pad || !output_pad->isConnected() || !output_pad->route()) {
        postMessage(MessageType::ERROR, "DecodeNode: output route is not connected");
        return false;
    }
    output_pad->route()->resize(selectRouteCapacity(out_caps.media_type));
    if (!sendCapsEvent("out_0", out_caps)) {
        return false;
    }
    fprintf(stderr, "[%s] CapsEvent sent: type=%d\n",
            name_.c_str(), static_cast<int>(out_caps.media_type));
    return true;
}

// ===================================================================
// process: send_packet → receive_frame 循环
// ===================================================================
void DecodeNode::process(const Buffer* input, std::vector<BufferRef>& outputs) {
    AVPacket* pkt = toAVPacket(input);
    if (!pkt) {
        postMessage(MessageType::ERROR, "DecodeNode: toAVPacket failed");
        return;
    }

    int ret = avcodec_send_packet(ctx_, pkt);
    av_packet_free(&pkt);
    if (ret < 0) {
        postMessage(MessageType::ERROR, "DecodeNode: avcodec_send_packet failed");
        return;
    }

    // 循环 receive_frame，一个 packet 可能产出多个 frame
    while (true) {
        AVFrame* frame = av_frame_alloc();
        if (!frame) {
            postMessage(MessageType::ERROR, "DecodeNode: av_frame_alloc failed");
            return;
        }

        ret = avcodec_receive_frame(ctx_, frame);
        if (ret == AVERROR(EAGAIN)) {
            av_frame_free(&frame);
            break;  // 需要更多输入数据
        }
        if (ret < 0) {
            av_frame_free(&frame);
            if (ret != AVERROR_EOF) {
                postMessage(MessageType::ERROR, "DecodeNode: avcodec_receive_frame failed");
            }
            break;
        }

        MediaType type = is_video_ ? MediaType::VIDEO_RAW : MediaType::AUDIO_RAW;
        // 解码后 frame->pts 已在 AV_TIME_BASE（微秒）域
        Buffer* buf = Buffer::fromAVFrame(frame, type, {1, 1000000}, framerate_);
        av_frame_free(&frame);

        if (!buf) {
            postMessage(MessageType::ERROR, "DecodeNode: Buffer::fromAVFrame failed");
            break;
        }
        // 新解码帧离开工厂后立即由 BufferRef 持有，停止或发布失败时由 Transform RAII 清理。
        outputs.emplace_back(buf);
    }
}

// ===================================================================
// onEvent: EOS 时 flush 解码器缓冲区，再向下游传播 EOS
// ===================================================================
void DecodeNode::onEvent(const Event& event) {
    if (std::holds_alternative<EOSEvent>(event)) {
        if (!flushed_) {
            flushed_ = true;

            // flush 解码器
            avcodec_send_packet(ctx_, nullptr);

            // 循环接收缓冲区中剩余的帧
            while (true) {
                AVFrame* frame = av_frame_alloc();
                if (!frame) {
                    break;
                }

                int ret = avcodec_receive_frame(ctx_, frame);
                if (ret < 0) {
                    av_frame_free(&frame);
                    break;
                }

                MediaType type = is_video_ ? MediaType::VIDEO_RAW : MediaType::AUDIO_RAW;
                BufferRef output(Buffer::fromAVFrame(
                    frame, type, {1, 1000000}, framerate_));
                av_frame_free(&frame);

                if (!output) {
                    break;
                }
                // EOS flush 产出的帧也使用同一移动所有权发布边界。
                if (!pushToDownstream(std::move(output))) {
                    return;
                }
            }
        }

        sendEOSDownstream();
        return;
    }

    postMessage(MessageType::ERROR,
                "CapsEvent received in runLoop; decode nodes must consume CapsEvent in onStreamInfo");
}

// ===================================================================
// onStop: 释放解码器资源
// ===================================================================
void DecodeNode::onStop() {
    if (ctx_) {
        avcodec_free_context(&ctx_);
    }
    codec_   = nullptr;
    flushed_ = false;
}

// ===================================================================
// toAVPacket: 将 Buffer 数据转为 AVPacket（深拷贝 payload）
// ===================================================================
AVPacket* DecodeNode::toAVPacket(const Buffer* buf) {
    if (!buf || buf->size > static_cast<size_t>(INT_MAX)) {
        return nullptr;
    }

    AVPacket* pkt = av_packet_alloc();
    if (!pkt) return nullptr;

    if (buf->size > 0) {
        if (!buf->data || av_new_packet(pkt, static_cast<int>(buf->size)) < 0) {
            av_packet_free(&pkt);
            return nullptr;
        }
        memcpy(pkt->data, buf->data, buf->size);
    }

    pkt->pts      = buf->pts;
    pkt->dts      = buf->dts;
    pkt->duration = buf->duration;

    if (std::holds_alternative<EncodedMeta>(buf->meta)) {
        pkt->flags = std::get<EncodedMeta>(buf->meta).flags;
    }

    return pkt;
}

} // namespace pipeline
