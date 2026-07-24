#include "pipeline/nodes/DecodeNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Edge.h"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavutil/channel_layout.h>
#include <libavutil/error.h>
}

#include <climits>
#include <utility>

namespace pipeline {

DecodeNode::DecodeNode(const std::string& name)
    : TransformNode(name) {
    addSinkPad("in", TemplateCaps{{MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED}});
}

bool DecodeNode::configureDecoder(const CapsEvent& caps) {
    if (caps.media_type != MediaType::VIDEO_ENCODED &&
        caps.media_type != MediaType::AUDIO_ENCODED) {
        postMessage(MessageType::ERROR, "DecodeNode: received non-encoded CapsEvent");
        return false;
    }

    // 根据 codec_id 找 FFmpeg Decoder
    codec_ = avcodec_find_decoder(caps.codec_id);
    if (!codec_) {
        postMessage(MessageType::ERROR, "DecodeNode: decoder not found for codec_id");
        return false;
    }

    // 分配全新的 AVCodecContext
    ctx_ = avcodec_alloc_context3(codec_);
    if (!ctx_) {
        postMessage(MessageType::ERROR, "DecodeNode: avcodec_alloc_context3 failed");
        return false;
    }

    ctx_->codec_id = caps.codec_id;
    // 框架 encoded Buffer 的时间戳统一为微秒，送入 decoder 时必须使用相同时间基。
    ctx_->pkt_timebase = AVRational{1, 1000000};

    is_video_ = caps.media_type == MediaType::VIDEO_ENCODED;
    if (is_video_) {
        ctx_->codec_type = AVMEDIA_TYPE_VIDEO;
        ctx_->width = caps.width;
        ctx_->height = caps.height;
        framerate_ = caps.framerate;
    } else {
        ctx_->codec_type = AVMEDIA_TYPE_AUDIO;

        // encoded 音频 Caps 只需 codec_id 便可选择 decoder
        // sample_rate 和 channel_layout 都不是 Decode 选择 decoder 的硬前提
        // 如果 Caps 提供了有效 sample_rate 就写入 AVCodecContext
        if (caps.sample_rate > 0) {
            ctx_->sample_rate = caps.sample_rate;
        }

        // 如果 Caps 中带着一个框架认可的、结构自洽的声道布局值
        if (caps.channel_layout.isValid()) {
            // 先构造临时 AVChannelLayout，因为 Caps 里保存的是自己的值类型，不能直接赋给 FFmpeg
            AVChannelLayout input_layout{};

            // toAV() 失败表示这份框架 ChannelLayout 不能转换成当前 FFmpeg 可接受的 AVChannelLayout
            if (!caps.channel_layout.toAV(&input_layout) ||
                av_channel_layout_copy(&ctx_->ch_layout, &input_layout) < 0)
            {
                av_channel_layout_uninit(&input_layout);
                postMessage(MessageType::ERROR, "DecodeNode: invalid encoded audio channel layout");
                return false;
            }

            // 成功了也直接uninit，因为已经完成深拷贝，它不再需要保留
            av_channel_layout_uninit(&input_layout);
        }
    }

    // 复制 extradata，并补齐 FFmpeg padding
    if (!caps.extradata.empty()) {
        ctx_->extradata = static_cast<uint8_t*>(
            av_mallocz(caps.extradata.size() + AV_INPUT_BUFFER_PADDING_SIZE));
        if (!ctx_->extradata) {
            postMessage(MessageType::ERROR, "DecodeNode: extradata alloc failed");
            return false;
        }
        memcpy(ctx_->extradata, caps.extradata.data(), caps.extradata.size());
        ctx_->extradata_size = static_cast<int>(caps.extradata.size());
    }

    // 真正打开 Decoder
    if (avcodec_open2(ctx_, codec_, nullptr) < 0) {
        postMessage(MessageType::ERROR, "DecodeNode: avcodec_open2 failed");
        return false;
    }

    // 打开成功不代表已经知道 RAW 输出格式；只能由真实 AVFrame 定案后再发布。
    flushed_ = false;
    fprintf(stderr, "[%s] decoder opened: codec=%d\n", name_.c_str(), caps.codec_id);
    return true;
}

// DecodeNode 接收到新的 CapsEvent，处理旧 Decoder 收尾并配置新 Decoder 的钩子
bool DecodeNode::onCaps(const std::string&, const CapsEvent& caps,
                        std::vector<QueueItem>* outputs) {
    if (!outputs) {
        postMessage(MessageType::ERROR, "DecodeNode: Caps application requires an output sequence");
        return false;
    }

    // 新 encoded Caps 不能越过旧 decoder 尚未取出的延迟帧
    // 必须把它们加入同一有序输出序列，再 ack 当前输入 Caps 并替换 decoder context
    if (ctx_ && !flushed_) {
        // 向 FFmpeg 发送空 Packet 开始旧 Decoder flush
        if (avcodec_send_packet(ctx_, nullptr) < 0 || !drainDecoder(*outputs)) {
            return false;
        }
        flushed_ = true;
    }

    // 释放旧 Decoder context
    if (ctx_) {
        avcodec_free_context(&ctx_);
    }
    codec_ = nullptr;

    // 配置新的 Decoder
    return configureDecoder(caps);
}

bool DecodeNode::appendOutputCapsForFrame(const AVFrame* frame,
                                          std::vector<QueueItem>& outputs) {
    CapsEvent caps;
    if (is_video_) {
        caps.media_type = MediaType::VIDEO_RAW;
        caps.width = frame->width;
        caps.height = frame->height;
        caps.pix_fmt = static_cast<AVPixelFormat>(frame->format);
    } else {
        caps.media_type = MediaType::AUDIO_RAW;
        caps.sample_rate = frame->sample_rate;
        caps.sample_fmt = static_cast<AVSampleFormat>(frame->format);
        if (!ChannelLayout::fromAV(frame->ch_layout, &caps.channel_layout)) {
            postMessage(MessageType::ERROR, "DecodeNode: invalid decoded audio channel layout");
            return false;
        }
    }

    // 生产侧自检：Decoder 只对自己产出的这一种 RAW 类型负责，校验它据以建 Buffer 的字段是否成立。
    const bool raw_caps_usable =
        is_video_ ? (caps.width > 0 && caps.height > 0 && caps.pix_fmt != AV_PIX_FMT_NONE)
                  : (caps.sample_rate > 0 && caps.sample_fmt != AV_SAMPLE_FMT_NONE &&
                     caps.channel_layout.isValid());
    if (!raw_caps_usable) {
        postMessage(MessageType::ERROR, "DecodeNode: decoded frame does not define usable raw Caps");
        return false;
    }

    if (output_caps_ && output_caps_->hasSameFormat(caps)) {
        return true;
    }

    // Queue this Route control event before its first Buffer. TransformNode publishes QueueItem strictly in order.
    outputs.emplace_back(Event{caps});
    output_caps_ = caps;
    return true;
}

bool DecodeNode::appendFrame(AVFrame* frame, std::vector<QueueItem>& outputs) {
    if (!appendOutputCapsForFrame(frame, outputs)) {
        return false;
    }

    const MediaType type = is_video_ ? MediaType::VIDEO_RAW : MediaType::AUDIO_RAW;
    BufferRef output(Buffer::fromAVFrame(frame, type, {1, 1000000}, framerate_));
    if (!output) {
        postMessage(MessageType::ERROR, "DecodeNode: Buffer::fromAVFrame failed");
        return false;
    }

    outputs.emplace_back(std::move(output));
    return true;
}

bool DecodeNode::drainDecoder(std::vector<QueueItem>& outputs) {
    while (true) {
        AVFrame* frame = av_frame_alloc();
        if (!frame) {
            postMessage(MessageType::ERROR, "DecodeNode: av_frame_alloc failed");
            return false;
        }

        const int ret = avcodec_receive_frame(ctx_, frame);
        if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF) {
            av_frame_free(&frame);
            return true;
        }
        if (ret < 0) {
            av_frame_free(&frame);
            postMessage(MessageType::ERROR, "DecodeNode: avcodec_receive_frame failed");
            return false;
        }

        const bool appended = appendFrame(frame, outputs);
        av_frame_free(&frame);
        if (!appended) {
            return false;
        }
    }
}

void DecodeNode::process(const Buffer* input, std::vector<QueueItem>& outputs) {
    if (!ctx_) {
        postMessage(MessageType::ERROR, "DecodeNode: input Buffer received without configured decoder");
        return;
    }

    AVPacket* packet = toAVPacket(input);
    if (!packet) {
        postMessage(MessageType::ERROR, "DecodeNode: toAVPacket failed");
        return;
    }

    const int ret = avcodec_send_packet(ctx_, packet);
    av_packet_free(&packet);
    if (ret < 0) {
        postMessage(MessageType::ERROR, "DecodeNode: avcodec_send_packet failed");
        return;
    }

    drainDecoder(outputs);
}

void DecodeNode::onEOS(std::vector<QueueItem>& outputs) {
    if (!ctx_ || flushed_) {
        return;
    }

    if (avcodec_send_packet(ctx_, nullptr) < 0) {
        postMessage(MessageType::ERROR, "DecodeNode: avcodec_send_packet flush failed");
        return;
    }
    if (!drainDecoder(outputs)) {
        return;
    }
    flushed_ = true;
}

void DecodeNode::onStop() {
    if (ctx_) {
        avcodec_free_context(&ctx_);
    }
    codec_ = nullptr;
    output_caps_.reset();
    flushed_ = false;
}

AVPacket* DecodeNode::toAVPacket(const Buffer* buf) {
    if (!buf || buf->size > static_cast<size_t>(INT_MAX)) {
        return nullptr;
    }

    AVPacket* packet = av_packet_alloc();
    if (!packet) {
        return nullptr;
    }

    if (buf->size > 0) {
        if (!buf->data || av_new_packet(packet, static_cast<int>(buf->size)) < 0) {
            av_packet_free(&packet);
            return nullptr;
        }
        memcpy(packet->data, buf->data, buf->size);
    }

    packet->pts = buf->pts;
    packet->dts = buf->dts;
    packet->duration = buf->duration;
    if (const auto* meta = std::get_if<EncodedMeta>(&buf->meta)) {
        packet->flags = meta->flags;
    }
    return packet;
}

} // namespace pipeline
