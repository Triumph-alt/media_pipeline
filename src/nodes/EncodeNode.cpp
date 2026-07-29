#include "pipeline/nodes/EncodeNode.h"

#include "pipeline/core/Buffer.h"

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavcodec/codec.h>
#include <libavutil/error.h>
#include <libavutil/frame.h>
#include <libavutil/imgutils.h>
#include <libavutil/mathematics.h>
#include <libswscale/swscale.h>
}

#include <cstring>
#include <utility>

namespace pipeline {
namespace {

constexpr AVRational kFrameworkTimeBase{1, 1000000};

std::string avErrorStr(int error) {
    char buffer[AV_ERROR_MAX_STRING_SIZE]{};
    av_strerror(error, buffer, sizeof(buffer));
    return buffer;
}

} // namespace

EncodeNode::EncodeNode(const std::string& name, EncodeConfig config)
    : TransformNode(name), config_(std::move(config)) {
    addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    addSrcPad("out_0", TemplateCaps{{MediaType::VIDEO_ENCODED}});
}

bool EncodeNode::onReady() {
    if (config_.codec_name.empty()) {
        postMessage(MessageType::ERROR, "EncodeNode: codec_name must be configured at construction");
        return false;
    }
    if (config_.framerate.num <= 0 || config_.framerate.den <= 0) {
        postMessage(MessageType::ERROR, "EncodeNode: configured framerate must be positive");
        return false;
    }

    // 编码目标是节点静态配置，因此 Ready 可先查找和验证 encoder；实际 context 仍必须等待
    // Running VIDEO_RAW Caps 提供 width、height、输入 pix_fmt 后才有条件打开。
    codec_ = avcodec_find_encoder_by_name(config_.codec_name.c_str());
    if (!codec_) {
        postMessage(MessageType::ERROR,
                    "EncodeNode: encoder '" + config_.codec_name + "' was not found");
        return false;
    }
    if (!av_codec_is_encoder(codec_) || codec_->type != AVMEDIA_TYPE_VIDEO) {
        postMessage(MessageType::ERROR,
                    "EncodeNode: configured codec is not a video encoder: " + config_.codec_name);
        return false;
    }
    return true;
}

bool EncodeNode::chooseOutputPixelFormat(AVPixelFormat input_format,
                                         AVPixelFormat* output_format) const {
    if (!output_format || input_format == AV_PIX_FMT_NONE) {
        return false;
    }

    const void* raw_formats = nullptr;
    int format_count = 0;
    if (avcodec_get_supported_config(nullptr, codec_, AV_CODEC_CONFIG_PIX_FORMAT, 0,
                                     &raw_formats, &format_count) < 0) {
        return false;
    }
    if (!raw_formats || format_count == 0) {
        // 没有静态像素格式约束时，encoder 接受当前上游真实格式，无需引入转换猜测。
        *output_format = input_format;
        return true;
    }

    const auto* formats = static_cast<const AVPixelFormat*>(raw_formats);
    for (int index = 0; index < format_count; ++index) {
        if (formats[index] == input_format) {
            *output_format = input_format;
            return true;
        }
    }

    // 不支持直接输入时，稳定选择 FFmpeg 当前版本公开返回的首个格式；转换路径随后由 swscale 建立。
    *output_format = formats[0];
    return true;
}

bool EncodeNode::configureConversion(const CapsEvent& caps, AVPixelFormat output_format) {
    releaseConversion();
    if (caps.pix_fmt == output_format) {
        return true;
    }

    sws_ctx_ = sws_getContext(caps.width, caps.height, caps.pix_fmt,
                              caps.width, caps.height, output_format,
                              SWS_BILINEAR, nullptr, nullptr, nullptr);
    if (!sws_ctx_) {
        postMessage(MessageType::ERROR, "EncodeNode: sws_getContext failed for encoder conversion");
        return false;
    }
    return true;
}

bool EncodeNode::configureEncoder(const CapsEvent& caps) {
    if (!codec_) {
        postMessage(MessageType::ERROR, "EncodeNode: encoder lookup was not completed in Ready");
        return false;
    }
    if (caps.media_type != MediaType::VIDEO_RAW || caps.width <= 0 || caps.height <= 0 ||
        caps.pix_fmt == AV_PIX_FMT_NONE) {
        postMessage(MessageType::ERROR,
                    "EncodeNode: VIDEO_RAW Caps require width, height and pix_fmt");
        return false;
    }

    AVPixelFormat output_format = AV_PIX_FMT_NONE;
    if (!chooseOutputPixelFormat(caps.pix_fmt, &output_format)) {
        postMessage(MessageType::ERROR,
                    "EncodeNode: encoder does not expose a usable output pixel format");
        return false;
    }

    AVCodecContext* replacement = avcodec_alloc_context3(codec_);
    if (!replacement) {
        postMessage(MessageType::ERROR, "EncodeNode: avcodec_alloc_context3 failed");
        return false;
    }

    replacement->codec_type = AVMEDIA_TYPE_VIDEO;
    replacement->codec_id = codec_->id;
    replacement->width = caps.width;
    replacement->height = caps.height;
    replacement->pix_fmt = output_format;
    replacement->framerate = config_.framerate;
    replacement->time_base = AVRational{config_.framerate.den, config_.framerate.num};
    // 当前输出 Caps 的 configuration 以 extradata 表达；请求 global header 强制 encoder 将其
    // 与首个可发布 Packet 的 codec configuration 分离，避免把“关键帧大概率带参数集”当合同。
    replacement->flags |= AV_CODEC_FLAG_GLOBAL_HEADER;

    // Encoder 输入 PTS 由框架统一使用微秒承载；time_base 是编码器固定配置，而不是 RAW Caps。
    if (replacement->time_base.num <= 0 || replacement->time_base.den <= 0) {
        avcodec_free_context(&replacement);
        postMessage(MessageType::ERROR, "EncodeNode: invalid encoder time base from configured framerate");
        return false;
    }

    if (avcodec_open2(replacement, codec_, nullptr) < 0) {
        avcodec_free_context(&replacement);
        postMessage(MessageType::ERROR,
                    "EncodeNode: avcodec_open2 failed for '" + config_.codec_name + "'");
        return false;
    }

    // 先让转换资源成功替换，再交接 context；失败时保持旧 context 仍可由上层错误路径统一释放。
    if (!configureConversion(caps, output_format)) {
        avcodec_free_context(&replacement);
        return false;
    }

    ctx_ = replacement;
    input_caps_ = caps;
    encoder_pix_fmt_ = output_format;
    flushed_ = false;
    output_caps_emitted_ = false;
    fprintf(stderr, "[%s] encoder opened: codec=%s %dx%d input_pix_fmt=%d encoder_pix_fmt=%d\n",
            name_.c_str(), config_.codec_name.c_str(), caps.width, caps.height,
            caps.pix_fmt, encoder_pix_fmt_);
    return true;
}

bool EncodeNode::onCaps(const std::string&, const CapsEvent& caps,
                        std::vector<QueueItem>* outputs) {
    if (!outputs) {
        postMessage(MessageType::ERROR, "EncodeNode: Caps application requires an output sequence");
        return false;
    }

    // 新输入 Caps 不能越过旧 encoder 的延迟 Packet；首版协议要求首个 Packet 就能确定配置，
    // 因而 flush 不存在跨 Caps 留存的未发布 Packet。
    if (ctx_ && !flushed_) {
        if (!sendFrameAndDrain(nullptr, *outputs)) {
            return false;
        }
        flushed_ = true;
    }

    releaseEncoder();
    return configureEncoder(caps);
}

bool EncodeNode::buildEncoderFrame(const Buffer* input, AVFrame** output_frame) const {
    if (!input || !output_frame || !ctx_) {
        return false;
    }
    if (!input->data || input->size == 0 || input->media_type != MediaType::VIDEO_RAW ||
        !std::holds_alternative<VideoRawMeta>(input->meta)) {
        return false;
    }

    uint8_t* source_data[4]{};
    int source_linesize[4]{};
    const int source_size = av_image_fill_arrays(source_data, source_linesize, input->data,
                                                 input_caps_.pix_fmt, input_caps_.width,
                                                 input_caps_.height, 1);
    if (source_size <= 0 || static_cast<size_t>(source_size) != input->size) {
        return false;
    }

    AVFrame* frame = av_frame_alloc();
    if (!frame) {
        return false;
    }
    frame->format = encoder_pix_fmt_;
    frame->width = ctx_->width;
    frame->height = ctx_->height;
    frame->pts = input->pts == AV_NOPTS_VALUE
        ? AV_NOPTS_VALUE
        : av_rescale_q(input->pts, kFrameworkTimeBase, ctx_->time_base);
    frame->duration = input->duration > 0
        ? av_rescale_q(input->duration, kFrameworkTimeBase, ctx_->time_base)
        : 0;

    if (av_frame_get_buffer(frame, 0) < 0 || av_frame_make_writable(frame) < 0) {
        av_frame_free(&frame);
        return false;
    }

    if (input_caps_.pix_fmt == encoder_pix_fmt_) {
        // AVFrame 自己拥有 refcounted encoder 输入存储，不借用 Route Buffer；encoder 延迟引用 Frame
        // 时不会越过当前 Delivery / BufferRef 的处理生命周期。
        av_image_copy(frame->data, frame->linesize,
                      const_cast<const uint8_t* const*>(source_data), source_linesize,
                      encoder_pix_fmt_, ctx_->width, ctx_->height);
    } else {
        const uint8_t* source_planes[4] = {
            source_data[0], source_data[1], source_data[2], source_data[3]
        };
        if (!sws_ctx_ || sws_scale(sws_ctx_, source_planes, source_linesize, 0, ctx_->height,
                                   frame->data, frame->linesize) != ctx_->height) {
            av_frame_free(&frame);
            return false;
        }
    }

    *output_frame = frame;
    return true;
}

bool EncodeNode::isOutputConfigurationReady(const AVPacket* packet) {
    if (!packet || !ctx_) {
        return false;
    }

    // 当前节点强制 GLOBAL_HEADER，因此首个 Packet 前必须已有可复制的 extradata；如果 encoder
    // 不能满足，就拒绝该配置，而不是猜测关键帧里是否内嵌了足够的 codec 参数集。
    return ctx_->extradata && ctx_->extradata_size > 0;
}

bool EncodeNode::appendOutputCapsBeforePackets(std::vector<QueueItem>& outputs) {
    if (output_caps_emitted_) {
        return true;
    }
    if (!ctx_) {
        postMessage(MessageType::ERROR, "EncodeNode: cannot build output Caps without encoder context");
        return false;
    }

    CapsEvent caps;
    caps.media_type = MediaType::VIDEO_ENCODED;
    caps.codec_id = codec_->id;
    caps.width = ctx_->width;
    caps.height = ctx_->height;
    caps.framerate = config_.framerate;
    if (ctx_->extradata && ctx_->extradata_size > 0) {
        caps.extradata.assign(ctx_->extradata, ctx_->extradata + ctx_->extradata_size);
    }

    // Caps 必须在这个首个 Packet 之前进入同一 outputs 序列。即使 H.264/H.265 使用 in-band
    // SPS/PPS 而 extradata 为空，这个空 vector 也代表已确认的该编码器输出模式，而不是未知值。
    outputs.emplace_back(Event{caps});
    output_caps_emitted_ = true;
    return true;
}

bool EncodeNode::appendEncodedPacket(const AVPacket* packet, std::vector<QueueItem>& outputs) {
    if (!packet || packet->size <= 0 || !packet->data) {
        postMessage(MessageType::ERROR, "EncodeNode: encoder returned an empty Packet");
        return false;
    }

    /*
        当前只接受首个实际 Packet 时已具备 stream configuration 的模式
        因为如果 extradata 只有 flush 后才最终确定，在当前框架合同下，会造成无界缓存，无法真正流式输出
        且空 extradata 不能一概认为非法，某些 H.264/H.265 输出模式使用 in-band SPS/PPS，空 extradata 可能是合法的
        所以 H.264/H.265 没有 global extradata 时，必须确认该首个 Packet 本身就是带 SPS/PPS 的关键帧
        否则不能先发布无法被下游正确解释的非关键 Packet，也不允许为等未来关键帧无界暂存
    */
    if (!output_caps_emitted_ && !isOutputConfigurationReady(packet)) {
        postMessage(MessageType::ERROR,
                    "EncodeNode: first encoded Packet arrived before stream configuration was available");
        return false;
    }

    BufferRef encoded(Buffer::fromAVPacket(packet, MediaType::VIDEO_ENCODED, ctx_->time_base));
    if (!encoded) {
        postMessage(MessageType::ERROR, "EncodeNode: Buffer::fromAVPacket failed");
        return false;
    }

    if (output_caps_emitted_) {
        outputs.emplace_back(std::move(encoded));
        return true;
    }

    // 首包确认 configuration 后，在同一有序 outputs 中先写 Caps，再交出这个 Packet。
    if (!appendOutputCapsBeforePackets(outputs)) {
        return false;
    }
    outputs.emplace_back(std::move(encoded));
    return true;
}

bool EncodeNode::drainEncoder(std::vector<QueueItem>& outputs) {
    while (true) {
        AVPacket* packet = av_packet_alloc();
        if (!packet) {
            postMessage(MessageType::ERROR, "EncodeNode: av_packet_alloc failed");
            return false;
        }

        const int result = avcodec_receive_packet(ctx_, packet);
        if (result == AVERROR(EAGAIN) || result == AVERROR_EOF) {
            av_packet_free(&packet);
            return true;
        }
        if (result < 0) {
            av_packet_free(&packet);
            postMessage(MessageType::ERROR,
                        "EncodeNode: avcodec_receive_packet failed: " + avErrorStr(result), result);
            return false;
        }

        const bool appended = appendEncodedPacket(packet, outputs);
        av_packet_free(&packet);
        if (!appended) {
            return false;
        }
    }
}

bool EncodeNode::sendFrameAndDrain(AVFrame* frame, std::vector<QueueItem>& outputs) {
    int result = avcodec_send_frame(ctx_, frame);
    if (result == AVERROR(EAGAIN)) {
        // EAGAIN 与 Decode 的方向相反：先取走 encoder 已就绪 Packet，再以同一仍存活 Frame 重发。
        if (!drainEncoder(outputs)) {
            return false;
        }
        result = avcodec_send_frame(ctx_, frame);
    }
    if (result < 0) {
        postMessage(MessageType::ERROR,
                    "EncodeNode: avcodec_send_frame failed: " + avErrorStr(result), result);
        return false;
    }
    return drainEncoder(outputs);
}

void EncodeNode::process(const Buffer* input, std::vector<QueueItem>& outputs) {
    if (!ctx_) {
        postMessage(MessageType::ERROR, "EncodeNode: input Buffer arrived without configured encoder");
        return;
    }

    AVFrame* frame = nullptr;
    if (!buildEncoderFrame(input, &frame)) {
        postMessage(MessageType::ERROR,
                    "EncodeNode: input Buffer does not match active Caps or encoder frame allocation failed");
        return;
    }

    const bool encoded = sendFrameAndDrain(frame, outputs);
    av_frame_free(&frame);
    if (!encoded) {
        return;
    }
}

void EncodeNode::onEOS(std::vector<QueueItem>& outputs) {
    if (!ctx_ || flushed_) {
        return;
    }

    if (!sendFrameAndDrain(nullptr, outputs)) {
        return;
    }
    flushed_ = true;

    // 如果 encoder 到 EOS 都未产生任何 Packet，当前没有可发布的 encoded Caps；TransformNode
    // 随后只传播 EOS，保持“Caps 只在能解释真实 Packet 时才出现”的生产侧边界。
}

void EncodeNode::releaseConversion() {
    if (sws_ctx_) {
        sws_freeContext(sws_ctx_);
        sws_ctx_ = nullptr;
    }
}

void EncodeNode::releaseEncoder() {
    releaseConversion();
    if (ctx_) {
        avcodec_free_context(&ctx_);
    }
    encoder_pix_fmt_ = AV_PIX_FMT_NONE;
    flushed_ = false;
    output_caps_emitted_ = false;
}

void EncodeNode::onStop() {
    releaseEncoder();
    codec_ = nullptr;
}

} // namespace pipeline
