#include "pipeline/nodes/DecodeNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Event.h"
#include "pipeline/core/MessageBus.h"
#include "pipeline/core/Pipeline.h"
#include "pipeline/core/StreamInfo.h"

#include <string>

namespace pipeline {

DecodeNode::DecodeNode(const std::string& name)
    : TransformNode(name) {}

// ===================================================================
// requestSrcPad：固定 "out"，已存在则复用
// ===================================================================

SrcPad* DecodeNode::requestSrcPad(MediaType type) {
    SrcPad* existing = getSrcPad("out");
    if (existing) return existing;

    size_t maxBufs = (type == MediaType::VIDEO) ? 5 : 30;
    return createSrcPad("out", maxBufs);
}

// ===================================================================
// requestSinkPad：固定 "in"，已存在则复用
// ===================================================================

SinkPad* DecodeNode::requestSinkPad(MediaType type) {
    SinkPad* existing = getSinkPad("in");
    if (existing) return existing;

    return createSinkPad("in");
}

// ===================================================================
// onLink：收到上游 StreamInfo 时，打开解码器
// ===================================================================

void DecodeNode::onLink(SinkPad* /*pad*/, const StreamInfo& info) {
    m_codecpar = info.codecpar;
    m_inputTimeBase = info.time_base;

    if (!m_codecpar) return;

    // 查找解码器
    const AVCodec* codec = avcodec_find_decoder(m_codecpar->codec_id);
    if (!codec) {
        m_bus->post({Message::Type::ERROR, this,
                     "DecodeNode: decoder not found for codec_id="
                         + std::to_string(m_codecpar->codec_id), 0});
        return;
    }

    // 分配上下文
    m_codecCtx = avcodec_alloc_context3(codec);
    if (!m_codecCtx) {
        m_bus->post({Message::Type::ERROR, this,
                     "DecodeNode: avcodec_alloc_context3 failed", 0});
        return;
    }

    // 拷贝 codec 参数到上下文
    int ret = avcodec_parameters_to_context(m_codecCtx, m_codecpar);
    if (ret < 0) {
        char errBuf[AV_ERROR_MAX_STRING_SIZE];
        av_strerror(ret, errBuf, sizeof(errBuf));
        m_bus->post({Message::Type::ERROR, this,
                     std::string("DecodeNode: avcodec_parameters_to_context failed: ") + errBuf, ret});
        avcodec_free_context(&m_codecCtx);
        return;
    }

    // 设置解码线程数
    if (hasParam("thread_count")) {
        m_codecCtx->thread_count = getParam<int>("thread_count", 1);
    }

    // 打开解码器
    ret = avcodec_open2(m_codecCtx, codec, nullptr);
    if (ret < 0) {
        char errBuf[AV_ERROR_MAX_STRING_SIZE];
        av_strerror(ret, errBuf, sizeof(errBuf));
        m_bus->post({Message::Type::ERROR, this,
                     std::string("DecodeNode: avcodec_open2 failed: ") + errBuf, ret});
        avcodec_free_context(&m_codecCtx);
        return;
    }

    // 更新输出 StreamInfo
    StreamInfo outInfo;
    if (m_codecCtx->codec_type == AVMEDIA_TYPE_VIDEO) {
        outInfo.type = MediaType::VIDEO;
        outInfo.width = m_codecCtx->width;
        outInfo.height = m_codecCtx->height;
        outInfo.pixelFmt = m_codecCtx->pix_fmt;
        outInfo.frameRate = m_codecCtx->framerate;
    } else if (m_codecCtx->codec_type == AVMEDIA_TYPE_AUDIO) {
        outInfo.type = MediaType::AUDIO;
        outInfo.sampleRate = m_codecCtx->sample_rate;
        outInfo.channels = m_codecCtx->ch_layout.nb_channels;
        outInfo.sampleFmt = m_codecCtx->sample_fmt;
    }
    outInfo.codecId = m_codecpar->codec_id;
    outInfo.time_base = m_codecCtx->time_base;

    // 更新 SrcPad 的 StreamInfo
    if (!m_srcPads.empty()) {
        m_srcPads[0]->setStreamInfo(outInfo);
    }
}

// ===================================================================
// onReady：解码器已在 onLink 打开，此处做收尾工作
// ===================================================================

void DecodeNode::onReady() {
    // 解码器已打开，无需额外操作
}

// ===================================================================
// workerLoop：从 SinkPad pop packet → 送入解码器 → 取出 frame → push
//
// packetPending 机制：send_packet 返回 EAGAIN 时，暂存当前 packet，
// 下次循环不取新包，直接重送同一包。
// ===================================================================

void DecodeNode::workerLoop() {
    // 等待进入 PLAYING 状态
    {
        std::unique_lock lock(m_stateMutex);
        m_stateCV.wait(lock, [this] {
            return m_state == NodeState::PLAYING || m_stopRequested;
        });
        if (m_stopRequested) return;
    }

    bool packetPending = false;
    std::shared_ptr<Buffer> pendingBuf;

    while (!m_stopRequested) {
        // 1. 取包：如果没有暂存的包，从 SinkPad 取
        if (!packetPending) {
            auto result = m_sinkPads[0]->pop(std::chrono::milliseconds(100));

            if (result.isEmpty()) {
                continue;
            }

            if (result.hasEvent()) {
                auto event = result.event();
                if (event->type == Event::Type::EOS) {
                    flushDecoder();
                    for (auto& pad : m_srcPads) {
                        pad->pushEvent(Event::makeEOS());
                    }
                    break;
                }
                continue;
            }

            if (result.hasBuffer()) {
                pendingBuf = result.buffer();
            } else {
                continue;
            }
        }

        // 2. 送解码器
        AVPacket* pkt = av_packet_alloc();
        pkt->data = static_cast<uint8_t*>(pendingBuf->data->data());
        pkt->size = static_cast<int>(pendingBuf->size);
        pkt->pts = pendingBuf->pts;
        pkt->dts = pendingBuf->dts;
        pkt->duration = pendingBuf->duration;
        pkt->stream_index = pendingBuf->streamIndex;
        if (pendingBuf->isKeyFrame()) {
            pkt->flags |= AV_PKT_FLAG_KEY;
        }

        int ret = avcodec_send_packet(m_codecCtx, pkt);
        av_packet_free(&pkt);

        if (ret == AVERROR(EAGAIN)) {
            packetPending = true;  // 暂存，下次循环重试
            continue;
        }
        if (ret < 0) {
            char errBuf[AV_ERROR_MAX_STRING_SIZE];
            av_strerror(ret, errBuf, sizeof(errBuf));
            m_bus->post({Message::Type::ERROR, this,
                         std::string("DecodeNode: avcodec_send_packet failed: ") + errBuf, ret});
            packetPending = false;
            continue;
        }

        // 发送成功，清标记，释放暂存的包
        packetPending = false;
        pendingBuf.reset();

        // 3. 取出所有已解码的帧
        while (true) {
            auto out = receiveFrame();
            if (!out) break;
            m_srcPads[0]->push(out);
        }
    }
}

// ===================================================================
// receiveFrame：从解码器取一帧
// ===================================================================

std::shared_ptr<Buffer> DecodeNode::receiveFrame() {
    AVFrame* frame = av_frame_alloc();
    if (!frame) return nullptr;

    int ret = avcodec_receive_frame(m_codecCtx, frame);
    if (ret < 0) {
        av_frame_free(&frame);
        if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF) {
            return nullptr;  // 需要更多输入 或 已经取完
        }
        char errBuf[AV_ERROR_MAX_STRING_SIZE];
        av_strerror(ret, errBuf, sizeof(errBuf));
        m_bus->post({Message::Type::ERROR, this,
                     std::string("DecodeNode: avcodec_receive_frame failed: ") + errBuf, ret});
        return nullptr;
    }

    auto buf = Buffer::fromAVFrame(frame, m_codecCtx->time_base,
                                    m_pipeline->memoryPool());
    av_frame_free(&frame);
    return buf;
}

// ===================================================================
// flushDecoder：发送空 packet 触发 flush，取出所有剩余帧
// ===================================================================

void DecodeNode::flushDecoder() {
    // 发送 nullptr packet 表示 flush
    avcodec_send_packet(m_codecCtx, nullptr);

    // 循环取出缓冲中的剩余帧
    while (true) {
        auto buf = receiveFrame();
        if (!buf) break;
        m_srcPads[0]->push(buf);
    }
}

// ===================================================================
// onNull：释放解码器
// ===================================================================

void DecodeNode::onNull() {
    if (m_codecCtx) {
        avcodec_free_context(&m_codecCtx);
    }
    m_codecpar = nullptr;
}

} // namespace pipeline
