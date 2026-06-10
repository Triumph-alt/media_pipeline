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
// onProbe：创建输入输出 Pad
// ===================================================================

void DecodeNode::onProbe() {
    createSinkPad("in");
    createSrcPad("out");
}

// ===================================================================
// onLink：上游连接建立时，保存 codec 参数和 time_base
// ===================================================================

void DecodeNode::onLink(SinkPad* /*pad*/, const StreamInfo& info) {
    m_codecpar = info.codecpar;
    m_inputTimeBase = info.time_base;
}

// ===================================================================
// onReady：打开解码器，更新输出 StreamInfo
// ===================================================================

void DecodeNode::onReady() {
    if (!m_codecpar) {
        m_bus->post({Message::Type::ERROR, this,
                     "DecodeNode: no codec parameters (onLink not called?)", 0});
        return;
    }

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

    m_srcPads[0]->setStreamInfo(outInfo);

    // 通知下游 StreamInfo 变化
    m_srcPads[0]->pushEvent(Event::makeStreamInfoChanged(outInfo));
}

// ===================================================================
// workerLoop：从 SinkPad pop packet → 送入解码器 → 取出 frame → push
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

    while (!m_stopRequested) {
        auto result = m_sinkPads[0]->pop(std::chrono::milliseconds(100));

        if (result.isEmpty()) {
            // 超时，继续循环检查 stopRequested
            continue;
        }

        if (result.hasEvent()) {
            auto event = result.event();
            if (event->type == Event::Type::EOS) {
                // flush 解码器，取出 B 帧缓冲中的剩余帧
                flushDecoder();
                // 传播 EOS 到下游
                for (auto& pad : m_srcPads) {
                    pad->pushEvent(Event::makeEOS());
                }
                break;
            }
            // 其他事件忽略
            continue;
        }

        if (result.hasBuffer()) {
            // 构造 AVPacket 引用 Buffer 数据（不拷贝）
            // 注意：pkt->data 指向 result.buffer() 的内存，result 必须保持存活
            AVPacket* pkt = av_packet_alloc();
            pkt->data = static_cast<uint8_t*>(result.buffer()->data->data());
            pkt->size = static_cast<int>(result.buffer()->size);
            pkt->pts = result.buffer()->pts;
            pkt->dts = result.buffer()->dts;
            pkt->duration = result.buffer()->duration;
            pkt->stream_index = result.buffer()->streamIndex;
            if (result.buffer()->isKeyFrame()) {
                pkt->flags |= AV_PKT_FLAG_KEY;
            }

            // 送 packet 到解码器，EAGAIN 时 drain 后重试
            while (!m_stopRequested) {
                int ret = avcodec_send_packet(m_codecCtx, pkt);
                if (ret == AVERROR(EAGAIN)) {
                    // 解码器满了，先取出已解码的帧，再重送同一个 packet
                    while (true) {
                        auto out = receiveFrame();
                        if (!out) break;
                        m_srcPads[0]->push(out);
                    }
                    continue;  // 重试同一个 pkt
                }
                if (ret < 0) {
                    char errBuf[AV_ERROR_MAX_STRING_SIZE];
                    av_strerror(ret, errBuf, sizeof(errBuf));
                    m_bus->post({Message::Type::ERROR, this,
                                 std::string("DecodeNode: avcodec_send_packet failed: ") + errBuf, ret});
                    break;
                }
                break;  // 成功
            }

            av_packet_free(&pkt);

            // 取出所有已解码的帧
            while (true) {
                auto out = receiveFrame();
                if (!out) break;
                m_srcPads[0]->push(out);
            }
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
