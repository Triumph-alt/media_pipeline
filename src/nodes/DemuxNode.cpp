#include "pipeline/nodes/DemuxNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Event.h"
#include "pipeline/core/MessageBus.h"
#include "pipeline/core/Pipeline.h"

#include <string>

extern "C" {
#include <libavutil/error.h>
}

namespace pipeline {

DemuxNode::DemuxNode(const std::string& name)
    : TransformNode(name) {}

// ===================================================================
// 辅助：检查 FFmpeg 流类型是否匹配 MediaType
// ===================================================================

bool DemuxNode::streamMatchesType(const AVStream* stream, MediaType type) const {
    switch (type) {
    case MediaType::VIDEO: return stream->codecpar->codec_type == AVMEDIA_TYPE_VIDEO;
    case MediaType::AUDIO: return stream->codecpar->codec_type == AVMEDIA_TYPE_AUDIO;
    default: return false;
    }
}

// ===================================================================
// onProbe：打开文件探测流信息（不创建 Pad）
// ===================================================================

void DemuxNode::onProbe() {
    std::string url = getParam<std::string>("url");
    if (url.empty()) {
        m_bus->post({Message::Type::ERROR, this, "DemuxNode: url parameter is empty", 0});
        return;
    }

    // 打开输入文件
    AVFormatContext* fmtCtx = nullptr;
    int ret = avformat_open_input(&fmtCtx, url.c_str(), nullptr, nullptr);
    if (ret < 0) {
        char errBuf[AV_ERROR_MAX_STRING_SIZE];
        av_strerror(ret, errBuf, sizeof(errBuf));
        m_bus->post({Message::Type::ERROR, this,
                     std::string("DemuxNode: avformat_open_input failed: ") + errBuf, ret});
        return;
    }
    m_fmtCtx = fmtCtx;

    // 读取流信息
    ret = avformat_find_stream_info(m_fmtCtx, nullptr);
    if (ret < 0) {
        char errBuf[AV_ERROR_MAX_STRING_SIZE];
        av_strerror(ret, errBuf, sizeof(errBuf));
        m_bus->post({Message::Type::ERROR, this,
                     std::string("DemuxNode: avformat_find_stream_info failed: ") + errBuf, ret});
        avformat_close_input(&m_fmtCtx);
        return;
    }
}

// ===================================================================
// requestSrcPad：按 MediaType 用 av_find_best_stream 找最佳流
// ===================================================================

SrcPad* DemuxNode::requestSrcPad(MediaType type) {
    if (!m_fmtCtx) return nullptr;

    // 检查是否已有同类型 Pad（复用）
    SrcPad* existing = getSrcPad(type);
    if (existing) return existing;

    // 用 av_find_best_stream 找最佳流
    AVMediaType avType;
    switch (type) {
    case MediaType::VIDEO: avType = AVMEDIA_TYPE_VIDEO; break;
    case MediaType::AUDIO: avType = AVMEDIA_TYPE_AUDIO; break;
    default: return nullptr;
    }

    int streamIdx = av_find_best_stream(m_fmtCtx, avType, -1, -1, nullptr, 0);
    if (streamIdx < 0) {
        return nullptr;  // 该类型的流不存在
    }

    AVStream* stream = m_fmtCtx->streams[streamIdx];

    // 创建 SrcPad，队列大小按媒体类型区分
    std::string padName = (type == MediaType::VIDEO) ? "video" : "audio";
    size_t maxBufs = (type == MediaType::VIDEO) ? 15 : 50;
    SrcPad* pad = createSrcPad(padName, maxBufs);

    // 填充 StreamInfo
    StreamInfo info;
    info.type = type;
    info.codecId = stream->codecpar->codec_id;
    info.codecpar = stream->codecpar;
    info.time_base = stream->time_base;
    info.duration = stream->duration > 0
        ? av_rescale_q(stream->duration, stream->time_base, {1, 1000000})
        : 0;

    if (type == MediaType::VIDEO) {
        info.width = stream->codecpar->width;
        info.height = stream->codecpar->height;
        info.pixelFmt = static_cast<AVPixelFormat>(stream->codecpar->format);
        AVRational fr = av_guess_frame_rate(m_fmtCtx, stream, nullptr);
        info.frameRate = fr;
    } else if (type == MediaType::AUDIO) {
        info.sampleRate = stream->codecpar->sample_rate;
        info.channels = stream->codecpar->ch_layout.nb_channels;
        info.sampleFmt = static_cast<AVSampleFormat>(stream->codecpar->format);
    }

    pad->setStreamInfo(info);

    // 建立映射：文件流 index → SrcPad index
    m_streamMap[streamIdx] = static_cast<int>(m_srcPads.size()) - 1;

    return pad;
}

// ===================================================================
// onReady：检查工作模式
// ===================================================================

void DemuxNode::onReady() {
    // FROM_URL 模式：资源已在 onProbe 分配
}

// ===================================================================
// workerLoop：核心工作循环
//
// 循环 av_read_frame → Buffer::fromAVPacket → push 到对应 SrcPad
// EOF 时对每个活跃的 SrcPad 发送 EOS Event
// ===================================================================

void DemuxNode::workerLoop() {
    // 等待进入 PLAYING 状态
    {
        std::unique_lock lock(m_stateMutex);
        m_stateCV.wait(lock, [this] {
            return m_state == NodeState::PLAYING || m_stopRequested;
        });
        if (m_stopRequested) return;
    }

    // 全局队列水位阈值 = 所有 SrcPad 的 maxBuffers 之和
    size_t maxTotalQueued = 0;
    for (auto& pad : m_srcPads) {
        maxTotalQueued += pad->queueMaxSize();
    }

    AVPacket* pkt = av_packet_alloc();
    if (!pkt) {
        m_bus->post({Message::Type::ERROR, this,
                     "DemuxNode: av_packet_alloc failed", 0});
        return;
    }

    while (!m_stopRequested) {
        // 主动背压（ffplay 风格）：
        //   条件 1: 所有队列总元素超过全局上限 → 暂停读取
        //   条件 2: 所有队列都达到 80% 高压线 → 暂停读取
        size_t totalQueued = 0;
        bool allQueuesFull = true;
        for (auto& pad : m_srcPads) {
            size_t qs = pad->queueSize();
            totalQueued += qs;
            size_t highMark = pad->queueMaxSize() * 4 / 5;  // 80% 高压线
            if (qs < highMark) {
                allQueuesFull = false;
            }
        }
        if (totalQueued >= maxTotalQueued || allQueuesFull) {
            std::unique_lock lock(m_stateMutex);
            m_stateCV.wait_for(lock, std::chrono::milliseconds(10));
            continue;
        }

        int ret = av_read_frame(m_fmtCtx, pkt);
        if (ret < 0) {
            if (ret == AVERROR_EOF || ret == AVERROR(EIO)) {
                // 正常 EOF：对每个活跃 SrcPad 发送 EOS
                for (size_t i = 0; i < m_srcPads.size(); i++) {
                    m_srcPads[i]->pushEvent(Event::makeEOS(static_cast<int>(i)));
                }
            } else {
                char errBuf[AV_ERROR_MAX_STRING_SIZE];
                av_strerror(ret, errBuf, sizeof(errBuf));
                m_bus->post({Message::Type::ERROR, this,
                             std::string("DemuxNode: av_read_frame failed: ") + errBuf, ret});
            }
            break;
        }

        // 查找该 packet 对应的 SrcPad
        auto it = m_streamMap.find(pkt->stream_index);
        if (it == m_streamMap.end()) {
            // 该流无对应 Pad，丢弃
            av_packet_unref(pkt);
            continue;
        }
        int padIdx = it->second;

        // 从 packet 创建 Buffer
        AVStream* stream = m_fmtCtx->streams[pkt->stream_index];
        auto buf = Buffer::fromAVPacket(pkt, stream->time_base,
                                        m_pipeline->memoryPool());

        // push（背压已控制，正常情况队列有空间；canPush 为防御性检查）
        if (m_srcPads[padIdx]->canPush()) {
            m_srcPads[padIdx]->push(buf);
        }

        av_packet_unref(pkt);
    }

    av_packet_free(&pkt);
}

// ===================================================================
// onNull：释放 FFmpeg 资源
// ===================================================================

void DemuxNode::onNull() {
    if (m_fmtCtx) {
        avformat_close_input(&m_fmtCtx);
    }
    m_streamMap.clear();
}

} // namespace pipeline
