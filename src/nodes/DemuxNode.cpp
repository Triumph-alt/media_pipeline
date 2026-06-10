#include "pipeline/nodes/DemuxNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Event.h"
#include "pipeline/core/MessageBus.h"
#include "pipeline/core/Pipeline.h"
#include "pipeline/core/StreamInfo.h"

#include <chrono>
#include <string>

namespace pipeline {

DemuxNode::DemuxNode(const std::string& name)
    : TransformNode(name) {}

// ===================================================================
// onProbe：打开输入、探测流信息、创建 SrcPad
// ===================================================================
void DemuxNode::onProbe() {
    // 读取用户设置的 url
    std::string url = getParam<std::string>("url");
    if (url.empty()) {
        m_bus->post({Message::Type::ERROR, this, "DemuxNode: url parameter is empty", 0});
        return;
    }

    // 打开输入
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

    // 探测流信息
    ret = avformat_find_stream_info(m_fmtCtx, nullptr);
    if (ret < 0) {
        char errBuf[AV_ERROR_MAX_STRING_SIZE];
        av_strerror(ret, errBuf, sizeof(errBuf));
        m_bus->post({Message::Type::ERROR, this,
                     std::string("DemuxNode: avformat_find_stream_info failed: ") + errBuf, ret});
        avformat_close_input(&m_fmtCtx);
        return;
    }

    // 遍历所有流，为每个支持的流创建 SrcPad
    m_streamMap.resize(m_fmtCtx->nb_streams, -1);

    // 用于给同类型流编号: video_0, video_1, audio_0, ...
    int videoIdx = 0;
    int audioIdx = 0;
    int subtitleIdx = 0;
    int padIdx = 0;

    for (unsigned i = 0; i < m_fmtCtx->nb_streams; i++) {
        AVStream* stream = m_fmtCtx->streams[i];
        AVCodecParameters* par = stream->codecpar;
        MediaType mediaType = MediaType::UNKNOWN;
        std::string padName;

        switch (par->codec_type) {
        case AVMEDIA_TYPE_VIDEO:
            mediaType = MediaType::VIDEO;
            padName = "video_" + std::to_string(videoIdx++);
            break;
        case AVMEDIA_TYPE_AUDIO:
            mediaType = MediaType::AUDIO;
            padName = "audio_" + std::to_string(audioIdx++);
            break;
        default:
            // 暂不支持的流类型，跳过
            m_streamMap[i] = -1;
            continue;
        }

        // 创建 SrcPad（音频帧小多缓，视频帧大少缓）
        size_t maxBufs = (mediaType == MediaType::AUDIO) ? 20 : 5;
        SrcPad* pad = createSrcPad(padName, maxBufs);

        // 填充 StreamInfo
        StreamInfo info;
        info.type = mediaType;
        info.codecId = par->codec_id;
        info.codecpar = par;
        info.time_base = stream->time_base;

        if (mediaType == MediaType::VIDEO) {
            info.width = par->width;
            info.height = par->height;
            info.pixelFmt = static_cast<AVPixelFormat>(par->format);
            // 帧率
            AVRational fr = av_guess_frame_rate(m_fmtCtx, stream, nullptr);
            info.frameRate = fr;
        } else if (mediaType == MediaType::AUDIO) {
            info.sampleRate = par->sample_rate;
            info.channels = par->ch_layout.nb_channels;
            info.sampleFmt = static_cast<AVSampleFormat>(par->format);
        }

        // 流总时长
        if (stream->duration != AV_NOPTS_VALUE && stream->time_base.den > 0) {
            info.duration = av_rescale_q(stream->duration, stream->time_base, {1, 1000000});
        }

        pad->setStreamInfo(info);

        // 记录映射
        m_streamMap[i] = padIdx++;
    }
}

// ===================================================================
// onReady：检查是否有上游连接，决定工作模式
// ===================================================================
void DemuxNode::onReady() {
    // 检查是否有上游连接
    if (!m_sinkPads.empty() && m_sinkPads[0]->isConnected()) {
        m_mode = Mode::FROM_UPSTREAM;
    } else {
        m_mode = Mode::FROM_URL;
        // FROM_URL 模式下，资源已在 onProbe 分配
    }
}

// ===================================================================
// workerLoop：核心工作循环
//
// FROM_URL 模式：循环 av_read_frame → Buffer::fromAVPacket → push
// EOF 时对每个活跃的 SrcPad 发送 EOS Event
// ===================================================================
void DemuxNode::workerLoop() {
    if (m_mode == Mode::FROM_URL) {
        // 等待进入 PLAYING 状态
        {
            std::unique_lock lock(m_stateMutex);
            m_stateCV.wait(lock, [this] {
                return m_state == NodeState::PLAYING || m_stopRequested;
            });
            if (m_stopRequested) {
                return;
            }
        }

        // 主动背压阈值（参考 ffplay）：
        //   全局总量 = 所有 SrcPad 的 maxBuffers 之和再留些余量
        //   单队列高压 = 每个 Pad 自己的 maxBuffers
        size_t maxTotalQueued = 0;
        for (auto& pad : m_srcPads) {
            maxTotalQueued += pad->queueMaxSize();
        }
        if (maxTotalQueued > 5) maxTotalQueued -= 5;  // 留一点余量

        AVPacket* pkt = av_packet_alloc();
        if (!pkt) {
            m_bus->post({Message::Type::ERROR, this,
                         "DemuxNode: av_packet_alloc failed", 0});
            return;
        }

        while (!m_stopRequested) {
            // 主动背压（ffplay 风格）：
            //   条件 1: 所有队列总元素超过全局上限 → 暂停读取
            //   条件 2: 任意一个队列达到自己的 maxBuffers → 暂停读取
            size_t totalQueued = 0;
            bool anyQueueFull = false;
            for (auto& pad : m_srcPads) {
                size_t qs = pad->queueSize();
                totalQueued += qs;
                if (qs >= pad->queueMaxSize()) {
                    anyQueueFull = true;
                }
            }
            if (totalQueued >= maxTotalQueued || anyQueueFull) {
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
            int padIdx = -1;
            if (pkt->stream_index >= 0 &&
                static_cast<size_t>(pkt->stream_index) < m_streamMap.size()) {
                padIdx = m_streamMap[pkt->stream_index];
            }
            if (padIdx < 0) {
                // 该流无对应 Pad，丢弃
                av_packet_unref(pkt);
                continue;
            }

            // 从 packet 创建 Buffer
            AVStream* stream = m_fmtCtx->streams[pkt->stream_index];
            auto buf = Buffer::fromAVPacket(pkt, stream->time_base,
                                            m_pipeline->memoryPool());

            // 阻塞 push（水位已控制，队列大概率有空间）
            m_srcPads[padIdx]->push(buf);

            av_packet_unref(pkt);
        }

        av_packet_free(&pkt);
    }
    // FROM_UPSTREAM 模式：预留，后续实现
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
