#pragma once

#include "pipeline/core/BaseNode.h"

extern "C" {
#include <libavformat/avformat.h>
}

namespace pipeline {

// ===================================================================
// AVDemuxNode: 基于 FFmpeg 的解复用节点
//
// 实现 DemuxNode 基类的四个纯虚钩子：
//   openInput    — avformat_open_input
//   probeStreams — avformat_find_stream_info + av_find_best_stream
//   readFrame    — av_read_frame，按 stream_index 分发
//   closeInput   — avformat_close_input
//
// Pad 创建、Caps 发送、Buffer 分发、EOS 和 Ready 回滚均由 DemuxNode 基类处理。
// ===================================================================
class AVDemuxNode final : public DemuxNode {
public:
    AVDemuxNode(const std::string& name, const std::string& url)
        : DemuxNode(name, url) {}

private:
    bool openInput(const std::string& url) override;
    bool probeStreams(DemuxProbeResult* result) override;
    DemuxReadResult readFrame() override;
    void closeInput() override;

    AVFormatContext* fmt_ctx_        = nullptr;
    int              video_stream_idx_ = -1;
    int              audio_stream_idx_ = -1;
};

} // namespace pipeline
