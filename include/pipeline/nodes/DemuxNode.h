#pragma once

#include "pipeline/core/INode.h"

extern "C" {
#include <libavformat/avformat.h>
}

#include <string>
#include <vector>

namespace pipeline {

// ===================================================================
// DemuxNode：解复用节点
//
// 继承 TransformNode，但工作模式分两种：
//   FROM_URL     — 无上游连接，自行打开文件/URL，循环 av_read_frame
//   FROM_UPSTREAM — 接收上游推送的容器数据进行解复用（预留）
//
// onProbe 时打开文件、探测流信息，为每个流创建一个 SrcPad。
// SrcPad 命名规则："video_0", "audio_0", "subtitle_0" 等。
// ===================================================================

class DemuxNode : public TransformNode {
public:
    explicit DemuxNode(const std::string& name);

    // 参数:
    //   "url" : std::string — 文件路径或网络 URL

protected:
    void onProbe() override;
    void onReady() override;
    void onNull() override;
    void workerLoop() override;
    std::shared_ptr<Buffer> process(std::shared_ptr<Buffer> /*input*/) override {
        return nullptr;  // DemuxNode 重写了 workerLoop，此函数不会被调用
    }

private:
    enum class Mode { FROM_URL, FROM_UPSTREAM };

    // ===== FFmpeg 资源 =====
    AVFormatContext* m_fmtCtx = nullptr;
    Mode m_mode = Mode::FROM_URL;

    // ===== 流索引映射 =====
    // m_streamMap[avStreamIndex] = pipeline SrcPad 的下标
    // 值为 -1 表示该流被忽略（无对应 SrcPad）
    std::vector<int> m_streamMap;
};

} // namespace pipeline
