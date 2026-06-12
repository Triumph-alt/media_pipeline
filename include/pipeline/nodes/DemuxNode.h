#pragma once

#include "pipeline/core/INode.h"

extern "C" {
#include <libavformat/avformat.h>
}

#include <string>
#include <unordered_map>

namespace pipeline {

// ===================================================================
// DemuxNode：解复用节点
//
// 继承 TransformNode，工作模式 FROM_URL：
//   onProbe 时打开文件探测流信息（不创建 Pad）
//   requestSrcPad 时用 av_find_best_stream 找最佳流，按需创建 SrcPad
//   workerLoop 循环 av_read_frame → push 到对应 SrcPad
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

    // 重写 requestSrcPad：按 MediaType 用 av_find_best_stream 找最佳流
    SrcPad* requestSrcPad(MediaType type) override;

private:
    // ===== FFmpeg 资源 =====
    AVFormatContext* m_fmtCtx = nullptr;

    // ===== 流索引映射 =====
    // m_streamMap[avStreamIndex] = pipeline SrcPad 的下标
    std::unordered_map<int, int> m_streamMap;

    // 检查 FFmpeg 流类型是否匹配 MediaType
    bool streamMatchesType(const AVStream* stream, MediaType type) const;
};

} // namespace pipeline
