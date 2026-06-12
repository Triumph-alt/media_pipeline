#pragma once

#include "pipeline/core/INode.h"

extern "C" {
#include <libavcodec/avcodec.h>
}

namespace pipeline {

// ===================================================================
// DecodeNode：解码节点
//
// 继承 TransformNode，从 SinkPad 接收编码后的 packet（AVPacket），
// 解码为 raw frame（AVFrame），推送到 SrcPad。
//
// Pad 在 resolveLink 时按需创建（requestSrcPad/requestSinkPad）。
// onLink 收到 codecpar 时打开解码器，更新 SrcPad StreamInfo。
// 收到 EOS 时 flush 解码器，取出 B 帧缓冲中的剩余帧再传播 EOS。
// ===================================================================

class DecodeNode : public TransformNode {
public:
    explicit DecodeNode(const std::string& name);

protected:
    void onLink(SinkPad* pad, const StreamInfo& info) override;
    void onReady() override;
    void onNull() override;
    void workerLoop() override;
    std::shared_ptr<Buffer> process(std::shared_ptr<Buffer> /*input*/) override {
        return nullptr;  // DecodeNode 重写了 workerLoop，此函数不会被调用
    }

    // 重写 request：固定 "in"/"out"，已存在则复用
    SrcPad* requestSrcPad(MediaType type) override;
    SinkPad* requestSinkPad(MediaType type) override;

private:
    // flush 解码器，取出剩余帧并推送到下游
    void flushDecoder();

    // 从解码器取一帧，返回 nullptr 表示无更多帧
    std::shared_ptr<Buffer> receiveFrame();

    // ===== FFmpeg 资源 =====
    AVCodecContext* m_codecCtx = nullptr;
    const AVCodecParameters* m_codecpar = nullptr;
    AVRational m_inputTimeBase = {0, 1};
};

} // namespace pipeline
