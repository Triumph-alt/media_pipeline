#pragma once

#include "pipeline/core/BaseNode.h"

struct AVCodec;
struct AVCodecContext;
struct AVPacket;

namespace pipeline {

// ===================================================================
// DecodeNode: FFmpeg 解码节点（TransformNode 子类）
//
// 接收 VIDEO_ENCODED / AUDIO_ENCODED，输出对应的 RAW 类型。
// onStreamInfo 中用 receiveCapsEvent 取输入 Caps → 打开解码器 →
// 从 ctx 读取输出参数 → sendCapsEvent 输出 Caps。
// process 中 send_packet → receive_frame 循环产出 Buffer。
// EOS 时 flush 解码器缓冲区。
// ===================================================================
class DecodeNode final : public TransformNode {
public:
    explicit DecodeNode(const std::string& name);

protected:
    bool onReady() override { return true; }
    void onStop() override;
    bool onStreamInfo() override;
    void process(const Buffer* input, std::vector<BufferRef>& outputs) override;
    void onEvent(const Event& event) override;

private:
    // 将 Buffer 数据转为 AVPacket（深拷贝 payload），用于 avcodec_send_packet
    AVPacket* toAVPacket(const Buffer* buf);

    AVCodecContext* ctx_       = nullptr;
    const AVCodec*  codec_    = nullptr;
    bool            is_video_ = false;
    AVRational      framerate_ = {0, 1};
    bool            flushed_  = false;
};

} // namespace pipeline
