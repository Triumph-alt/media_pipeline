#pragma once

#include "pipeline/core/BaseNode.h"

struct AVCodec;
struct AVCodecContext;
struct AVFrame;
struct AVPacket;

namespace pipeline {

// ===================================================================
// DecodeNode: 运行期 Caps 驱动的 FFmpeg 解码 Transform
//
// 输入 encoded Caps 到达时打开或重开 decoder。真实 RAW 格式必须由 AVFrame 确定：
// 首帧或格式变化帧之前先输出完整 RAW Caps，再输出对应 Buffer。EOS flush 复用同一
// 有序 QueueItem 产出路径，避免绕开 BufferRef 所有权边界。
// ===================================================================
class DecodeNode final : public TransformNode {
public:
    explicit DecodeNode(const std::string& name);

protected:
    bool onReady() override { return true; }
    void onStop() override;
    bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override;
    void process(const Buffer* input, std::vector<QueueItem>& outputs) override;
    void onEOS(std::vector<QueueItem>& outputs) override;

private:
    AVPacket* toAVPacket(const Buffer* buf);
    bool configureDecoder(const CapsEvent& caps);
    bool drainDecoder(std::vector<QueueItem>& outputs);
    bool appendFrame(AVFrame* frame, std::vector<QueueItem>& outputs);
    bool appendOutputCapsForFrame(const AVFrame* frame, std::vector<QueueItem>& outputs);

    AVCodecContext* ctx_ = nullptr;
    const AVCodec* codec_ = nullptr;
    bool is_video_ = false;
    // encoded 流的 nominal timing hint，仅用于推导输出视频 Buffer 的 duration。
    AVRational framerate_ = {0, 1};
    bool flushed_ = false;
    std::optional<CapsEvent> output_caps_;
};

} // namespace pipeline
