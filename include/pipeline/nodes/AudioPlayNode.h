#pragma once

#include "pipeline/core/BaseNode.h"

extern "C" {
#include <libavutil/samplefmt.h>
}

struct SwrContext;
struct SDL_AudioStream;

namespace pipeline {

// ===================================================================
// AudioPlayNode: SDL3 音频播放节点（SinkNode 子类）
//
// onStreamInfo 中初始化 SDL 音频流，必要时创建 swr 做采样格式转换。
// consume 中推数据到 SDL 音频流，更新主时钟。
// onStop 中关闭 SDL 音频流和释放 swr。
// ===================================================================
class AudioPlayNode final : public SinkNode {
public:
    explicit AudioPlayNode(const std::string& name);

protected:
    bool onReady() override { return true; }
    bool onStreamInfo() override;
    void consume(const Buffer* buf) override;
    void onStop() override;

private:
    int             sample_rate_     = 0;
    int             channels_        = 0;
    int             bytes_per_sample_ = 0;
    int64_t         submitted_samples_ = 0;
    AVSampleFormat  input_sample_fmt_  = AV_SAMPLE_FMT_NONE;

    // SDL 音频流（void* 避免头文件污染 SDL.h）
    void* audio_stream_ = nullptr;

    // swr 采样格式转换（输入非目标格式时使用）
    SwrContext* swr_ctx_      = nullptr;
    uint8_t*    swr_buffer_   = nullptr;
    int         swr_buffer_size_ = 0;
};

} // namespace pipeline
