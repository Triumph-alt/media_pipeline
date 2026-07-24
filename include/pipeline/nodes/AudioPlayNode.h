#pragma once

#include "pipeline/core/BaseNode.h"

#include <cstdint>

extern "C" {
#include <libavutil/samplefmt.h>
}

struct SwrContext;
struct SDL_AudioStream;

namespace pipeline {

// ===================================================================
// AudioPlayNode: 固定 canonical 输出的 SDL3 音频 Sink
//
// Ready 建立一次应用侧 canonical SDL 提交格式：S16 packed / 默认设备派生采样率 /
// 固定 stereo FL/FR。Running 中每份完整 AudioRaw Caps 只重建 input→canonical 的
// swr；设备周期、背压、提交账本和 Clock 永远使用 canonical 量纲。
// ===================================================================
class AudioPlayNode final : public SinkNode {
public:
    explicit AudioPlayNode(const std::string& name);

protected:
    bool onReady() override;
    bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override;
    void runLoop() override;
    void consume(const Buffer* buf) override;
    void onDrain() override;
    void onStop() override;

private:
    bool openCanonicalStream();
    bool configureInputResampler(const CapsEvent& caps);
    bool drainSwr();
    bool ensureSwrBuffer(int output_frames);
    bool waitForBufferSpace();
    bool updateClockFromQueued(SDL_AudioStream* stream, int64_t* queued_frames = nullptr);
    bool submitAndTrack(SDL_AudioStream* stream, const uint8_t* data, int size);
    bool convertAndSubmit(const Buffer* buf);
    void waitCancellable(int64_t us);

    // 固定 application-side SDL 提交格式；整个 Pipeline 生命周期内不随输入 Caps 改变。
    int canonical_rate_ = 0;
    static constexpr int kCanonicalChannels = 2;
    static constexpr int kCanonicalBytesPerSample = 2;
    int canonical_bytes_per_frame_ = kCanonicalChannels * kCanonicalBytesPerSample;

    // 当前输入 Caps 派生的 swr 配置；仅在 Running 收到 AudioRaw Caps 时变化。
    int input_rate_ = 0;
    int input_channels_ = 0;
    AVSampleFormat input_sample_fmt_ = AV_SAMPLE_FMT_NONE;
    ChannelLayout input_layout_;

    // 全部为 canonical frame / microsecond 量纲。
    bool anchor_seen_ = false;
    int64_t anchor_pts_us_ = 0;
    int64_t frames_before_anchor_ = 0;
    int64_t submitted_frames_ = 0;
    int device_period_frames_ = 0;
    int64_t device_period_us_ = 0;
    int low_watermark_frames_ = 0;
    int high_watermark_frames_ = 0;

    void* audio_stream_ = nullptr;
    SwrContext* swr_ctx_ = nullptr;
    uint8_t* swr_buffer_ = nullptr;
    int swr_buffer_size_ = 0;
};

} // namespace pipeline
