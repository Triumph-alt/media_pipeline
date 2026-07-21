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
// AudioPlayNode: SDL3 音频播放节点（SinkNode 子类）
//
// onStreamInfo 中初始化 SDL 音频流、查询设备周期并据此设置背压水位，必要时创建 swr 做采样格式转换
// consume 中先做 SDL 提交背压（迟滞水位），再推数据到 SDL，最后用"已提交 − 队列积压 − 一个设备周期" 的真实消费进度更新主时钟。
// onDrain 中排空 swr 尾部、flush SDL，并等设备真正播完尾音后才放行 EOS。
// onStop 中关闭 SDL 音频流和释放 swr。
// ===================================================================
class AudioPlayNode final : public SinkNode {
public:
    explicit AudioPlayNode(const std::string& name);

protected:
    bool onReady() override { return true; }
    bool onStreamInfo() override;
    void consume(const Buffer* buf) override;
    void onDrain() override;
    void onStop() override;

private:
    // 音频背压：SDL 积压超过高水位则（取消感知地）等到低水位
    // 返回 false 表示被 stop 打断，调用方直接返回、不 put、不更新时钟
    bool waitForBufferSpace();
    // 统一执行 SDL 提交后的帧记账与 Clock 刷新；可选返回当前 queued 帧数
    // 返回 false 表示 queued 查询失败，且已通过 MessageBus 上报 ERROR
    bool updateClockFromQueued(SDL_AudioStream* stream, int64_t* queued_frames = nullptr);
    // SDL_PutAudioStreamData 成功后统一记账并刷新 Clock
    // 返回 false 表示提交或 queued 查询失败，且已通过 MessageBus 上报 ERROR
    bool submitAndTrack(SDL_AudioStream* stream, const uint8_t* data, int size);
    // 取消感知地等待 us 微秒（能被 stop_requested_ 打断）
    void waitCancellable(int64_t us);

    /* 音频格式 */
    int sample_rate_ = 0;
    int channels_ = 0;
    int bytes_per_sample_ = 0;                             // 一个 sample frame 的总字节数
    AVSampleFormat input_sample_fmt_ = AV_SAMPLE_FMT_NONE; // 输入格式

    /* 从锚定帧开始的消费进度 = submitted_frames_ - frames_before_anchor_ - SDL 中仍排队的帧数 */
    bool anchor_seen_ = false;         // 是否已经遇到首个有效音频 PTS
    int64_t anchor_pts_us_ = 0;        // 首个有效音频 PTS
    int64_t frames_before_anchor_ = 0; // 首个有效 PTS 对应 Buffer 提交前已经交给 SDL 的 NOPTS 前缀帧数
    int64_t submitted_frames_ = 0;     // 从开始到现在成功提交给 SDL 的全部 PCM 帧数

    /* 设备周期与背压水位（单位：源采样率帧数 / 微秒） */
    int device_period_frames_ = 0;  // 一个设备周期，多少 PCM 帧
    int64_t device_period_us_ = 0;  // 一个设备周期对应多少微秒
    int low_watermark_frames_ = 0;  // 低水位 LOW
    int high_watermark_frames_ = 0; // 高水位 HIGH = LOW + 迟滞带

    /* 后端资源 */
    void* audio_stream_ = nullptr;  // SDL AudioStream 和设备播放入口
    SwrContext* swr_ctx_ = nullptr; // swr 采样格式转换（输入非目标格式时使用）
    uint8_t* swr_buffer_ = nullptr; // 保存转换后的 PCM
    int swr_buffer_size_ = 0;       // 当前转换缓冲容量
};

} // namespace pipeline
