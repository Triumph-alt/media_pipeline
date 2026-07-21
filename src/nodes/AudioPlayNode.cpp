#include "pipeline/nodes/AudioPlayNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Clock.h"
#include "pipeline/core/Pipeline.h"

extern "C" {
#include <SDL3/SDL.h>
#include <libswresample/swresample.h>
#include <libavutil/opt.h>
#include <libavutil/channel_layout.h>
#include <libavutil/mathematics.h>
}

#include <algorithm>
#include <chrono>
#include <thread>

namespace pipeline {

// 背压 / 排水调参（命名常量，避免无出处的魔法数字）
// 单位说明：以"设备周期"为基本单位，小周期设备另加毫秒级绝对下限兜底。
namespace {
constexpr int kLowWatermarkPeriods = 3;   // N_low：欠载安全下限（设备周期数）
constexpr int kHysteresisPeriods   = 8;   // N_band：迟滞带宽（每次开闸的提交批量）
constexpr int kDrainWaitPeriods    = 3;   // EOS 尾音等待的设备周期数（覆盖 (b)+后端缓冲）
constexpr int kLowWatermarkFloorMs = 20;  // LOW 绝对下限（小周期设备护栏）
constexpr int kHysteresisFloorMs   = 80;  // 迟滞带绝对下限
constexpr int kWaitSliceMs         = 10;  // 取消感知等待的轮询粒度
}

// ===================================================================
// 构造函数
// ===================================================================
AudioPlayNode::AudioPlayNode(const std::string& name)
    : SinkNode(name) {
    addSinkPad("in", TemplateCaps{{MediaType::AUDIO_RAW}});
}

// ===================================================================
// onStreamInfo: 初始化 SDL 音频流，查询设备周期并设置背压水位，必要时建 swr
//
// 目标格式：S16（SDL3 广泛支持的格式）。输入格式从 CapsEvent 获取，非 S16 走 swr。
// 设备周期 P = sample_frames / 设备频率，是时钟 (b) 项、背压水位和 drain 尾音
// 等待的共同时间基准；查询失败按 10ms 兜底。
// ===================================================================
bool AudioPlayNode::onStreamInfo() {
    // 接收上游 Caps 然后保存
    if (!receiveCapsEvent("in")) {
        return false;
    }
    const CapsEvent& caps = negotiated_caps_["in"];

    sample_rate_ = caps.sample_rate;
    channels_ = caps.channels;
    input_sample_fmt_ = caps.sample_fmt;

    // 目标格式：S16
    constexpr int target_bytes_per_sample = 2;  // S16 = 2 bytes
    bytes_per_sample_ = target_bytes_per_sample * channels_;

    // 初始化 SDL 音频子系统
    if (!SDL_InitSubSystem(SDL_INIT_AUDIO)) {
        postMessage(MessageType::ERROR, "AudioPlayNode: SDL_InitSubSystem(SDL_INIT_AUDIO) failed");
        return false;
    }

    // 描述 AudioPlayNode 交给 SDL 的数据格式
    SDL_AudioSpec spec;
    SDL_zero(spec);
    spec.format = SDL_AUDIO_S16;
    spec.channels = channels_;
    spec.freq = sample_rate_;

    // 打开默认播放设备创建 AudioStream，并把 AudioStream 绑定到该设备
    audio_stream_ = SDL_OpenAudioDeviceStream(
        SDL_AUDIO_DEVICE_DEFAULT_PLAYBACK, &spec, nullptr, nullptr);
    if (!audio_stream_) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_OpenAudioDeviceStream failed: ") + SDL_GetError());
        return false;
    }
    auto* stream = static_cast<SDL_AudioStream*>(audio_stream_);

    /* 计算设备周期：SDL 每次交给设备的帧数除以设备每秒播放的帧数（采样率） */
    SDL_AudioSpec dev_spec;    // 物理设备实际采样率
    int dev_sample_frames = 0; // SDL 每次向设备提交的一块数据包含多少帧

    /* 找到 AudioStream 绑定的音频设备，然后查询设备实际采样率和 SDL 每次向设备喂多少帧 */
    if (SDL_GetAudioDeviceFormat(SDL_GetAudioStreamDevice(stream), &dev_spec, &dev_sample_frames) &&
        dev_spec.freq > 0 && dev_sample_frames > 0) {
        // 一个设备周期换算成 AudioPlayNode 输入侧采样率后，对应多少 PCM frame
        device_period_frames_ = static_cast<int>(
            static_cast<int64_t>(dev_sample_frames) * sample_rate_ / dev_spec.freq);
        // 把一个设备周期换算成微秒
        device_period_us_ = static_cast<int64_t>(dev_sample_frames) * 1000000LL / dev_spec.freq;
    } else {
        // 如果 SDL 查询失败，就假设一个设备周期是 10ms
        device_period_frames_ = sample_rate_ / 100;
        device_period_us_ = 10000;
    }
    if (device_period_frames_ <= 0) {
        device_period_frames_ = sample_rate_ / 100;
    }

    // 背压水位（帧）：以设备周期为单位推导，小周期设备加绝对下限
    const int low_floor  = sample_rate_ * kLowWatermarkFloorMs / 1000;
    const int band_floor = sample_rate_ * kHysteresisFloorMs / 1000;
    low_watermark_frames_  = std::max(kLowWatermarkPeriods * device_period_frames_, low_floor);
    high_watermark_frames_ = low_watermark_frames_ +
                             std::max(kHysteresisPeriods * device_period_frames_, band_floor);

    // 恢复播放设备
    if (!SDL_ResumeAudioStreamDevice(stream)) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_ResumeAudioStreamDevice failed: ") + SDL_GetError());
        return false;
    }

    // 如果输入格式不是 S16，创建 swr 转换上下文
    if (input_sample_fmt_ != AV_SAMPLE_FMT_S16) {
        // 建立声道布局
        AVChannelLayout out_ch_layout = AV_CHANNEL_LAYOUT_STEREO;
        if (channels_ != 2) {
            av_channel_layout_default(&out_ch_layout, channels_);
        }

        int ret = swr_alloc_set_opts2(
            &swr_ctx_,
            &out_ch_layout, AV_SAMPLE_FMT_S16, sample_rate_,        // 输出
            &out_ch_layout, input_sample_fmt_, sample_rate_,         // 输入
            0, nullptr);
        if (channels_ != 2) {
            av_channel_layout_uninit(&out_ch_layout);
        }
        if (ret < 0 || !swr_ctx_) {
            postMessage(MessageType::ERROR, "AudioPlayNode: swr_alloc_set_opts2 failed");
            return false;
        }

        ret = swr_init(swr_ctx_);
        if (ret < 0) {
            postMessage(MessageType::ERROR, "AudioPlayNode: swr_init failed");
            return false;
        }
    }

    // 音频时钟：标记有音频（策略位），锚点在首个有效 Buffer 时确定
    pipeline_->clock()->setHasAudio(true);
    anchor_seen_ = false;
    anchor_pts_us_ = 0;
    frames_before_anchor_ = 0;
    submitted_frames_ = 0;

    return true;
}

// ===================================================================
// waitForBufferSpace: 背压闸门（双阈值迟滞）
//
// SDL 内部缓冲对 App 无界；此处用设备周期水位把它限住，借"晚 ack"把背压
// 沿 Route 传导到上游。积压 > HIGH 时取消感知地等到 ≤ LOW。
// 返回 false 表示被 stop 打断或出错（调用方不 put、不更新时钟）。
// ===================================================================
bool AudioPlayNode::waitForBufferSpace() {
    // 检查 AudioStream 是否存在
    auto* stream = static_cast<SDL_AudioStream*>(audio_stream_);
    if (!stream) {
        return false;
    }

    // 检查 Pipeline 是否正在停止
    if (stop_requested_.load()) {
        return false;
    }

    // 查询 SDL 当前积压了多少字节音频
    int queued_bytes = SDL_GetAudioStreamQueued(stream);
    if (queued_bytes < 0) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_GetAudioStreamQueued failed: ") + SDL_GetError());
        return false;
    }
    // 计算 SDL 当前积压了多少帧音频
    int queued_frames = queued_bytes / bytes_per_sample_;

    // 未超高水位，直接放行
    if (queued_frames <= high_watermark_frames_) {
        return true;   
    }

    // 超过高水位：取消感知地等积压降到低水位（迟滞带保证成批提交，避免逐块抖动）
    while (queued_frames > low_watermark_frames_) {
        if (stop_requested_.load()) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kWaitSliceMs));
        queued_bytes = SDL_GetAudioStreamQueued(stream);
        if (queued_bytes < 0) {
            postMessage(MessageType::ERROR,
                        std::string("AudioPlayNode: SDL_GetAudioStreamQueued failed: ") + SDL_GetError());
            return false;
        }
        queued_frames = queued_bytes / bytes_per_sample_;
    }
    return !stop_requested_.load();
}

bool AudioPlayNode::updateClockFromQueued(SDL_AudioStream* stream,
                                          int64_t* queued_frames_out) {
    if (!stream) {
        postMessage(MessageType::ERROR,
                    "AudioPlayNode: cannot update clock without an audio stream");
        return false;
    }

    // 如果正常提交 PCM 时还没遇到有效 PTS：只累计 submitted_frames_，不查询 queued，不更新 Clock
    // 但 drain 会传入 queued_frames_out，即使没有 PTS，也要查询队列，以便等待 SDL 排空
    if (!anchor_seen_ && !queued_frames_out) {
        return true;
    }

    // 查询 SDL 剩余积压
    const int queued_bytes = SDL_GetAudioStreamQueued(stream);
    if (queued_bytes < 0) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_GetAudioStreamQueued failed: ") + SDL_GetError());
        return false;
    }

    // 计算已经成功提交，但尚未被 SDL 设备读取路径取走的 PCM 帧数
    const int64_t queued_frames = queued_bytes / bytes_per_sample_;
    if (queued_frames_out) {
        *queued_frames_out = queued_frames;
    }

    // 没有锚点时不更新 Clock，但传入 queued_frames_out 时也要返回 queued，让 drain 能够等待设备排空
    if (!anchor_seen_) {
        return true;
    }

    // 锚定帧之后的消费进度 = 到现在成功提交给 SDL 的全部帧 - 锚定帧之前已经提交的 NOPTS 前缀 - 还没被 SDL 取走的全部帧
    const int64_t consumed_from_anchor =
        submitted_frames_ - frames_before_anchor_ - queued_frames;
    if (consumed_from_anchor < 0) {
        // 负数表示 SDL 虽然已经收到锚定帧，但设备还在消费锚定帧之前的 NOPTS 前缀
        // 此时 Clock 保持未锚定，VideoRender 仍按未锚定策略呈现，因此前缀窗口内可能短暂不同步
        return true;
    }

    // 刚取走的数据可能仍处于当前设备周期中还没有真正到达可听端，因此再减去一个设备周期作为估计修正
    int64_t audible = consumed_from_anchor - device_period_frames_;
    if (audible < 0) {
        // 如果 consumed ∈ [0, device_period] ，采样点上的 audible 被钳为 0，
        // 音频位置不早于 anchor_pts_us_；两次采样之间 Clock 仍按墙钟插值
        audible = 0;
    }

    // 这里假设 swr 不改变采样率，submitted/queued 与 PTS 的帧映射为 1:1；
    // 真正重采样时必须重新定义输出帧到媒体 PTS 的换算关系
    const int64_t clock_us =
        anchor_pts_us_ + audible * 1000000LL / sample_rate_;

    // 把位置交给 Pipeline Clock，VideoRenderNode 随后以它为同步基准
    pipeline_->clock()->setAudioPosition(clock_us);
    return true;
}

bool AudioPlayNode::submitAndTrack(SDL_AudioStream* stream,
                                   const uint8_t* data, int size) {
    // 向 SDL 提交 PCM
    if (!SDL_PutAudioStreamData(stream, data, size)) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_PutAudioStreamData failed: ") + SDL_GetError());
        return false;
    }

    // 提交成功后才计入全程输出帧账本；锚定前也须完整记账，因为 frames_before_anchor_ 要扣除这些 NOPTS 前缀
    submitted_frames_ += size / bytes_per_sample_;

    // 根据最新 SDL 队列刷新 Clock
    return updateClockFromQueued(stream);
}

// ===================================================================
// consume: 背压闸门 → 推数据到 SDL → 用真实消费进度更新主时钟
//
// 锚定判定必须先于本 Buffer 的输出帧计入 submitted_frames_，否则锚定帧
// 自身会被写入 frames_before_anchor_，导致时钟恒定滞后一个 Buffer 时长。
// ===================================================================
void AudioPlayNode::consume(const Buffer* buf) {
    if (!buf || buf->size == 0) {
        return;
    }

    // 只在遇到第一个真正有效的音频 PTS 时才锚定；此前 NOPTS 帧照常 put，但不参与锚点判定
    // 未锚定期间 Clock 返回 kUnanchored，VideoRenderNode 会"未锚定立即呈现"，不会卡死
    if (!anchor_seen_ && buf->pts != AV_NOPTS_VALUE) {
        // 已经找到首个有效 PTS
        anchor_seen_ = true;
        // 保存该 Buffer 第一帧 PCM 对应的媒体时间
        anchor_pts_us_ = buf->pts;
        // 保存当前 Buffer 提交前已经向 SDL 提交了多少 NOPTS 帧,不包含锚定 Buffer 自己
        frames_before_anchor_ = submitted_frames_;
    }

    // 背压，等待期间发生 stop 或 SDL 查询错误直接返回，不再提交当前 Buffer
    if (!waitForBufferSpace()) {
        return;
    }

    // 获取 SDL AudioStream
    auto* stream = static_cast<SDL_AudioStream*>(audio_stream_);
    if (!stream) {
        return;
    }

    const uint8_t* data = buf->data;        // 解码器输出 PCM 的地址
    int size = static_cast<int>(buf->size); // 解码器输出 PCM 的字节数

    uint8_t* converted[1] = {};             // 输出 plane 指针数组，S16 packed 只有一个 plane
    int converted_size = 0;                 // 本次预计需要的输出缓冲容量，单位是字节

    // 格式转换（如需）
    if (swr_ctx_) {
        // 计算输入每个单声道 sample 的字节数
        int in_bytes_per_sample = av_get_bytes_per_sample(input_sample_fmt_);
        // 计算这个 Buffer 包含多少 PCM frame
        int nb_samples = static_cast<int>(buf->size) / (in_bytes_per_sample * channels_);

        // 计算输出需要多少字节：帧数 * 每一帧的字节数
        converted_size = nb_samples * bytes_per_sample_;
        if (converted_size > swr_buffer_size_) {
            // 如果当前容量不足，需要释放旧缓冲分配更大的缓冲
            av_free(swr_buffer_);
            swr_buffer_ = static_cast<uint8_t*>(av_malloc(converted_size + AV_INPUT_BUFFER_PADDING_SIZE));
            swr_buffer_size_ = converted_size;
        }

        // 把缓冲地址交给输出 plane
        converted[0] = swr_buffer_;

        // 准备输入 plane 指针
        const uint8_t* in_data[AV_NUM_DATA_POINTERS] = {};
        if (av_sample_fmt_is_planar(input_sample_fmt_)) {
            // 如果输入是 planar
            int plane_size = nb_samples * in_bytes_per_sample;
            for (int i = 0; i < channels_; i++) {
                in_data[i] = buf->data + i * plane_size;
            }
        } else {
            // 如果输入是 packed
            in_data[0] = buf->data;
        }

        // 执行转换，返回实际输出帧数
        int converted_samples = swr_convert(swr_ctx_, converted, nb_samples,
                                             in_data, nb_samples);
        if (converted_samples < 0) {
            postMessage(MessageType::ERROR, "AudioPlayNode: swr_convert failed");
            return;
        }

        // 指向转换后的 S16 packed 缓冲和实际输出的大小
        data = swr_buffer_;
        size = converted_samples * bytes_per_sample_;
    }

    if (!submitAndTrack(stream, data, size)) {
        return;
    }
}

// ===================================================================
// onDrain: 上报最终 EOS 前，等设备真正播完此前提交的音频
//
// 从上游收到 EOSEvent 不等同于真正播放完成，不能直接上报 EOS
// 还需要排空 swr 尾部、flush SDL，等输入队列被设备吃空，再等几个设备周期覆盖设备缓冲 + 后端缓冲的尾音
// 主动 stop 时立即返回，不等尾音，SDL 缓冲随 onStop 销毁，截尾可接受
// ===================================================================
void AudioPlayNode::onDrain() {
    if (stop_requested_.load()) {
        // 主动停不等尾音
        return;   
    }

    // 获取 SDL AudioStream 供后续 swr 尾部数据提交使用
    auto* stream = static_cast<SDL_AudioStream*>(audio_stream_);
    if (!stream) {
        return;
    }

    // 排空重采样器尾部（真重采样时按 swr_get_delay 估算输出容量）
    if (swr_ctx_) {
        // 查询 swr 内部还保留多少个当前采样率下的 PCM frame
        const int64_t delay = swr_get_delay(swr_ctx_, sample_rate_);
        if (delay > 0) {
            // 计算尾部输出缓冲容量还有多少帧
            const int out_samples = static_cast<int>(
                av_rescale_rnd(delay, sample_rate_, sample_rate_, AV_ROUND_UP));

            // 计算尾部输出缓冲容量所需字节数
            const int out_bytes = out_samples * bytes_per_sample_;
            if (out_bytes > swr_buffer_size_) {
                av_free(swr_buffer_);
                swr_buffer_ = static_cast<uint8_t*>(av_malloc(out_bytes + AV_INPUT_BUFFER_PADDING_SIZE));
                swr_buffer_size_ = out_bytes;
            }
            if (!swr_buffer_) {
                postMessage(MessageType::ERROR, "AudioPlayNode::onDrain: drain buffer alloc failed");
                return;
            }

            uint8_t* out[1] = { swr_buffer_ };
            // 不再提供新输入，把内部已经缓存的数据尽量输出
            const int got = swr_convert(swr_ctx_, out, out_samples, nullptr, 0);
            if (got < 0) {
                // swr 尾部转换失败，不能当作"已排空"
                postMessage(MessageType::ERROR, "AudioPlayNode::onDrain: swr_convert flush failed");
                return;
            }
            // 尾部数据必须走统一提交入口，确保 submitted_frames_ 与 Clock 账本一致
            if (got > 0 && !submitAndTrack(stream, swr_buffer_, got * bytes_per_sample_)) {
                return;
            }
        }
    }

    // 结束 SDL AudioStream 的输入阶段，让内部残留数据可以继续被设备消费
    if (!SDL_FlushAudioStream(stream)) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode::onDrain: SDL_FlushAudioStream failed: ") + SDL_GetError());
        return;
    }

    // 等设备把输入队列吃空；每次查询都刷新 Clock，覆盖 consumed_from_anchor 从负数转为非负数的边界窗口
    while (!stop_requested_.load()) {
        int64_t queued_frames = 0;
        if (!updateClockFromQueued(stream, &queued_frames)) {
            return;
        }
        if (queued_frames == 0) {
            // SDL AudioStream 输入队列中数据已经全部被设备取走
            break;
        }

        // 每次等待 10ms
        std::this_thread::sleep_for(std::chrono::milliseconds(kWaitSliceMs));
    }

    // 再等几个设备周期，覆盖设备周期缓冲 + 后端缓冲的尾音，不可观测，按上界等待
    waitCancellable(kDrainWaitPeriods * device_period_us_);
}

void AudioPlayNode::waitCancellable(int64_t us) {
    const int64_t slice = static_cast<int64_t>(kWaitSliceMs) * 1000;
    while (us > 0 && !stop_requested_.load()) {
        // 分成最多 10ms 的小段沉睡，期间允许主动停止
        const int64_t s = std::min(us, slice);
        std::this_thread::sleep_for(std::chrono::microseconds(s));
        us -= s;
    }
}

// ===================================================================
// onStop: 关闭 SDL 音频流和释放 swr
//
// 必须支持部分初始化状态。自然结束路径 drain 已清空 SDL 缓冲；
// 主动停路径 drain 被取消，未播完的数据随流销毁（可接受）。
// ===================================================================
void AudioPlayNode::onStop() {
    if (audio_stream_) {
        SDL_DestroyAudioStream(static_cast<SDL_AudioStream*>(audio_stream_));
        audio_stream_ = nullptr;
    }
    if (swr_buffer_) {
        av_free(swr_buffer_);
        swr_buffer_ = nullptr;
        swr_buffer_size_ = 0;
    }
    if (swr_ctx_) {
        swr_free(&swr_ctx_);
    }
    SDL_QuitSubSystem(SDL_INIT_AUDIO);
}

} // namespace pipeline
