#include "pipeline/nodes/AudioPlayNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Clock.h"
#include "pipeline/core/Pipeline.h"

extern "C" {
#include <SDL3/SDL.h>
#include <libswresample/swresample.h>
#include <libavutil/channel_layout.h>
#include <libavutil/mathematics.h>
#include <libavutil/samplefmt.h>
}

#include <algorithm>
#include <climits>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

namespace pipeline {

namespace {
constexpr int kLowWatermarkPeriods = 3;
constexpr int kHysteresisPeriods = 8;
constexpr int kDrainWaitPeriods = 3;
constexpr int kLowWatermarkFloorMs = 20;
constexpr int kHysteresisFloorMs = 80;
constexpr int kWaitSliceMs = 10;

bool isStandardChannelLayout(const ChannelLayout& layout) {
    // AudioPlay 当前固定输出为 stereo，允许 swr 对 FFmpeg 预定义的 native layout
    // 使用标准 downmix/upmix 矩阵；CUSTOM、UNSPEC、Ambisonic 没有本节点承诺的
    // 映射语义，必须在重配边界拒绝，不能仅按声道数猜默认布局。
    if (layout.order != AV_CHANNEL_ORDER_NATIVE || !layout.isValid()) {
        return false;
    }

    AVChannelLayout candidate{};
    if (!layout.toAV(&candidate)) {
        return false;
    }

    void* opaque = nullptr;
    bool found = false;
    while (const AVChannelLayout* standard = av_channel_layout_standard(&opaque)) {
        if (av_channel_layout_compare(&candidate, standard) == 0) {
            found = true;
            break;
        }
    }
    av_channel_layout_uninit(&candidate);
    return found;
}

} // namespace

AudioPlayNode::AudioPlayNode(const std::string& name)
    : SinkNode(name) {
    addSinkPad("in", TemplateCaps{{MediaType::AUDIO_RAW}});
}

bool AudioPlayNode::openCanonicalStream() {
    if (!SDL_InitSubSystem(SDL_INIT_AUDIO)) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_InitSubSystem(SDL_INIT_AUDIO) failed: ") + SDL_GetError());
        return false;
    }

    // 默认设备打开前给出的只是建议格式，我们只取建议采样率，应用侧提交格式始终固定为 S16 立体声
    // 上游 Caps 或物理设备变化都不改变 canonical 格式，SDL 负责 canonical 到物理设备的末段转换
    SDL_AudioSpec preferred_spec;
    SDL_zero(preferred_spec);
    if (!SDL_GetAudioDeviceFormat(SDL_AUDIO_DEVICE_DEFAULT_PLAYBACK, &preferred_spec, nullptr) ||
        preferred_spec.freq <= 0) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_GetAudioDeviceFormat failed: ") + SDL_GetError());
        return false;
    }

    canonical_rate_ = preferred_spec.freq;
    SDL_AudioSpec canonical_spec;
    SDL_zero(canonical_spec);
    canonical_spec.format = SDL_AUDIO_S16;
    canonical_spec.channels = kCanonicalChannels;
    canonical_spec.freq = canonical_rate_;

    audio_stream_ = SDL_OpenAudioDeviceStream(
        SDL_AUDIO_DEVICE_DEFAULT_PLAYBACK, &canonical_spec, nullptr, nullptr);
    if (!audio_stream_) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_OpenAudioDeviceStream failed: ") + SDL_GetError());
        return false;
    }

    auto* stream = static_cast<SDL_AudioStream*>(audio_stream_);
    SDL_AudioSpec device_spec;
    SDL_zero(device_spec);
    int physical_period_frames = 0;
    if (!SDL_GetAudioDeviceFormat(SDL_GetAudioStreamDevice(stream), &device_spec,
                                  &physical_period_frames) ||
        device_spec.freq <= 0 || physical_period_frames <= 0) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_GetAudioDeviceFormat for opened device failed: ") +
                        SDL_GetError());
        return false;
    }

    // 物理设备周期只在 Ready 时换算一次，后续水位、SDL queued 字节换算和 Clock 全部以 canonical 帧为量纲
    // 输入 Caps 不改变这些设备侧账本
    device_period_us_ = static_cast<int64_t>(physical_period_frames) * 1000000LL / device_spec.freq;
    device_period_frames_ = static_cast<int>(
        (static_cast<int64_t>(physical_period_frames) * canonical_rate_ + device_spec.freq - 1) /
        device_spec.freq);
    if (device_period_frames_ <= 0 || device_period_us_ <= 0) {
        postMessage(MessageType::ERROR, "AudioPlayNode: invalid canonical device period");
        return false;
    }

    const int low_floor = canonical_rate_ * kLowWatermarkFloorMs / 1000;
    const int band_floor = canonical_rate_ * kHysteresisFloorMs / 1000;
    low_watermark_frames_ = std::max(kLowWatermarkPeriods * device_period_frames_, low_floor);
    high_watermark_frames_ = low_watermark_frames_ +
                             std::max(kHysteresisPeriods * device_period_frames_, band_floor);

    if (!SDL_ResumeAudioStreamDevice(stream)) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_ResumeAudioStreamDevice failed: ") + SDL_GetError());
        return false;
    }

    // 设备就绪后才允许音频播放
    // anchor 和提交账本必须与本轮新建的 canonical Stream 同时归零
    anchor_seen_ = false;
    anchor_pts_us_ = 0;
    frames_before_anchor_ = 0;
    submitted_frames_ = 0;
    pipeline_->clock()->setHasAudio(true);
    return true;
}

bool AudioPlayNode::onReady() {
    return openCanonicalStream();
}

bool AudioPlayNode::ensureSwrBuffer(int output_frames) {
    if (output_frames < 0 ||
        output_frames > INT_MAX / canonical_bytes_per_frame_) {
        postMessage(MessageType::ERROR, "AudioPlayNode: resample output buffer size overflow");
        return false;
    }

    // 此缓冲只承载 swr 输出的 canonical PCM
    // 容量按帧数乘 canonical_bytes_per_frame_ 计算，输入采样率和布局不能参与这里的大小计算
    const int required_bytes = output_frames * canonical_bytes_per_frame_;
    if (required_bytes <= swr_buffer_size_) {
        return true;
    }

    uint8_t* replacement = static_cast<uint8_t*>(av_malloc(required_bytes));
    if (!replacement) {
        postMessage(MessageType::ERROR, "AudioPlayNode: resample output buffer allocation failed");
        return false;
    }

    av_free(swr_buffer_);
    swr_buffer_ = replacement;
    swr_buffer_size_ = required_bytes;
    return true;
}

bool AudioPlayNode::drainSwr() {
    if (!swr_ctx_) {
        return true;
    }

    // null 输入告诉 swr 不会再有当前格式的输入帧
    // 循环取走重采样滤波器内仍可形成完整 canonical 帧的尾部
    // 这条路径同时服务于运行期 Caps 重配和最终 EOS drain
    auto* stream = static_cast<SDL_AudioStream*>(audio_stream_);
    while (!stop_requested_.load()) {
        // 以 canonical_rate_ 取 delay，返回值已经是后续提交账本和 SDL 队列使用的帧单位
        const int64_t delay_frames = swr_get_delay(swr_ctx_, canonical_rate_);
        if (delay_frames < 0 || delay_frames > INT_MAX) {
            postMessage(MessageType::ERROR, "AudioPlayNode: swr_get_delay failed while draining");
            return false;
        }
        if (delay_frames == 0) {
            return true;
        }
        if (!ensureSwrBuffer(static_cast<int>(delay_frames))) {
            return false;
        }

        uint8_t* output[1] = {swr_buffer_};
        const int produced = swr_convert(swr_ctx_, output, static_cast<int>(delay_frames), nullptr, 0);
        if (produced < 0) {
            postMessage(MessageType::ERROR, "AudioPlayNode: swr_convert drain failed");
            return false;
        }
        if (produced == 0) {
            // swr_get_delay 可能保留不足一个 canonical 帧的分数滤波延迟
            // null 输入转换返回零表示已无完整 PCM 可提交，此时 drain 正常完成
            return true;
        }
        if (!waitForBufferSpace() ||
            !submitAndTrack(stream, swr_buffer_, produced * canonical_bytes_per_frame_)) {
            return false;
        }
    }
    return false;
}

bool AudioPlayNode::configureInputResampler(const CapsEvent& caps) {
    // 消费侧校验 AudioPlay 建 swr 所需的字段：采样率、样本格式、标准声道布局(isStandardChannelLayout 已含有效性)。
    if (caps.media_type != MediaType::AUDIO_RAW || caps.sample_rate <= 0 ||
        caps.sample_fmt == AV_SAMPLE_FMT_NONE || !isStandardChannelLayout(caps.channel_layout)) {
        postMessage(MessageType::ERROR,
                    "AudioPlayNode: only standard native input channel layouts with valid rate/format are supported");
        return false;
    }

    // 当前 Caps 是输入侧格式，canonical 输出格式不变
    // 先构造 replacement，全部成功后才替换旧 swr，失败不会破坏仍可继续使用的旧配置
    AVChannelLayout input_layout{};
    if (!caps.channel_layout.toAV(&input_layout)) {
        postMessage(MessageType::ERROR, "AudioPlayNode: failed to materialize input channel layout");
        return false;
    }

    // 输出端固定为 canonical S16 立体声
    // 输入端完全由当前 AudioRaw Caps 定义，因此格式边界只重建 swr 而不重开 SDL 设备
    AVChannelLayout output_layout = AV_CHANNEL_LAYOUT_STEREO;
    SwrContext* replacement = nullptr;
    const int alloc_result = swr_alloc_set_opts2(
        &replacement,
        &output_layout, AV_SAMPLE_FMT_S16, canonical_rate_,
        &input_layout, caps.sample_fmt, caps.sample_rate,
        0, nullptr);
    av_channel_layout_uninit(&input_layout);
    if (alloc_result < 0 || !replacement) {
        postMessage(MessageType::ERROR, "AudioPlayNode: swr_alloc_set_opts2 failed");
        return false;
    }
    if (swr_init(replacement) < 0) {
        swr_free(&replacement);
        postMessage(MessageType::ERROR, "AudioPlayNode: swr_init failed");
        return false;
    }

    // replacement 已初始化成功后才释放旧 swr
    // active Caps 更新和其 Delivery 的 ack 由外层 applyCapsEvent 在本函数成功返回后完成
    swr_free(&swr_ctx_);
    swr_ctx_ = replacement;
    input_rate_ = caps.sample_rate;
    input_sample_fmt_ = caps.sample_fmt;
    input_layout_ = caps.channel_layout;
    input_channels_ = caps.channel_layout.channels;
    return true;
}

bool AudioPlayNode::onCaps(const std::string&, const CapsEvent& caps,
                           std::vector<QueueItem>*) {
    // 当前 Caps 是一条有序重配置边界
    // 先把旧 swr 的尾部转换并提交到固定 canonical SDL 队列
    // 队列不清空，因为旧新两段 PCM 的提交格式完全相同，连续播放才不会截断音频
    if (!drainSwr() || stop_requested_.load()) {
        return false;
    }
    return configureInputResampler(caps);
}

void AudioPlayNode::runLoop() {
    SinkNode::runLoop();
    // Pipeline creates this worker through std::thread; clean SDL thread-local state before returning.
    SDL_CleanupTLS();
}

bool AudioPlayNode::waitForBufferSpace() {
    auto* stream = static_cast<SDL_AudioStream*>(audio_stream_);
    if (!stream || stop_requested_.load()) {
        return false;
    }

    // queued 是 SDL 输入侧尚未被设备读取的 canonical 字节数
    // 高水位以上先等待到低水位，形成迟滞，避免每个小 Buffer 都在阈值附近反复睡眠和唤醒
    int queued_bytes = SDL_GetAudioStreamQueued(stream);
    if (queued_bytes < 0) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_GetAudioStreamQueued failed: ") + SDL_GetError());
        return false;
    }
    int queued_frames = queued_bytes / canonical_bytes_per_frame_;
    while (queued_frames > high_watermark_frames_) {
        if (stop_requested_.load()) {
            return false;
        }
        do {
            std::this_thread::sleep_for(std::chrono::milliseconds(kWaitSliceMs));
            queued_bytes = SDL_GetAudioStreamQueued(stream);
            if (queued_bytes < 0) {
                postMessage(MessageType::ERROR,
                            std::string("AudioPlayNode: SDL_GetAudioStreamQueued failed: ") + SDL_GetError());
                return false;
            }
            queued_frames = queued_bytes / canonical_bytes_per_frame_;
        } while (queued_frames > low_watermark_frames_ && !stop_requested_.load());
    }
    return !stop_requested_.load();
}

bool AudioPlayNode::updateClockFromQueued(SDL_AudioStream* stream, int64_t* queued_frames_out) {
    if (!stream) {
        postMessage(MessageType::ERROR, "AudioPlayNode: cannot update Clock without AudioStream");
        return false;
    }

    if (!anchor_seen_ && !queued_frames_out) {
        return true;
    }

    const int queued_bytes = SDL_GetAudioStreamQueued(stream);
    if (queued_bytes < 0) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_GetAudioStreamQueued failed: ") + SDL_GetError());
        return false;
    }
    const int64_t queued_frames = queued_bytes / canonical_bytes_per_frame_;
    if (queued_frames_out) {
        *queued_frames_out = queued_frames;
    }
    if (!anchor_seen_) {
        return true;
    }

    // submitted_frames_ 和 queued_frames_ 都是 canonical 帧
    // 两者相减得到设备读路径已经消耗的帧数，扣掉锚点前 NOPTS 前缀后才是媒体时间轴上的进度
    const int64_t consumed_from_anchor =
        submitted_frames_ - frames_before_anchor_ - queued_frames;
    if (consumed_from_anchor < 0) {
        return true;
    }

    // SDL 不暴露硬件末端缓冲深度
    // 保守减去一个已换算到 canonical 的设备周期，避免 Clock 把尚未可听的帧当成已播放
    const int64_t audible_frames = std::max<int64_t>(0, consumed_from_anchor - device_period_frames_);
    const int64_t clock_us =
        anchor_pts_us_ + audible_frames * 1000000LL / canonical_rate_;
    pipeline_->clock()->setAudioPosition(clock_us);
    return true;
}

bool AudioPlayNode::submitAndTrack(SDL_AudioStream* stream, const uint8_t* data, int size) {
    if (!data || size <= 0 || size % canonical_bytes_per_frame_ != 0) {
        postMessage(MessageType::ERROR, "AudioPlayNode: invalid canonical PCM submission");
        return false;
    }
    if (!SDL_PutAudioStreamData(stream, data, size)) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_PutAudioStreamData failed: ") + SDL_GetError());
        return false;
    }

    // SDL 成功接管 canonical PCM 后才增加提交账本
    // 随后立刻采样 queued 并重锚 Clock，避免背压等待期间只靠墙钟外推
    submitted_frames_ += size / canonical_bytes_per_frame_;
    return updateClockFromQueued(stream);
}

bool AudioPlayNode::convertAndSubmit(const Buffer* buf) {
    if (!swr_ctx_) {
        postMessage(MessageType::ERROR, "AudioPlayNode: Buffer received without configured resampler");
        return false;
    }
    const auto* meta = std::get_if<AudioRawMeta>(&buf->meta);
    if (!meta || meta->nb_samples <= 0 || !buf->data) {
        postMessage(MessageType::ERROR, "AudioPlayNode: invalid AudioRaw Buffer metadata");
        return false;
    }

    // BufferMeta 只提供本帧的 nb_samples
    // sample_fmt、声道数和 planar/packed 布局都必须服从最近成功应用的 active Caps
    const int input_bytes_per_sample = av_get_bytes_per_sample(input_sample_fmt_);
    if (input_bytes_per_sample <= 0 || input_channels_ <= 0) {
        postMessage(MessageType::ERROR, "AudioPlayNode: invalid active input sample format");
        return false;
    }

    const size_t expected_bytes = static_cast<size_t>(meta->nb_samples) *
                                  input_bytes_per_sample * input_channels_;
    if (expected_bytes != buf->size) {
        postMessage(MessageType::ERROR,
                    "AudioPlayNode: AudioRaw Buffer size does not match active Caps and nb_samples");
        return false;
    }

    // fromAVFrame 当前把 planar 数据按 channel plane 顺序紧密拼接
    // packed 格式只有一个交错平面，swr 输入指针数组必须按两种布局分别构造
    std::vector<const uint8_t*> input_planes;
    if (av_sample_fmt_is_planar(input_sample_fmt_)) {
        const size_t plane_bytes = static_cast<size_t>(meta->nb_samples) * input_bytes_per_sample;
        input_planes.reserve(static_cast<size_t>(input_channels_));
        for (int channel = 0; channel < input_channels_; ++channel) {
            input_planes.push_back(buf->data + static_cast<size_t>(channel) * plane_bytes);
        }
    } else {
        input_planes.push_back(buf->data);
    }

    // swr 的历史 delay 先以输入采样率表示，再与本帧输入样本一起按比例上取整到 canonical 帧
    // 这个容量保证 swr_convert 不会因输出空间不足而把本应立即提交的样本滞留在内部 FIFO
    const int64_t delay_at_input_rate = swr_get_delay(swr_ctx_, input_rate_);
    if (delay_at_input_rate < 0 || delay_at_input_rate > INT_MAX - meta->nb_samples) {
        postMessage(MessageType::ERROR, "AudioPlayNode: swr_get_delay failed before conversion");
        return false;
    }
    const int output_capacity = static_cast<int>(av_rescale_rnd(
        delay_at_input_rate + meta->nb_samples, canonical_rate_, input_rate_, AV_ROUND_UP));
    if (output_capacity <= 0 || !ensureSwrBuffer(output_capacity)) {
        return false;
    }

    uint8_t* output[1] = {swr_buffer_};
    const int produced = swr_convert(swr_ctx_, output, output_capacity,
                                     input_planes.data(), meta->nb_samples);
    if (produced < 0) {
        postMessage(MessageType::ERROR, "AudioPlayNode: swr_convert failed");
        return false;
    }
    if (produced == 0) {
        return true;
    }

    auto* stream = static_cast<SDL_AudioStream*>(audio_stream_);
    return waitForBufferSpace() &&
           submitAndTrack(stream, swr_buffer_, produced * canonical_bytes_per_frame_);
}

void AudioPlayNode::consume(const Buffer* buf) {
    if (!buf || buf->media_type != MediaType::AUDIO_RAW || buf->size == 0) {
        postMessage(MessageType::ERROR, "AudioPlayNode: received invalid AudioRaw Buffer");
        return;
    }

    // 当前 Buffer 尚未送入 swr 时取 delay
    // 因此 delay 只代表此前 NOPTS 前缀残留的 canonical 输出，锚点才能准确落在首个有效 PTS 帧之前
    if (!anchor_seen_ && buf->pts != AV_NOPTS_VALUE) {
        const int64_t pending_prefix_frames = swr_ctx_ ? swr_get_delay(swr_ctx_, canonical_rate_) : 0;
        if (pending_prefix_frames < 0) {
            postMessage(MessageType::ERROR, "AudioPlayNode: swr_get_delay failed while anchoring Clock");
            return;
        }
        anchor_seen_ = true;
        anchor_pts_us_ = buf->pts;
        // submitted_frames_ 是已经进入 SDL 队列的前缀
        // pending_prefix_frames 是尚在 swr 中、时间上仍位于锚点前的前缀
        frames_before_anchor_ = submitted_frames_ + pending_prefix_frames;
    }

    convertAndSubmit(buf);
}

void AudioPlayNode::onDrain() {
    // SinkNode 已先 ack 输入 EOS
    // 此处只负责把输出侧完整播完，成功返回后 SinkNode 才向 Pipeline 报告自然 EOS
    if (stop_requested_.load()) {
        return;
    }
    // 先排空 swr 的滤波尾部，再通知 SDL 将其内部仍待转换的数据推进到设备读路径
    // 两次动作缺一不可，否则最后一段输入或 SDL 内部转换缓存都可能被提前销毁
    if (!drainSwr() || stop_requested_.load()) {
        return;
    }

    auto* stream = static_cast<SDL_AudioStream*>(audio_stream_);
    if (!stream || !SDL_FlushAudioStream(stream)) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode::onDrain: SDL_FlushAudioStream failed: ") + SDL_GetError());
        return;
    }

    while (!stop_requested_.load()) {
        int64_t queued_frames = 0;
        if (!updateClockFromQueued(stream, &queued_frames)) {
            return;
        }
        if (queued_frames == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(kWaitSliceMs));
    }

    // queued 归零只说明 SDL 输入队列已被设备读路径取走
    // 再等待三个物理设备周期，为不可观测的后端和硬件尾部保留经验上界
    waitCancellable(kDrainWaitPeriods * device_period_us_);
}

void AudioPlayNode::waitCancellable(int64_t us) {
    const int64_t slice_us = static_cast<int64_t>(kWaitSliceMs) * 1000;
    while (us > 0 && !stop_requested_.load()) {
        const int64_t current_slice = std::min(us, slice_us);
        std::this_thread::sleep_for(std::chrono::microseconds(current_slice));
        us -= current_slice;
    }
}

void AudioPlayNode::onStop() {
    if (audio_stream_) {
        SDL_DestroyAudioStream(static_cast<SDL_AudioStream*>(audio_stream_));
        audio_stream_ = nullptr;
    }
    av_free(swr_buffer_);
    swr_buffer_ = nullptr;
    swr_buffer_size_ = 0;
    swr_free(&swr_ctx_);
    SDL_QuitSubSystem(SDL_INIT_AUDIO);
}

} // namespace pipeline
