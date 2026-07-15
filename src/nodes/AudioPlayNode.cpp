#include "pipeline/nodes/AudioPlayNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Pipeline.h"

extern "C" {
#include <SDL3/SDL.h>
#include <libswresample/swresample.h>
#include <libavutil/opt.h>
#include <libavutil/channel_layout.h>
}

namespace pipeline {

// ===================================================================
// 构造函数
// ===================================================================
AudioPlayNode::AudioPlayNode(const std::string& name)
    : SinkNode(name) {
    addSinkPad("in", TemplateCaps{{MediaType::AUDIO_RAW}});
}

// ===================================================================
// onStreamInfo: 初始化 SDL 音频流，必要时创建 swr
//
// 目标格式：S16（SDL3 广泛支持的格式）。
// 输入格式从接收的 CapsEvent 获取，若非 S16 则通过 swr 转换。
// Clock 初始化：标记有音频，设置 has_audio。
// ===================================================================
bool AudioPlayNode::onStreamInfo() {
    if (!receiveCapsEvent("in")) {
        return false;
    }
    const CapsEvent& caps = negotiated_caps_["in"];

    sample_rate_      = caps.sample_rate;
    channels_         = caps.channels;
    input_sample_fmt_ = caps.sample_fmt;

    // 目标格式：S16
    constexpr int target_bytes_per_sample = 2;  // S16 = 2 bytes
    bytes_per_sample_ = target_bytes_per_sample * channels_;

    // 初始化 SDL 音频子系统
    if (!SDL_InitSubSystem(SDL_INIT_AUDIO)) {
        postMessage(MessageType::ERROR, "AudioPlayNode: SDL_InitSubSystem(SDL_INIT_AUDIO) failed");
        return false;
    }

    // 设置目标 spec（S16、输入的 channels 和 sample_rate）
    SDL_AudioSpec spec;
    SDL_zero(spec);
    spec.format   = SDL_AUDIO_S16;
    spec.channels = channels_;
    spec.freq     = sample_rate_;

    // 打开默认播放设备的音频流
    audio_stream_ = SDL_OpenAudioDeviceStream(
        SDL_AUDIO_DEVICE_DEFAULT_PLAYBACK, &spec, nullptr, nullptr);
    if (!audio_stream_) {
        postMessage(MessageType::ERROR,
                    std::string("AudioPlayNode: SDL_OpenAudioDeviceStream failed: ") + SDL_GetError());
        return false;
    }

    // 开始播放
    SDL_ResumeAudioStreamDevice(static_cast<SDL_AudioStream*>(audio_stream_));

    // 如果输入格式不是 S16，创建 swr 转换上下文
    if (input_sample_fmt_ != AV_SAMPLE_FMT_S16) {
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

    // 标记有音频，用于 Clock 回退判断
    pipeline_->clock()->setHasAudio(true);
    submitted_samples_ = 0;

    return true;
}

// ===================================================================
// consume: 推数据到 SDL 音频流，更新主时钟
//
// 严格按设计文档 §9.3 的 AudioPlayNode::consume 逻辑。
// SDL3 的 AudioStream 会自动处理设备格式差异。
// ===================================================================
void AudioPlayNode::consume(const Buffer* buf) {
    if (!buf || buf->size == 0) return;

    const uint8_t* data = buf->data;
    int            size = static_cast<int>(buf->size);

    uint8_t* converted[1] = {};
    int      converted_size = 0;

    // 格式转换（如需）
    if (swr_ctx_) {
        // 计算输入样本数
        int in_bytes_per_sample = av_get_bytes_per_sample(input_sample_fmt_);
        int nb_samples = static_cast<int>(buf->size) / (in_bytes_per_sample * channels_);

        // 分配输出缓冲区
        converted_size = nb_samples * bytes_per_sample_;
        if (converted_size > swr_buffer_size_) {
            av_free(swr_buffer_);
            swr_buffer_ = static_cast<uint8_t*>(av_malloc(converted_size + AV_INPUT_BUFFER_PADDING_SIZE));
            swr_buffer_size_ = converted_size;
        }
        converted[0] = swr_buffer_;

        // 将输入的 planar/packed 数据按 FFmpeg 布局传给 swr
        const uint8_t* in_data[AV_NUM_DATA_POINTERS] = {};
        if (av_sample_fmt_is_planar(input_sample_fmt_)) {
            // planar: 每个 plane 一个声道
            int plane_size = nb_samples * in_bytes_per_sample;
            for (int i = 0; i < channels_; i++) {
                in_data[i] = buf->data + i * plane_size;
            }
        } else {
            // packed: 交错排列，只有一个 plane
            in_data[0] = buf->data;
        }

        int converted_samples = swr_convert(swr_ctx_, converted, nb_samples,
                                             in_data, nb_samples);
        if (converted_samples < 0) {
            postMessage(MessageType::ERROR, "AudioPlayNode: swr_convert failed");
            return;
        }

        data = swr_buffer_;
        size = converted_samples * bytes_per_sample_;
    }

    // 推数据到 SDL 音频流
    SDL_PutAudioStreamData(static_cast<SDL_AudioStream*>(audio_stream_), data, size);

    // 更新主时钟（设计文档 §9.3）
    submitted_samples_ += size / bytes_per_sample_;
    int buffered_bytes = SDL_GetAudioStreamAvailable(static_cast<SDL_AudioStream*>(audio_stream_));
    int buffered_samples = buffered_bytes / bytes_per_sample_;
    int64_t played_samples = submitted_samples_ - buffered_samples;
    int64_t audio_pts_us = (played_samples * 1000000LL) / sample_rate_;
    pipeline_->clock()->setAudioPosition(audio_pts_us);
}

// ===================================================================
// onStop: 关闭 SDL 音频流和释放 swr
//
// 必须支持部分初始化状态。
// ===================================================================
void AudioPlayNode::onStop() {
    if (audio_stream_) {
        SDL_DestroyAudioStream(static_cast<SDL_AudioStream*>(audio_stream_));
        audio_stream_ = nullptr;
    }
    if (swr_buffer_) {
        av_free(swr_buffer_);
        swr_buffer_ = nullptr;
    }
    if (swr_ctx_) {
        swr_free(&swr_ctx_);
    }
    SDL_QuitSubSystem(SDL_INIT_AUDIO);
}

} // namespace pipeline
