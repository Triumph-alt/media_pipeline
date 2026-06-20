#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace pipeline {

// ===================================================================
// Clock: A/V 同步时钟
//
// 以音频播放进度为主时钟（Master Clock）：
//   AudioPlayNode 每次送数据给 SDL 后调用 setAudioPosition() 更新
//   VideoRenderNode 调用 getPositionUs() 获取当前时间基准
//
// 无音频时回退到系统墙钟。
// ===================================================================
class Clock {
public:
    // AudioPlayNode 调用：更新音频播放位置
    void setAudioPosition(int64_t pts_us) {
        audio_base_pts_us_.store(pts_us, std::memory_order_relaxed);
        audio_base_wall_us_.store(nowUs(), std::memory_order_relaxed);
    }

    // 获取当前主时钟位置（微秒）
    // 有音频：最后一次 setAudioPosition 的 pts + 距离上次调用的系统时间差
    // 无音频：系统墙钟 - 起始时间
    int64_t getPositionUs() const {
        if (!has_audio_.load(std::memory_order_relaxed)) {
            return nowUs() - wall_start_us_;
        }
        int64_t elapsed = nowUs() - audio_base_wall_us_.load(std::memory_order_relaxed);
        return audio_base_pts_us_.load(std::memory_order_relaxed) + elapsed;
    }

    // 设置是否有音频主时钟
    void setHasAudio(bool has) {
        has_audio_.store(has, std::memory_order_relaxed);
    }

    // 重置墙钟起始时间（play 时调用）
    void reset() {
        wall_start_us_ = nowUs();
    }

private:
    std::atomic<bool>    has_audio_{false};
    std::atomic<int64_t> audio_base_pts_us_{0};
    std::atomic<int64_t> audio_base_wall_us_{0};
    int64_t              wall_start_us_{0};

    static int64_t nowUs() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
};

} // namespace pipeline
