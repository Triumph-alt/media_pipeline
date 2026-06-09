#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace pipeline {

// ===================================================================
// Clock：A/V 同步时钟
//
// 有音频时：AudioPlayNode 每帧调 advance() 推进时钟，
//          VideoRenderNode 调 getPositionUs() 获取音频播放位置。
// 无音频时：getPositionUs() 返回墙钟（steady_clock）差值。
// ===================================================================

class Clock {
public:
    Clock();

    // AudioPlayNode 调用：推进时钟（按写入的音频时长）
    void advance(int64_t durationUs);

    // VideoRenderNode 调用：获取当前时间位置（微秒）
    int64_t getPositionUs() const;

    // 是否有音频主时钟
    void setAudioMaster(bool has);
    bool hasAudioMaster() const;

    // 重置（Pipeline stop 后重新 play 时调用）
    void reset();

private:
    bool m_hasAudioMaster = false;
    std::atomic<int64_t> m_audioPositionUs{0};
    std::chrono::steady_clock::time_point m_startWallTime;
};

} // namespace pipeline
