#include "pipeline/core/Clock.h"

namespace pipeline {

Clock::Clock() {
    m_startWallTime = std::chrono::steady_clock::now();
}

void Clock::advance(int64_t durationUs) {
    m_audioPositionUs.fetch_add(durationUs, std::memory_order_relaxed);
}

int64_t Clock::getPositionUs() const {
    if (m_hasAudioMaster) {
        return m_audioPositionUs.load(std::memory_order_relaxed);
    }
    // 无音频时用墙钟
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
        now - m_startWallTime).count();
}

void Clock::setAudioMaster(bool has) {
    m_hasAudioMaster = has;
}

bool Clock::hasAudioMaster() const {
    return m_hasAudioMaster;
}

void Clock::reset() {
    m_audioPositionUs.store(0, std::memory_order_relaxed);
    m_startWallTime = std::chrono::steady_clock::now();
}

} // namespace pipeline
