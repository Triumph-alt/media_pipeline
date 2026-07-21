#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>

namespace pipeline {

// ===================================================================
// Clock: A/V 同步主时钟
//
// 语义：表示"当前估计已到达可听输出端的媒体时间位置"。
//   - 锚定：首个真实媒体时间戳。有音频时由 AudioPlayNode 用首个有效音频
//     Buffer 的 PTS 锚定（setAudioPosition 连续刷新）；无音频时由呈现方
//     （VideoRenderNode）用首帧 PTS 一次性锚定（anchorOnce）。
//   - 速率：已锚定后按墙钟外推（base_pts + 距 base_wall 的墙钟差）。
//     有音频时音频节点每次提交都用真实消费进度重锚，插值只在两次采样间生效；
//     无音频时墙钟速率即呈现速率，自洽无漂移。
//   - 权威：音频消费进度是主时钟的唯一权威，setAudioPosition 无条件重锚。
//     不靠"丢弃落后样本"换取单调——那样等于把音频主时钟偷换成"墙钟、只准音频
//     往前顶"。正常播放时样本本身单调且与墙钟同速，报出位置平滑；
//     欠载时设备停播、样本如实停滞，时钟如实跟随（不向墙钟虚构前进）。
//   - 偏移：音频路径含一段不可观测、有界恒定的硬件缓冲领先，已知不精确、不校准。
//
// has_audio_ 仅作策略位（VideoRenderNode 据此决定首帧是否 anchorOnce），
// 不影响 getPositionUs 的计算路径——锚定后无论有无音频都是同一公式。
// ===================================================================
class Clock {
public:
    // getPositionUs() 在未锚定时，Clock 还不知道当前对应媒体中的第几秒，所以返回这个特殊值
    static constexpr int64_t kUnanchored = std::numeric_limits<int64_t>::min();

    // AudioPlayNode 每次提交后调用：用真实消费进度（已含首帧 PTS 锚）重锚主时钟
    // 告诉 Clock “在当前这个墙钟时刻，音频估计播放到了媒体的 pts_us”
    // 音频样本权威：无条件重锚（base_pts = pts、base_wall = now），不靠"拒绝落后样本"换取单调；
    // 正常播放时样本本身单调且与墙钟同速，欠载时样本如实停滞，时钟如实跟随
    void setAudioPosition(int64_t pts_us) {
        // 媒体位置：无条件采纳音频实测位置作为新锚点
        base_pts_us_.store(pts_us, std::memory_order_relaxed);
        // 以当前墙钟作为该锚点成立的时刻，也就是这个媒体位置成立时的系统时间
        base_wall_us_.store(nowUs(), std::memory_order_relaxed);
        // 标记已锚定
        anchored_.store(true, std::memory_order_relaxed);
    }

    // 无音频时由呈现方（VideoRenderNode）首帧调用：用第一帧 PTS 建立一次锚点，后续按墙钟播放
    void anchorOnce(int64_t pts_us) {
        if (anchored_.load(std::memory_order_relaxed)) {
            // 已经锚定，不覆盖
            return;
        }
        base_pts_us_.store(pts_us, std::memory_order_relaxed);
        base_wall_us_.store(nowUs(), std::memory_order_relaxed);
        anchored_.store(true, std::memory_order_relaxed);
    }

    // 获取当前主时钟位置（微秒）
    int64_t getPositionUs() const {
        if (!anchored_.load(std::memory_order_relaxed)) {
            // 未锚定：返回 kUnanchored
            return kUnanchored;
        }
        // 已锚定，先计算自锚点以来的墙钟差
        const int64_t elapsed = nowUs() - base_wall_us_.load(std::memory_order_relaxed);
        // 返回 base_pts + 距 base_wall 的墙钟差
        return base_pts_us_.load(std::memory_order_relaxed) + elapsed;
    }

    // 策略位：是否存在音频主时钟来源（Ready 阶段由音频节点设置）
    void setHasAudio(bool has) {
        has_audio_.store(has, std::memory_order_relaxed);
    }
    bool hasAudio() const {
        return has_audio_.load(std::memory_order_relaxed);
    }

    // play 时调用：清除锚定与基准，等待新一轮 Running 重新锚定
    // has_audio_ 不在此清除——它在每次 play 的 Ready 阶段由音频节点重新设置
    void reset() {
        anchored_.store(false, std::memory_order_relaxed);
        base_pts_us_.store(0, std::memory_order_relaxed);
        base_wall_us_.store(0, std::memory_order_relaxed);
    }

private:
    std::atomic<bool> anchored_{false};    // 存不存在锚点
    std::atomic<int64_t> base_pts_us_{0};  // 某个已知的媒体时间位置（微秒）
    std::atomic<int64_t> base_wall_us_{0}; // 那个媒体位置成立时的墙钟时刻（steady_clock 微秒）
    std::atomic<bool> has_audio_{false};

    static int64_t nowUs() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
};

} // namespace pipeline
