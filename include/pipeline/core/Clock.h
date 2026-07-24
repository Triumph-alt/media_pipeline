#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>

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
    // 限时等待启动栅栏时，某一次调用的结果
    enum class StartupBarrierWaitResult {
        RELEASED,  // 所有参与者已到齐，门打开
        TIMEOUT,   // 给定等待时间内其他参与者还没到，门仍关着
        CANCELLED, // stop、ERROR 或窗口关闭已经取消本轮起播
    };

    // 栅栏的持久状态。CANCELLED 可以覆盖 RELEASED，避免 stop 与首个外部输出竞争时
    // 让尚未真正提交/Present 的 worker 继续起跑。
    enum class StartupBarrierState {
        REGISTERING, // Ready 阶段允许呈现型 Sink 登记参与者
        WAITING,     // 人数已封闭，等待全部已登记参与者到达
        RELEASED,    // 全部有效参与者已到达，允许首次外部呈现
        CANCELLED,   // stop / ERROR / STOP_REQUESTED，禁止首次外部呈现
    };

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

    // 一次性登记本轮同步起跑参与者。
    bool registerStartupParticipant() {
        std::lock_guard<std::mutex> lock(startup_mutex_);
        // 只有登记阶段允许增加参与者；Pipeline 封闭人数或已经取消后，登记都属于时序错误。
        if (startup_state_ != StartupBarrierState::REGISTERING) {
            return false;
        }
        ++registered_startup_participants_;
        return true;
    }

    // 封闭本轮参与者人数，随后等待参与者到达。
    bool sealStartupParticipants() {
        std::lock_guard<std::mutex> lock(startup_mutex_);
        // 参与者只能由 REGISTERING 进入 WAITING；重复封闭或取消后的封闭都失败。
        if (startup_state_ != StartupBarrierState::REGISTERING) {
            return false;
        }

        // 没有需要同步起跑的参与者时，封闭后直接进入 RELEASED；否则等待它们各自 arrive。
        startup_state_ = registered_startup_participants_ == 0
            ? StartupBarrierState::RELEASED
            : StartupBarrierState::WAITING;

        // 若封闭时已直接释放，唤醒理论上可能存在的等待者，使状态转换对外完整可见。
        if (startup_state_ == StartupBarrierState::RELEASED) {
            startup_cv_.notify_all();
        }
        return true;
    }

    // 到达栅栏并无限等待正常释放或取消。
    bool arriveAndWaitForStartup() {
        std::unique_lock<std::mutex> lock(startup_mutex_);

        // REGISTERING 说明人数还未固定；CANCELLED 说明本轮起跑已终止，二者都不能继续输出。
        if (startup_state_ == StartupBarrierState::CANCELLED ||
            startup_state_ == StartupBarrierState::REGISTERING) {
            return false;
        }

        // RELEASED 是一次性终态，后续调用无需重复计数或等待。
        if (startup_state_ == StartupBarrierState::RELEASED) {
            return true;
        }

        // 当前调用者已具备首次外部输出条件，计入到达人数。
        ++arrived_startup_participants_;

        // 最后到达者负责从 WAITING 切到 RELEASED，并唤醒此前已到达的等待者。
        if (arrived_startup_participants_ == registered_startup_participants_) {
            // 先完成状态写入并释放锁，随后被唤醒的线程可立刻重新获取锁并观察 RELEASED。
            startup_state_ = StartupBarrierState::RELEASED;
            lock.unlock();
            startup_cv_.notify_all();
            return true;
        }

        // 非最后到达者原子地释放锁并睡眠；谓词同时过滤虚假唤醒。
        startup_cv_.wait(lock, [this] {
            return startup_state_ != StartupBarrierState::WAITING;
        });
        return startup_state_ == StartupBarrierState::RELEASED;
    }

    // 到达栅栏并最多等待 timeout；调用者可在 TIMEOUT 后处理自身事务再重试。
    StartupBarrierWaitResult arriveAndWaitForStartupFor(std::chrono::microseconds timeout,
                                                        bool& arrived) {
        std::unique_lock<std::mutex> lock(startup_mutex_);
        if (startup_state_ == StartupBarrierState::CANCELLED ||
            startup_state_ == StartupBarrierState::REGISTERING) {
            return StartupBarrierWaitResult::CANCELLED;
        }
        if (startup_state_ == StartupBarrierState::RELEASED) {
            return StartupBarrierWaitResult::RELEASED;
        }

        // arrived 由可重试调用者持久保存：首次调用才计数，超时后的重试只继续等待。
        if (!arrived) {
            ++arrived_startup_participants_;
            arrived = true;
            if (arrived_startup_participants_ == registered_startup_participants_) {
                // 最后到达者完成状态切换后先解锁，再唤醒所有已经到达的等待者。
                startup_state_ = StartupBarrierState::RELEASED;
                lock.unlock();
                startup_cv_.notify_all();
                return StartupBarrierWaitResult::RELEASED;
            }
        }

        // 超时只表示仍在 WAITING；调用者可返回自己的事件循环，之后以同一个 arrived 再次进入。
        if (!startup_cv_.wait_for(lock, timeout, [this] {
                return startup_state_ != StartupBarrierState::WAITING;
            })) {
            return StartupBarrierWaitResult::TIMEOUT;
        }
        return startup_state_ == StartupBarrierState::RELEASED
            ? StartupBarrierWaitResult::RELEASED
            : StartupBarrierWaitResult::CANCELLED;
    }

    // 已登记的参与者若在首次外部呈现前自然 EOS，必须撤销本轮席位，避免另一侧永久等待
    // 到达者不能撤销：它已经在等待并应由 stop/error 的 cancel 统一打断
    void withdrawStartupParticipant() {
        std::lock_guard<std::mutex> lock(startup_mutex_);
        if (startup_state_ != StartupBarrierState::WAITING ||
            registered_startup_participants_ == 0) {
            return;
        }
        --registered_startup_participants_;
        if (arrived_startup_participants_ == registered_startup_participants_) {
            startup_state_ = StartupBarrierState::RELEASED;
            startup_cv_.notify_all();
        }
    }

    // stop/error/STOP_REQUESTED 的一次性取消入口。它不修改时间锚定，只解除所有启动等待
    // CANCELLED 覆盖此前 RELEASED，阻止尚未真正提交/Present 的 worker 在 stop 竞争中起跑
    void cancelStartupBarrier() {
        std::lock_guard<std::mutex> lock(startup_mutex_);
        if (startup_state_ == StartupBarrierState::CANCELLED) {
            return;
        }
        startup_state_ = StartupBarrierState::CANCELLED;
        startup_cv_.notify_all();
    }

    // play 时调用：清除锚定、启动 rendezvous 与基准，等待新一轮 Running 重新锚定。
    // has_audio_ 不在此清除——它在每次 play 的 Ready 阶段由音频节点重新设置。
    void reset() {
        anchored_.store(false, std::memory_order_relaxed);
        base_pts_us_.store(0, std::memory_order_relaxed);
        base_wall_us_.store(0, std::memory_order_relaxed);

        std::lock_guard<std::mutex> lock(startup_mutex_);
        registered_startup_participants_ = 0;
        arrived_startup_participants_ = 0;
        startup_state_ = StartupBarrierState::REGISTERING;
    }

private:
    std::atomic<bool> anchored_{false};    // 存不存在锚点
    std::atomic<int64_t> base_pts_us_{0};  // 某个已知的媒体时间位置（微秒）
    std::atomic<int64_t> base_wall_us_{0}; // 那个媒体位置成立时的墙钟时刻（steady_clock 微秒）
    std::atomic<bool> has_audio_{false};

    // 启动栅栏与媒体时钟值分开加锁，它只管理首次外部呈现的共同起跑，所有字段均由 startup_mutex_ 保护
    std::mutex startup_mutex_;                   // 栅栏状态锁
    std::condition_variable startup_cv_;         // 条件变量，唤醒任何在栅栏中睡眠的 worker
    size_t registered_startup_participants_ = 0; // 本轮启动必须到栅栏报到的参与者总数
    size_t arrived_startup_participants_ = 0;    // 已经实际准备好首次外部输出，并到栅栏报到的数量
    StartupBarrierState startup_state_ = StartupBarrierState::REGISTERING;

    static int64_t nowUs() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
};

} // namespace pipeline
