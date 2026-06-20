#pragma once

#include "pipeline/core/Event.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>

namespace pipeline {

// ===================================================================
// 默认队列容量（按 MediaType 选择）
// ===================================================================
constexpr size_t DEFAULT_QUEUE_CAPACITY_VIDEO_RAW   = 4;    // ~12MB for 1080p YUV420
constexpr size_t DEFAULT_QUEUE_CAPACITY_AUDIO_RAW   = 50;   // PCM 帧小，多缓减少卡顿
constexpr size_t DEFAULT_QUEUE_CAPACITY_ENCODED     = 128;  // 设大以降低 DemuxNode tryPush 丢包概率
constexpr size_t DEFAULT_QUEUE_CAPACITY_CONTAINER   = 32;

// ===================================================================
// BoundedQueue: 有界线程安全队列，是 Edge 持有的实例，每条连接边独立一个队列
// 同时支持阻塞和非阻塞两种 API：
// 阻塞用于单路连接，背压自然传导——队列满时上游 push 阻塞
// 非阻塞用于多路分叉，队列满时返回 false，由调用方丢弃
//
// 外部通知（setExternalNotify）：
//   push 成功后触发注册的回调。
//   MuxNode 用此机制将自身的 mux_cv_ 与多个 Queue 联动：
//   任意一路 SinkPad 的 Queue 收到数据，mux_cv_ 就被唤醒
//   回调在 push 线程中执行，必须轻量（只做 notify，不做阻塞操作）
// ===================================================================
class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {}

    ~BoundedQueue() = default;

    // 禁止拷贝/移动（队列被 Edge 持有，无需移动）
    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    // 阻塞 push：队列满时等待，直到有空间或被 flush
    void pushBlocking(QueueItem item);

    // 阻塞 pop：队列空时等待，直到有数据或被 flush
    // 返回 nullopt 表示被 flush 唤醒（队列已关闭）
    std::optional<QueueItem> popBlocking();

    // 非阻塞 push：队列满时返回 false，不阻塞
    bool tryPush(QueueItem item);

    // 非阻塞 pop：队列空时返回 nullopt
    std::optional<QueueItem> tryPop();

    // 查看队首但不取出（MuxNode 选最小 DTS 时使用）
    std::optional<QueueItem> peek() const;

    // flush：唤醒所有阻塞的 push/pop，用于 stop 流程
    void flush();

    // 调整队列容量，只在 onStreamInfo() 阶段调用
    // 此时队列里只有 CapsEvent，不会有数据溢出问题
    void resize(size_t new_capacity);

    // ===== 状态查询 =====
    size_t size() const;
    bool empty() const;
    bool full() const;

    // ===== 外部通知回调 =====
    using NotifyCallback = std::function<void()>;

    // 注册外部 notify 回调（每个 push 成功后触发）
    void setExternalNotify(NotifyCallback cb);

private:
    // push 成功后的通知逻辑（释放锁之后调用，避免死锁）
    void notifyAfterPush();

    std::queue<QueueItem>    queue_;
    mutable std::mutex       mutex_;
    std::condition_variable  not_empty_;
    std::condition_variable  not_full_;
    size_t                   capacity_;
    bool                     flushing_        = false;
    NotifyCallback           external_notify_ = nullptr;
};

} // namespace pipeline
