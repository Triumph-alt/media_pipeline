#pragma once

#include "pipeline/core/Event.h"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace pipeline {

class OutputRoute;

// publishBlocking 的显式结果。PUBLISHED 之外的结果都表示 item 未进入 Route。
enum class RoutePublishResult {
    PUBLISHED,
    CANCELLED,
    NO_SUBSCRIBERS,
};

// ===================================================================
// RouteDelivery: 一次订阅交付的 RAII 令牌
//
// acquire 时不推进订阅游标；ack() 后游标才推进，Route 才可能回收 Entry。
// 析构不自动 ack：节点提前退出或处理失败时不得把未处理数据标记为完成。
// stop/error 通过 OutputRoute::cancel() 统一放弃未完成 Delivery。
// ===================================================================
class RouteDelivery {
public:
    RouteDelivery() = default;
    ~RouteDelivery();

    RouteDelivery(const RouteDelivery&) = delete;
    RouteDelivery& operator=(const RouteDelivery&) = delete;

    RouteDelivery(RouteDelivery&& other) noexcept;
    RouteDelivery& operator=(RouteDelivery&& other) noexcept;

    // item() 仅在有效且未 ack 的 Delivery 上调用；ack/abandon 会立即释放本地 BufferRef。
    const QueueItem& item() const { return item_.value(); }
    bool ack();
    explicit operator bool() const { return route_ != nullptr && item_.has_value(); }

private:
    friend class OutputRoute;

    RouteDelivery(std::shared_ptr<OutputRoute> route,
                  uint64_t subscriber_id,
                  uint64_t sequence,
                  QueueItem item)
        : route_(std::move(route)),
          subscriber_id_(subscriber_id),
          sequence_(sequence),
          item_(std::move(item)) {}

    void releaseWithoutAck();

    std::shared_ptr<OutputRoute> route_;
    uint64_t subscriber_id_ = 0;
    uint64_t sequence_ = 0;
    // 空 Delivery/ack 后/abandon 后均为 nullopt；有值时才持有本次交付的本地引用。
    std::optional<QueueItem> item_;
};

// ===================================================================
// RouteSubscription: Edge 对一个静态 OutputRoute 的订阅
//
// 同一 Subscription 同时最多持有一个未 ack 的 Delivery。重复 acquire 会失败，
// 保证每个游标严格串行推进。
// ===================================================================
class RouteSubscription {
public:
    RouteSubscription() = default;

    std::optional<RouteDelivery> acquireBlocking();
    std::optional<RouteDelivery> tryAcquire();
    std::optional<QueueItem> peek() const;

    explicit operator bool() const { return route_ != nullptr; }

private:
    friend class OutputRoute;
    friend class Graph;
    friend class SinkPad;

    RouteSubscription(std::shared_ptr<OutputRoute> route, uint64_t subscriber_id)
        : route_(std::move(route)), subscriber_id_(subscriber_id) {}

    std::shared_ptr<OutputRoute> route_;
    uint64_t subscriber_id_ = 0;
};

// ===================================================================
// OutputRoute: 静态、有界、多订阅者的可靠有序日志
//
// - 每个 QueueItem 只保存一次；BufferRef 拷贝只增加引用计数，不复制 payload。
// - 所有静态可靠订阅者 ack 后，Entry 才能从 Route 回收。
// - 达到硬容量时 publishBlocking 阻塞，最慢订阅者形成最终背压。
// - seal() 后订阅关系不可修改；cancel() 唤醒全部 publisher/subscriber。
// ===================================================================
class OutputRoute final : public std::enable_shared_from_this<OutputRoute> {
public:
    // 初始容量只用于 Build→Ready 之间承载 Caps；实际 MediaType 在 Ready 后 resize。
    explicit OutputRoute(size_t capacity) : capacity_(capacity) {}

    // Route 由逻辑输出唯一拥有，不允许复制或赋值。
    OutputRoute(const OutputRoute&) = delete;
    OutputRoute& operator=(const OutputRoute&) = delete;

    // Build/link 期间注册静态订阅者；seal/cancel 后拒绝新订阅。
    RouteSubscription subscribe();
    // 仅用于 link 失败回滚；seal 后不允许移除静态订阅者。
    bool removeSubscription(uint64_t subscriber_id);
    // Build 成功后封闭订阅集合。
    bool seal();

    // 可靠发布一项；Route 满时阻塞至最慢订阅者 ack 释放空间或 Route 被 cancel。
    RoutePublishResult publishBlocking(QueueItem item);

    // 强制停止：丢弃未完成 Entry，唤醒所有 Route 等待者。
    void cancel();
    // Ready 阶段按实际 MediaType 更新条目硬容量。
    void resize(size_t new_capacity);
    size_t capacity() const;

    // 注册轻量通知回调；当前用于唤醒 Mux 的多输入等待。
    void setNotifyCallback(std::function<void()> callback);

    // 只读运行状态，用于测试和诊断。
    size_t retainedItems() const;
    size_t subscriberCount() const;
    bool sealed() const;
    bool cancelled() const;

private:
    friend class RouteSubscription;
    friend class RouteDelivery;

    struct Entry {
        // Route 内严格递增的逻辑序号；item 只保存一份，供全部订阅者共享读取。
        uint64_t sequence = 0;
        QueueItem item;
    };

    struct SubscriberState {
        // 下一次 acquire 的序号；in_flight 表示该项正在当前订阅者处理，尚未 ack。
        uint64_t next_sequence = 0;
        bool in_flight = false;
    };

    // Subscription/Delivery 的内部入口：acquire 不推进游标，ack 才推进并可能回收前缀。
    std::optional<RouteDelivery> acquire(uint64_t subscriber_id, bool blocking);
    std::optional<QueueItem> peek(uint64_t subscriber_id) const;
    bool ack(uint64_t subscriber_id, uint64_t sequence);
    void abandon(uint64_t subscriber_id, uint64_t sequence);

    // mutex_ 已持有：删除所有可靠订阅者均已 ack 的连续 Entry。
    void reclaimAcknowledgedPrefixLocked();
    size_t indexForSequenceLocked(uint64_t sequence) const;
    void notifyRouteActivity();

    // data_available_ 唤醒 acquire；space_available_ 唤醒被容量阻塞的 publish。
    mutable std::mutex mutex_;
    std::condition_variable data_available_;
    std::condition_variable space_available_;

    // 保留日志及全部静态订阅者的独立读取状态。
    std::deque<Entry> entries_;
    std::unordered_map<uint64_t, SubscriberState> subscribers_;

    // [head_sequence_, tail_sequence_) 是当前 Route 已发布的逻辑序号范围。
    uint64_t head_sequence_ = 0;
    uint64_t tail_sequence_ = 0;
    uint64_t next_subscriber_id_ = 1;
    size_t capacity_ = 0;
    bool sealed_ = false;
    bool cancelled_ = false;
    std::vector<std::function<void()>> notify_callbacks_;
};

} // namespace pipeline
