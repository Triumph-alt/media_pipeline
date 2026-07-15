#include "pipeline/core/OutputRoute.h"

#include <algorithm>
#include <utility>

namespace pipeline {

// ===================================================================
// RouteDelivery
// ===================================================================
RouteDelivery::~RouteDelivery() {
    // 未显式 ack 的 Delivery 只撤销 in_flight，不推进订阅游标
    releaseWithoutAck();
}

RouteDelivery::RouteDelivery(RouteDelivery&& other) noexcept
    : route_(std::move(other.route_)),
      subscriber_id_(other.subscriber_id_),
      sequence_(other.sequence_),
      item_(std::move(other.item_)) {
    other.subscriber_id_ = 0;
    other.sequence_ = 0;
}

RouteDelivery& RouteDelivery::operator=(RouteDelivery&& other) noexcept {
    if (this != &other) {
        releaseWithoutAck();
        route_ = std::move(other.route_);
        subscriber_id_ = other.subscriber_id_;
        sequence_ = other.sequence_;
        item_ = std::move(other.item_);
        other.subscriber_id_ = 0;
        other.sequence_ = 0;
    }
    return *this;
}

bool RouteDelivery::ack() {
    if (!route_) {
        return false;
    }

    auto route = std::move(route_);
    // ack 成功与否都终结当前 Delivery，避免析构再次对同一 sequence abandon
    bool acknowledged = route->ack(subscriber_id_, sequence_);
    item_.reset();
    subscriber_id_ = 0;
    sequence_ = 0;
    return acknowledged;
}

void RouteDelivery::releaseWithoutAck() {
    if (!route_) {
        return;
    }

    auto route = std::move(route_);
    route->abandon(subscriber_id_, sequence_);
    item_.reset();
    subscriber_id_ = 0;
    sequence_ = 0;
}

// ===================================================================
// RouteSubscription
// ===================================================================
std::optional<RouteDelivery> RouteSubscription::acquireBlocking() {
    if (!route_) {
        return std::nullopt;
    }
    return route_->acquire(subscriber_id_, true);
}

std::optional<RouteDelivery> RouteSubscription::tryAcquire() {
    if (!route_) {
        return std::nullopt;
    }
    return route_->acquire(subscriber_id_, false);
}

std::optional<QueueItem> RouteSubscription::peek() const {
    if (!route_) {
        return std::nullopt;
    }
    return route_->peek(subscriber_id_);
}

// ===================================================================
// OutputRoute: 静态订阅关系
// ===================================================================
RouteSubscription OutputRoute::subscribe() {
    std::lock_guard lock(mutex_);
    if (sealed_ || cancelled_) {
        return {};
    }

    uint64_t id = next_subscriber_id_++;
    // 新订阅者从当前 tail 开始，静态拓扑下 build 前 Route 尚无媒体数据
    subscribers_.emplace(id, SubscriberState{tail_sequence_, false});
    return RouteSubscription(shared_from_this(), id);
}

bool OutputRoute::removeSubscription(uint64_t subscriber_id) {
    std::lock_guard lock(mutex_);
    if (sealed_) {
        return false;
    }

    auto it = subscribers_.find(subscriber_id);
    if (it == subscribers_.end() || it->second.in_flight) {
        return false;
    }

    subscribers_.erase(it);
    reclaimAcknowledgedPrefixLocked();
    space_available_.notify_all();
    return true;
}

bool OutputRoute::seal() {
    std::lock_guard lock(mutex_);
    if (cancelled_ || subscribers_.empty()) {
        return false;
    }
    // seal 后 Graph 不再允许新增或移除 Edge Subscription
    sealed_ = true;
    return true;
}

// ===================================================================
// OutputRoute: publish / acquire / ack
// ===================================================================
RoutePublishResult OutputRoute::publishBlocking(QueueItem item) {
    std::unique_lock lock(mutex_);

    if (capacity_ == 0 || !sealed_) {
        return RoutePublishResult::CANCELLED;
    }
    if (subscribers_.empty()) {
        return RoutePublishResult::NO_SUBSCRIBERS;
    }

    space_available_.wait(lock, [this] {
        // 只有最慢订阅者 ack 并回收前缀后才会重新出现 publish 槽位
        return cancelled_ || entries_.size() < capacity_;
    });

    if (cancelled_) {
        return RoutePublishResult::CANCELLED;
    }

    entries_.push_back(Entry{tail_sequence_, std::move(item)});
    ++tail_sequence_;

    // Route 只保留一个 Entry，所有订阅者随后各自 acquire 共享的 BufferRef

    lock.unlock();
    data_available_.notify_all();
    notifyRouteActivity();
    return RoutePublishResult::PUBLISHED;
}

std::optional<RouteDelivery> OutputRoute::acquire(uint64_t subscriber_id, bool blocking) {
    std::unique_lock lock(mutex_);

    auto ready = [this, subscriber_id] {
        auto it = subscribers_.find(subscriber_id);
        if (it == subscribers_.end()) {
            return true;
        }
        return cancelled_ ||
               // 每个 Subscription 同时只能处理一个 Delivery，防止同一游标并发推进
               (!it->second.in_flight && it->second.next_sequence < tail_sequence_);
    };

    if (blocking) {
        data_available_.wait(lock, ready);
    } else if (!ready()) {
        return std::nullopt;
    }

    auto it = subscribers_.find(subscriber_id);
    if (cancelled_ || it == subscribers_.end() || it->second.in_flight ||
        it->second.next_sequence >= tail_sequence_) {
        return std::nullopt;
    }

    uint64_t sequence = it->second.next_sequence;
    size_t index = indexForSequenceLocked(sequence);
    if (index >= entries_.size()) {
        return std::nullopt;
    }

    // acquire 只复制 QueueItem 的 BufferRef 句柄，底层 payload 仍由 Route Entry 共享
    it->second.in_flight = true;
    QueueItem item = entries_[index].item;
    return RouteDelivery(shared_from_this(), subscriber_id, sequence, std::move(item));
}

std::optional<QueueItem> OutputRoute::peek(uint64_t subscriber_id) const {
    std::lock_guard lock(mutex_);

    auto it = subscribers_.find(subscriber_id);
    if (cancelled_ || it == subscribers_.end() || it->second.in_flight ||
        it->second.next_sequence >= tail_sequence_) {
        return std::nullopt;
    }

    size_t index = indexForSequenceLocked(it->second.next_sequence);
    if (index >= entries_.size()) {
        return std::nullopt;
    }
    return entries_[index].item;
}

bool OutputRoute::ack(uint64_t subscriber_id, uint64_t sequence) {
    std::unique_lock lock(mutex_);

    auto it = subscribers_.find(subscriber_id);
    if (cancelled_ || it == subscribers_.end() || !it->second.in_flight ||
        it->second.next_sequence != sequence) {
        return false;
    }

    it->second.in_flight = false;
    ++it->second.next_sequence;

    // ack 后以所有可靠订阅者的最小游标回收连续前缀

    size_t previous_size = entries_.size();
    reclaimAcknowledgedPrefixLocked();
    bool released_space = entries_.size() < previous_size;

    lock.unlock();
    data_available_.notify_all();
    if (released_space) {
        space_available_.notify_all();
    }
    return true;
}

void OutputRoute::abandon(uint64_t subscriber_id, uint64_t sequence) {
    std::lock_guard lock(mutex_);

    auto it = subscribers_.find(subscriber_id);
    if (it == subscribers_.end() || !it->second.in_flight ||
        it->second.next_sequence != sequence) {
        return;
    }

    // 未处理完成的数据不推进游标，只允许同一订阅者重新 acquire。
    it->second.in_flight = false;
    data_available_.notify_all();
}

void OutputRoute::reclaimAcknowledgedPrefixLocked() {
    if (subscribers_.empty()) {
        entries_.clear();
        head_sequence_ = tail_sequence_;
        return;
    }

    uint64_t min_sequence = tail_sequence_;
    // 最慢订阅者决定仍需保留的最早 sequence
    for (const auto& [_, state] : subscribers_) {
        min_sequence = std::min(min_sequence, state.next_sequence);
    }

    while (!entries_.empty() && head_sequence_ < min_sequence) {
        entries_.pop_front();
        ++head_sequence_;
    }
}

size_t OutputRoute::indexForSequenceLocked(uint64_t sequence) const {
    if (sequence < head_sequence_) {
        return entries_.size();
    }
    return static_cast<size_t>(sequence - head_sequence_);
}

// ===================================================================
// OutputRoute: lifecycle / query
// ===================================================================
void OutputRoute::cancel() {
    {
        std::lock_guard lock(mutex_);
        if (cancelled_) {
            return;
        }
        cancelled_ = true;
        // cancel 不做 drain，强制丢弃未完成日志并使所有 in-flight Delivery 失效
        entries_.clear();
        head_sequence_ = tail_sequence_;
        for (auto& [_, state] : subscribers_) {
            state.in_flight = false;
        }
    }

    data_available_.notify_all();
    space_available_.notify_all();
    notifyRouteActivity();
}

void OutputRoute::resize(size_t new_capacity) {
    std::lock_guard lock(mutex_);
    capacity_ = new_capacity;
    space_available_.notify_all();
}

size_t OutputRoute::capacity() const {
    std::lock_guard lock(mutex_);
    return capacity_;
}

void OutputRoute::setNotifyCallback(std::function<void()> callback) {
    std::lock_guard lock(mutex_);
    notify_callbacks_.push_back(std::move(callback));
}

void OutputRoute::notifyRouteActivity() {
    std::vector<std::function<void()>> callbacks;
    {
        std::lock_guard lock(mutex_);
        callbacks = notify_callbacks_;
    }
    // 在 Route 锁外调用，避免 Mux 回调反向触碰 Route 时产生死锁
    for (auto& callback : callbacks) {
        if (callback) {
            callback();
        }
    }
}

size_t OutputRoute::retainedItems() const {
    std::lock_guard lock(mutex_);
    return entries_.size();
}

size_t OutputRoute::subscriberCount() const {
    std::lock_guard lock(mutex_);
    return subscribers_.size();
}

bool OutputRoute::sealed() const {
    std::lock_guard lock(mutex_);
    return sealed_;
}

bool OutputRoute::cancelled() const {
    std::lock_guard lock(mutex_);
    return cancelled_;
}

} // namespace pipeline
