#include "pipeline/core/BoundedQueue.h"

namespace pipeline {

// ===================================================================
// 阻塞 push：队列满时等待，直到有空间或被 flush
// ===================================================================
void BoundedQueue::pushBlocking(QueueItem item) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [this] {
        return flushing_ || queue_.size() < capacity_;
    });

    // flush 状态下不入队
    if (flushing_) {
        return;
    }   

    queue_.push(std::move(item));
    lock.unlock();

    notifyAfterPush();
}

// ===================================================================
// 阻塞 pop：队列空时等待，直到有数据或被 flush
// ===================================================================
std::optional<QueueItem> BoundedQueue::popBlocking() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] {
        return flushing_ || !queue_.empty();
    });

    if (queue_.empty()) {
        // flush 且队列空 → 返回 nullopt，通知调用方退出
        return std::nullopt;
    }

    auto item = std::move(queue_.front());
    queue_.pop();
    lock.unlock();

    not_full_.notify_one();
    return item;
}

// ===================================================================
// 非阻塞 push：队列满时返回 false
// ===================================================================
bool BoundedQueue::tryPush(QueueItem item) {
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (flushing_ || queue_.size() >= capacity_) {
            return false;
        }

        queue_.push(std::move(item));
    }

    // 锁释放后触发外部通知，避免回调中再次加锁导致死锁
    notifyAfterPush();
    return true;
}

// ===================================================================
// 非阻塞 pop：队列空时返回 nullopt
// ===================================================================
std::optional<QueueItem> BoundedQueue::tryPop() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (queue_.empty()) {
        return std::nullopt;
    }

    auto item = std::move(queue_.front());
    queue_.pop();
    not_full_.notify_one();
    return item;
}

// ===================================================================
// peek：查看队首但不取出
// ===================================================================
std::optional<QueueItem> BoundedQueue::peek() const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (queue_.empty()) {
        return std::nullopt;
    }

    // 注意：这里返回的是队首元素的拷贝（variant 值类型）
    // 对于 QueueItem = variant<BufferRef, Event>，拷贝是安全的
    return queue_.front();
}

// ===================================================================
// flush：唤醒所有阻塞线程，标记队列关闭
// ===================================================================
void BoundedQueue::flush() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        flushing_ = true;
    }
    not_empty_.notify_all();
    not_full_.notify_all();
}

// ===================================================================
// resize：调整队列容量
// 只在 onStreamInfo() 阶段调用，此时线程尚未启动，无并发问题
// ===================================================================
void BoundedQueue::resize(size_t new_capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    capacity_ = new_capacity;
    // 唤醒可能因队列满而阻塞的 push（如果有）
    not_full_.notify_all();
}

// ===================================================================
// 状态查询
// ===================================================================
size_t BoundedQueue::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

bool BoundedQueue::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
}

bool BoundedQueue::full() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size() >= capacity_;
}

// ===================================================================
// 外部通知回调
// ===================================================================
void BoundedQueue::setExternalNotify(NotifyCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    external_notify_ = std::move(cb);
}

// ===================================================================
// notifyAfterPush：释放锁后调用，避免回调中再次加锁导致死锁
// ===================================================================
void BoundedQueue::notifyAfterPush() {
    not_empty_.notify_one();
    if (external_notify_) external_notify_();
}

} // namespace pipeline
