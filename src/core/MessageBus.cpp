#include "pipeline/core/MessageBus.h"

namespace pipeline {

// ===================================================================
// post: 线程安全投递消息
// ===================================================================
void MessageBus::post(Message msg) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(msg));
    }
    cv_.notify_one();
}

// ===================================================================
// waitMessage: Pipeline 监听线程调用，阻塞等待下一条消息
//
// running 为 false 且队列空时返回 nullopt（正常退出信号）
// running 为 false 但队列非空时继续处理剩余消息
// ===================================================================
std::optional<MessageBus::Message> MessageBus::waitMessage(std::atomic<bool>& running) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&] {
        return !queue_.empty() || !running.load();
    });

    if (queue_.empty()) {
        return std::nullopt;  // running 为 false 且队列空，退出
    }

    auto msg = std::move(queue_.front());
    queue_.pop();
    return msg;
}

// ===================================================================
// setObserver: 用户注册观测回调
// ===================================================================
void MessageBus::setObserver(ObserverCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    observer_ = std::move(cb);
}

// ===================================================================
// notify: 唤醒可能阻塞在 waitMessage 上的线程
// ===================================================================
void MessageBus::notify() {
    cv_.notify_all();
}

} // namespace pipeline
