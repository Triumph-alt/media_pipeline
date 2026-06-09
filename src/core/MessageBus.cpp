#include "pipeline/core/MessageBus.h"

namespace pipeline {

void MessageBus::post(Message msg) {
    std::function<void(const Message&)> cb;

    {
        std::lock_guard lock(m_mutex);
        if (m_callback) {
            cb = m_callback;
        } else {
            m_queue.push(std::move(msg));
        }
    }

    // 回调在锁外调用，避免死锁
    if (cb) {
        cb(msg);
    }
}

void MessageBus::setCallback(std::function<void(const Message&)> cb) {
    std::lock_guard lock(m_mutex);
    m_callback = std::move(cb);

    // 设置回调时，先把队列里积压的消息全部触发
    while (!m_queue.empty()) {
        cb(m_queue.front());
        m_queue.pop();
    }
}

std::optional<Message> MessageBus::poll(std::chrono::milliseconds /*timeout*/) {
    std::lock_guard lock(m_mutex);
    if (m_queue.empty()) {
        return std::nullopt;
    }

    Message msg = std::move(m_queue.front());
    m_queue.pop();
    return msg;
}

std::optional<Message> MessageBus::tryPoll() {
    std::lock_guard lock(m_mutex);
    if (m_queue.empty()) {
        return std::nullopt;
    }
    
    Message msg = std::move(m_queue.front());
    m_queue.pop();
    return msg;
}

} // namespace pipeline
