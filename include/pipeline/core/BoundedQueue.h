#pragma once

#include "Types.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

namespace pipeline {

template<typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t maxSize,
                          OverflowPolicy policy = OverflowPolicy::BLOCK)
        : m_maxSize(maxSize), m_policy(policy) {}

    // ===== 阻塞 push =====
    // BLOCK: 队列满时阻塞，直到有空位或超时
    // DROP_OLDEST: 队列满时丢弃最老的
    // DROP_NEWEST: 队列满时丢弃本次传入的
    bool push(T item,
              std::chrono::milliseconds timeout = std::chrono::milliseconds::max()) {
        std::unique_lock lock(m_mutex);

        if (m_flushed) return false;

        if (m_queue.size() >= m_maxSize) {
            switch (m_policy) {
            case OverflowPolicy::DROP_OLDEST:
                m_queue.pop();
                break;
            case OverflowPolicy::DROP_NEWEST:
                return false;
            case OverflowPolicy::BLOCK:
                if (!m_notFull.wait_for(lock, timeout, [this] {
                        return m_flushed || m_queue.size() < m_maxSize;
                    })) {
                    return false;
                }
                if (m_flushed) {
                    return false;
                }
                break;
            }
        }

        m_queue.push(std::move(item));
        m_notEmpty.notify_one();
        return true;
    }

    // ===== 阻塞 pop =====
    // 超时或 flush 后队列为空返回 std::nullopt
    std::optional<T> pop(std::chrono::milliseconds timeout = std::chrono::milliseconds::max()) {
        std::unique_lock lock(m_mutex);

        if (m_notEmpty.wait_for(lock, timeout, [this] {
                return m_flushed || !m_queue.empty();
            })) {
            if (!m_queue.empty()) {
                T item = std::move(m_queue.front());
                m_queue.pop();
                m_notFull.notify_one();
                return item;
            }
        }

        return std::nullopt;
    }

    // ===== 查看队头但不取出 =====
    std::optional<T> peek() {
        std::lock_guard lock(m_mutex);
        if (m_queue.empty()) return std::nullopt;
        return m_queue.front();
    }

    // ===== flush：清空队列，唤醒所有阻塞线程 =====
    void flush() {
        std::lock_guard lock(m_mutex);
        m_flushed = true;
        std::queue<T> empty;
        m_queue.swap(empty);
        m_notEmpty.notify_all();
        m_notFull.notify_all();
    }

    bool isFlushed() const { return m_flushed; }
    bool canPush() const {
        std::lock_guard lock(m_mutex);
        return !m_flushed && m_queue.size() < m_maxSize;
    }
    size_t size() const {
        std::lock_guard lock(m_mutex);
        return m_queue.size();
    }
    bool empty() const {
        std::lock_guard lock(m_mutex);
        return m_queue.empty();
    }

    // 禁止拷贝/移动
    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

private:
    std::queue<T> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_notEmpty;
    std::condition_variable m_notFull;
    size_t m_maxSize;
    OverflowPolicy m_policy;
    bool m_flushed = false;
};

} // namespace pipeline
