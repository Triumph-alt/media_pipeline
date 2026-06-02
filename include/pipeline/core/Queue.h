#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <cstddef>
#include <optional>

namespace pipeline {

template<typename T> class Queue {
    public:
        explicit Queue(size_t capacity)
            :capacity_(capacity)
        {

        }

        // 满了阻塞，直到有空位或 flush
        void push(T item) {
            std::unique_lock<std::mutex> lock(mutex_);
            not_full_.wait(lock, [this] {
                return queue_.size() < capacity_ || flushed_;
            });

            if (flushed_) {
                return;
            }

            queue_.push_back(std::move(item));
            not_empty_.notify_one();
        }

        // 空了阻塞，直到有数据或 flush
        // flush 后队列已空返回 std::nullopt
        std::optional<T> pop() {
            std::unique_lock<std::mutex> lock(mutex_);
            not_empty_.wait(lock, [this] {
                return !queue_.empty() || flushed_;
            });

            if (queue_.empty()) {
                return std::nullopt;
            }

            T item = std::move(queue_.front());
            queue_.pop_front();
            not_full_.notify_one();
            return item;
        }

        bool try_push(T item) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (flushed_ || queue_.size() >= capacity_) {
                return false;
            }

            queue_.push_back(std::move(item));
            not_empty_.notify_one();
            return true;
        }

        std::optional<T> try_pop() {
            std::lock_guard<std::mutex> lock(mutex_);
            if (queue_.empty()) {
                return std::nullopt;
            }

            T item = std::move(queue_.front());
            queue_.pop_front();
            not_full_.notify_one();
            return item;
        }

        void flush() {
            std::lock_guard<std::mutex> lock(mutex_);
            flushed_ = true;
            queue_.clear();
            not_full_.notify_all();
            not_empty_.notify_all();
        }

        bool is_flushed() const {
            std::lock_guard<std::mutex> lock(mutex_);
            return flushed_;
        }

        size_t size() const {
            std::lock_guard<std::mutex> lock(mutex_);
            return queue_.size();
        }

        size_t capacity() const {
            return capacity_;
        }
        
    private:
        const size_t capacity_;
        std::deque<T> queue_;
        mutable std::mutex mutex_;
        std::condition_variable not_full_;
        std::condition_variable not_empty_;
        bool flushed_ = false;
};

} // namespace pipeline