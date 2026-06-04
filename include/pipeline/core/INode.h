#pragma once

#include "BufferPool.h"
#include "Queue.h"

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace pipeline {

class INode {
    public:
        virtual ~INode() = default;
        virtual const char* name() const = 0;

        virtual void start();
        virtual void stop();

        // ---- 输入队列（数组，支持多输入扩展） ----
        void set_input_queue(std::shared_ptr<Queue<Buffer*>> q, size_t index = 0) {
            if (index >= input_queues_.size()) input_queues_.resize(index + 1);
            input_queues_[index] = std::move(q);
        }
        std::shared_ptr<Queue<Buffer*>> input_queue(size_t index = 0) const {
            if (index >= input_queues_.size()) return nullptr;
            return input_queues_[index];
        }

        // ---- 输出队列（数组，支持多输出扩展） ----
        void set_output_queue(std::shared_ptr<Queue<Buffer*>> q, size_t index = 0) {
            if (index >= output_queues_.size()) output_queues_.resize(index + 1);
            output_queues_[index] = std::move(q);
        }
        std::shared_ptr<Queue<Buffer*>> output_queue(size_t index = 0) const {
            if (index >= output_queues_.size()) return nullptr;
            return output_queues_[index];
        }

        void set_pool(BufferPool* pool) { pool_ = pool; }

    protected:
        // 子类实现 process() 的约定：
        // - 如果要把 buf 传给 output_queues_，必须先调用 pool_->add_ref(buf)
        // - 不需要自己调用 release()，run() 会在 process() 返回后统一 release
        //
        // index 约定：
        // - input_queues_[0]：唯一输入（普通节点）
        // - output_queues_[0]：主输出，视频流或唯一输出
        // - output_queues_[1]：次输出，音频流（FFmpegDemux 专用）
        // Source 节点重写 run() 时只往 output_queues_[0] push
        // FFmpegDemux 重写 run() 时视频往 index 0，音频往 index 1
        virtual void process(Buffer* buf) = 0;

        // 标准线程循环，子类一般不需要覆盖
        // Source 节点没有输入队列，需要自己重写 run()
        virtual void run();

        std::vector<std::shared_ptr<Queue<Buffer*>>> input_queues_;
        std::vector<std::shared_ptr<Queue<Buffer*>>> output_queues_;
        std::thread                     thread_;
        BufferPool*                     pool_ = nullptr;
        std::atomic<bool>               stop_requested_{false};
};

} // namespace pipeline
