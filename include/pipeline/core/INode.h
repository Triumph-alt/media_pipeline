#pragma once

#include "BufferPool.h"
#include "Queue.h"

#include <atomic>
#include <memory>
#include <thread>

namespace pipeline {

class INode {
    public:
        virtual ~INode() = default;
        virtual const char* name() const = 0;

        virtual void start();
        virtual void stop();

        void set_input_queue(std::shared_ptr<Queue<Buffer*>> q) {
            input_queue_ = std::move(q);
        }
        void set_output_queue(std::shared_ptr<Queue<Buffer*>> q) {
            output_queue_ = std::move(q);
        }
        void set_pool(BufferPool* pool) { pool_ = pool; }

        std::shared_ptr<Queue<Buffer*>> input_queue()  const {
            return input_queue_;
        }
        std::shared_ptr<Queue<Buffer*>> output_queue() const {
            return output_queue_;
        }

    protected:
        // 子类实现 process() 的约定：
        // - 如果要把 buf 传给 output_queue_，必须先调用 pool_->add_ref(buf)
        // - 不需要自己调用 release()，run() 会在 process() 返回后统一 release
        virtual void process(Buffer* buf) = 0;

        // 标准线程循环，子类一般不需要覆盖
        // Source 节点没有输入队列，需要自己重写 run()
        virtual void run();

        std::shared_ptr<Queue<Buffer*>> input_queue_;
        std::shared_ptr<Queue<Buffer*>> output_queue_;
        std::thread                     thread_;
        BufferPool*                     pool_ = nullptr;
        std::atomic<bool>               stop_requested_{false};
};

} // namespace pipeline