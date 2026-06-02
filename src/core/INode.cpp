#include "INode.h"

#include <cassert>

namespace pipeline {

void INode::start() {
    thread_ = std::thread([this] {
        run();
    });
}

void INode::stop() {
    stop_requested_.store(true, std::memory_order_release);
    if (input_queue_) {
        input_queue_->flush();
    }
    if (thread_.joinable()) {
        thread_.join();
    }
}

void INode::run() {
    // 标准 Filter/Sink 节点的线程循环
    // Source 节点没有输入队列，必须重写 run()
    assert(input_queue_ && "input_queue_ is null, did you forget to override run()?");
    assert(pool_ && "pool_ is null, did you forget to call set_pool()?");

    while (true) {
        auto result = input_queue_->pop();
        if (!result.has_value()) break;

        Buffer* buf = result.value();
        process(buf);
        // process() 结束后归还当前节点的引用
        // 若 process() 内部已 add_ref 并传给下游，refcount 不会归零
        pool_->release(buf);
    }
}

} // namespace pipeline
