#include "Pipeline.h"

#include <cassert>
#include <stdexcept>

namespace pipeline {

Pipeline::Pipeline(size_t queue_depth, size_t block_size, size_t block_count)
    : queue_depth_(queue_depth)
    , block_size_(block_size)
    , block_count_(block_count)
{

}

Pipeline& Pipeline::add(std::unique_ptr<INode> node) {
    assert(node && "node is null");
    assert(!playing_ && "cannot add node while playing");
    nodes_.push_back(std::move(node));
    return *this;
}

void Pipeline::play() {
    assert(!nodes_.empty() && "no nodes added");
    assert(!playing_ && "already playing");

    // 1. 创建全局 BufferPool
    pool_ = std::make_unique<BufferPool>(block_size_, block_count_);

    // 2. 给每个节点注入 pool
    for (auto& node : nodes_) {
        node->set_pool(pool_.get());
    }

    // 3. 在相邻节点之间创建 Queue
    //    nodes_[0] → queue[0] → nodes_[1] → queue[1] → nodes_[2] → ...
    //    N 个节点需要 N-1 个队列
    for (size_t i = 0; i + 1 < nodes_.size(); ++i) {
        auto q = std::make_shared<Queue<Buffer*>>(queue_depth_);
        queues_.push_back(q);
        nodes_[i]->set_output_queue(q);         // 上游的输出
        nodes_[i + 1]->set_input_queue(q);      // 下游的输入
    }

    // 4. 按顺序启动所有节点线程
    for (auto& node : nodes_) {
        node->start();
    }

    playing_ = true;
}

void Pipeline::stop() {
    if (!playing_) {
        return;
    }

    // 逆序停止：从 Sink 往 Source 方向依次 flush input_queue 并 join
    // 下游先退出后，上游 push 到已 flush 的队列会静默丢弃，上游线程能正常退出
    for (auto it = nodes_.rbegin(); it != nodes_.rend(); ++it) {
        (*it)->stop();
    }

    queues_.clear();
    pool_.reset();
    playing_ = false;
}

} // namespace pipeline