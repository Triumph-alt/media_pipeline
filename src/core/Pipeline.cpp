#include "Pipeline.h"

#include <cassert>

namespace pipeline {

Pipeline::Pipeline(size_t block_size, size_t block_count)
    : block_size_(block_size)
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

    // 3. 按顺序启动所有节点线程
    // 队列全部由调用方手动连接，play() 不做自动连接
    // 原因：管线存在分叉（Demux 双输出），线性自动连接在分叉后失效
    for (auto& node : nodes_) {
        node->start();
    }

    playing_ = true;
}

void Pipeline::stop() {
    if (!playing_) return;

    // 逆序停止：从 Sink 往 Source 方向依次 flush input_queue 并 join
    // 下游先退出后，上游 push 到已 flush 的队列会静默丢弃，上游线程能正常退出
    for (auto it = nodes_.rbegin(); it != nodes_.rend(); ++it) {
        (*it)->stop();
    }

    pool_.reset();
    playing_ = false;
}

} // namespace pipeline
