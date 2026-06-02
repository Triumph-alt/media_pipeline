#pragma once

#include "BufferPool.h"
#include "INode.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace pipeline {

class Pipeline {
public:
    explicit Pipeline(
        size_t queue_depth = 8,                  // 节点间队列容量
        size_t block_size = 1920 * 1080 * 3 / 2, // Buffer 大小（字节）
        size_t block_count = 16                  // Buffer 池块数
    );

    // 添加节点，转移所有权到 Pipeline，返回自身引用以支持链式调用
    Pipeline& add(std::unique_ptr<INode> node);

    // 启动管线
    void play();

    // 停止管线（逆序停止）
    void stop();

    // 访问 BufferPool（外部如 Source 节点 acquire 时需要）
    BufferPool* pool() { return pool_.get(); }

private:
    const size_t queue_depth_;
    const size_t block_size_;
    const size_t block_count_;

    std::unique_ptr<BufferPool> pool_;
    std::vector<std::shared_ptr<Queue<Buffer*>>> queues_;
    std::vector<std::unique_ptr<INode>> nodes_;
    bool playing_ = false;
};

} // namespace pipeline