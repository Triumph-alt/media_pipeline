#pragma once

#include "pipeline/core/BoundedQueue.h"
#include "pipeline/core/Caps.h"

#include <memory>
#include <string>

namespace pipeline {

// 前向声明
class BaseNode;

// ===================================================================
// Edge: 图的一等公民，表示两个 Pad 之间的连接
//
// 每条 Edge 持有一个独立的 BoundedQueue，连接边即缓冲区。
// Queue 在 Graph::ready() 阶段创建（非 link 时），容量根据
// SrcPad 的 TemplateCaps 中的 MediaType 自动选择。
// ===================================================================
struct Edge {
    // 连接关系
    BaseNode* src_node;
    std::string src_pad_name;
    BaseNode* dst_node;
    std::string dst_pad_name;

    // 每条边独立的队列（Ready 阶段由 Graph 创建）
    std::unique_ptr<BoundedQueue> queue;

    // 工厂：创建 Edge（不含 Queue，Queue 在 createQueuesForNode() 中创建）
    static std::unique_ptr<Edge> create(
        BaseNode* src, const std::string& src_pad,
        BaseNode* dst, const std::string& dst_pad);
};

// 根据实际 MediaType 选择队列容量（在 onStreamInfo() 里 resize 时调用）
size_t selectQueueCapacity(MediaType type);

} // namespace pipeline
