#include "pipeline/core/Edge.h"

namespace pipeline {

// ===================================================================
// Edge::create: 工厂方法，创建 Edge（不含 Queue）
//
// Queue 在 createQueuesForNode() 中用固定容量 8 创建，
// 上游节点在 onStreamInfo() 里根据实际 MediaType resize 到正确容量。
// ===================================================================
std::unique_ptr<Edge> Edge::create(
    BaseNode* src, const std::string& src_pad,
    BaseNode* dst, const std::string& dst_pad)
{
    auto edge = std::make_unique<Edge>();
    edge->src_node     = src;
    edge->src_pad_name = src_pad;
    edge->dst_node     = dst;
    edge->dst_pad_name = dst_pad;

    // queue 在 createQueuesForNode() 中创建
    return edge;
}

// ===================================================================
// selectQueueCapacity: 根据实际 MediaType 选择队列容量
// 在 onStreamInfo() 里 resize 时调用
// ===================================================================
size_t selectQueueCapacity(MediaType type) {
    switch (type) {
        case MediaType::VIDEO_RAW:     return DEFAULT_QUEUE_CAPACITY_VIDEO_RAW;
        case MediaType::AUDIO_RAW:     return DEFAULT_QUEUE_CAPACITY_AUDIO_RAW;
        case MediaType::VIDEO_ENCODED:
        case MediaType::AUDIO_ENCODED: return DEFAULT_QUEUE_CAPACITY_ENCODED;
        case MediaType::CONTAINER:     return DEFAULT_QUEUE_CAPACITY_CONTAINER;
    }
    return DEFAULT_QUEUE_CAPACITY_ENCODED;
}

} // namespace pipeline
