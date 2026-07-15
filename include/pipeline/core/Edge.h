#pragma once

#include "pipeline/core/Caps.h"
#include "pipeline/core/OutputRoute.h"

#include <memory>
#include <string>

namespace pipeline {

class BaseNode;

// ===================================================================
// Edge: 图中的一条静态订阅连接
//
// OutputRoute 由逻辑输出拥有；Edge 只持有该 Route 的一个 Subscription。
// 同一逻辑输出的多条 Edge 共享 Route，但各自维护独立读取游标。
// ===================================================================
struct Edge {
    // 图结构端点：用于拓扑排序、孤立节点检查和逆拓扑资源释放。
    BaseNode* src_node;
    std::string src_pad_name;
    BaseNode* dst_node;
    std::string dst_pad_name;

    // 数据面订阅：对应 src_pad_name 所属 OutputRoute 的独立读取游标。
    // acquire/ack 只影响本 Edge 的消费进度；同 Route 的其他 Edge 不受影响。
    RouteSubscription subscription;

    // 创建图边；Subscription 由 Graph::link() 在源 Route 上创建后移入 Edge。
    static std::unique_ptr<Edge> create(
        BaseNode* src, const std::string& src_pad,
        BaseNode* dst, const std::string& dst_pad);
};

// 根据实际 MediaType 选择 Route 的条目容量。
size_t selectRouteCapacity(MediaType type);

} // namespace pipeline
