#include "pipeline/core/Graph.h"

#include <algorithm>
#include <queue>
#include <unordered_set>

namespace pipeline {

// ===================================================================
// addNode: 添加节点
// ===================================================================
void Graph::addNode(std::unique_ptr<BaseNode> node) {
    nodes_.push_back(std::move(node));
}

// ===================================================================
// link: 声明连接
//
// DemuxNode：记录懒连接意图到 pending_links_，不创建 Pad 和 Edge
// 其他节点：立即创建 Pad，做 TemplateCaps 兼容性检查
// ===================================================================
void Graph::link(BaseNode* src, const std::string& src_pad_name,
                 BaseNode* dst, const std::string& dst_pad_name)
{
    // 懒连接节点（如 DemuxNode）：只记录意图，onReady() 里再创建 Pad 和 Edge
    if (src->needsLazyConnection()) {
        src->addPendingLink(src_pad_name);
        // 同时在 dst 上创建 SinkPad（如果还没有的话）
        if (!dst->getSinkPad(dst_pad_name)) {
            TemplateCaps sink_caps;
            sink_caps.supported_types = {MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED};
            dst->addSinkPad(dst_pad_name, std::move(sink_caps));
        }
        return;
    }

    // 非懒连接节点：立即创建 Pad
    SrcPad* src_pad = src->getSrcPad(src_pad_name);
    if (!src_pad) {
        TemplateCaps src_caps;
        // 默认 TemplateCaps，实际类型由子类在 onReady/onStreamInfo 里确定
        src_caps.supported_types = {MediaType::VIDEO_RAW, MediaType::AUDIO_RAW,
                                    MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED,
                                    MediaType::CONTAINER};
        src_pad = src->addSrcPad(src_pad_name, std::move(src_caps));
    }

    SinkPad* sink_pad = dst->getSinkPad(dst_pad_name);
    if (!sink_pad) {
        TemplateCaps sink_caps;
        sink_caps.supported_types = {MediaType::VIDEO_RAW, MediaType::AUDIO_RAW,
                                     MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED,
                                     MediaType::CONTAINER};
        sink_pad = dst->addSinkPad(dst_pad_name, std::move(sink_caps));
    }

    // 静态 Caps 检查
    if (!checkCapsCompatibility(src_pad->templateCaps(), sink_pad->templateCaps())) {
        // TODO: 通过 MessageBus 报错
        return;
    }

    // 创建 Edge（不含 Queue，Queue 在 ready() 阶段创建）
    auto edge = Edge::create(src, src_pad_name, dst, dst_pad_name);
    src_pad->edge_ = edge.get();
    sink_pad->edge_ = edge.get();
    edges_.push_back(std::move(edge));
}

// ===================================================================
// build: Build 阶段完整校验
// ===================================================================
bool Graph::build() {
    // 1. 拓扑排序（Kahn 算法）
    //    计算每个节点的入度
    std::unordered_map<BaseNode*, int> in_degree;
    for (auto* node : nodes_) {
        in_degree[node] = 0;
    }

    for (auto& edge : edges_) {
        in_degree[edge->dst_node]++;
    }

    // 入度为 0 的节点入队
    std::queue<BaseNode*> q;
    for (auto& [node, deg] : in_degree) {
        if (deg == 0) q.push(node);
    }

    topo_order_.clear();
    while (!q.empty()) {
        BaseNode* node = q.front();
        q.pop();
        topo_order_.push_back(node);

        // 减少后继节点的入度
        for (auto& edge : edges_) {
            if (edge->src_node == node) {
                if (--in_degree[edge->dst_node] == 0) {
                    q.push(edge->dst_node);
                }
            }
        }
    }

    // 2. 环路检测：拓扑排序结果数量 != 节点数量，说明有环
    if (topo_order_.size() != nodes_.size()) {
        // TODO: 通过 MessageBus 报错
        return false;
    }

    // 3. 孤立节点检测：拓扑排序已覆盖所有连通节点
    //    未被排序的节点就是孤立节点（但上面的环路检测已经包含了这种情况）

    return true;
}

// ===================================================================
// ready: Ready 阶段三步穿插
// ===================================================================
bool Graph::ready() {
    for (auto* node : topo_order_) {
        // 步骤1：初始化自身资源
        if (!node->onReady()) {
            return false;
        }

        // 步骤2：为此节点所有 SrcPad 的 Edge 创建 BoundedQueue
        createQueuesForNode(node);

        // 步骤3：发送/处理 CapsEvent
        if (!node->onStreamInfo()) {
            return false;
        }
    }
    return true;
}

// ===================================================================
// createQueuesForNode: 为节点所有 SrcPad 的 Edge 创建 BoundedQueue
// ===================================================================
void Graph::createQueuesForNode(BaseNode* node) {
    for (auto& pad : node->src_pads_) {
        if (pad->edge_ && !pad->edge_->queue) {
            // 固定容量 8，后续在 onStreamInfo() 里 resize 到正确容量
            pad->edge_->queue = std::make_unique<BoundedQueue>(8);
        }
    }
}

// ===================================================================
// checkCapsCompatibility: 检查 TemplateCaps 兼容性
// ===================================================================
bool Graph::checkCapsCompatibility(const TemplateCaps& src, const TemplateCaps& dst) {
    return src.isCompatibleWith(dst);
}

} // namespace pipeline
