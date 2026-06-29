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
// 优先查找已有 Pad，找不到时调用节点的 requestSrcPad/requestSinkPad。
// 三种失败情况返回 false：
//   1. 目标 Pad 已被占用（isConnected()）
//   2. 节点拒绝创建（requestPad 返回 nullptr）
//   3. TemplateCaps 不兼容
// ===================================================================
bool Graph::link(BaseNode* src, const std::string& src_pad_name,
                 BaseNode* dst, const std::string& dst_pad_name,
                 MediaType hint_type)
{
    // 1. 查找或请求 SrcPad
    SrcPad* src_pad = src->getSrcPad(src_pad_name);
    if (src_pad) {
        if (src_pad->isConnected()) return false;  // 已被占用
    } else {
        src_pad = src->requestSrcPad(src_pad_name, hint_type);
        if (!src_pad) return false;  // 节点拒绝创建
    }

    // 2. 查找或请求 SinkPad
    SinkPad* sink_pad = dst->getSinkPad(dst_pad_name);
    if (sink_pad) {
        if (sink_pad->isConnected()) return false;  // 已被占用
    } else {
        sink_pad = dst->requestSinkPad(dst_pad_name, hint_type);
        if (!sink_pad) return false;  // 节点拒绝创建
    }

    // 3. 静态 Caps 检查
    if (!checkCapsCompatibility(src_pad->templateCaps(), sink_pad->templateCaps())) {
        return false;
    }

    // 4. 创建 Edge（不含 Queue，Queue 在 ready() 阶段创建）
    auto edge = Edge::create(src, src_pad_name, dst, dst_pad_name);
    src_pad->edge_ = edge.get();
    sink_pad->edge_ = edge.get();
    edges_.push_back(std::move(edge));

    return true;
}

// ===================================================================
// build: Build 阶段完整校验
// ===================================================================
bool Graph::build() {
    // 1. 拓扑排序（Kahn 算法）
    std::unordered_map<BaseNode*, int> in_degree;
    for (auto& node : nodes_) in_degree[node.get()] = 0;

    for (auto& edge : edges_) {
        in_degree[edge->dst_node]++;
    }

    std::queue<BaseNode*> q;
    for (auto& [node, deg] : in_degree) {
        if (deg == 0) q.push(node);
    }

    topo_order_.clear();
    while (!q.empty()) {
        BaseNode* node = q.front();
        q.pop();
        topo_order_.push_back(node);

        for (auto& edge : edges_) {
            if (edge->src_node == node) {
                if (--in_degree[edge->dst_node] == 0) {
                    q.push(edge->dst_node);
                }
            }
        }
    }

    // 2. 环路检测：topo 排序结果数量 != 节点数量 → 有环
    if (topo_order_.size() != nodes_.size()) {
        return false;
    }

    // 3. 孤立节点检测：入度出度均为 0 的节点
    //    这种节点会被 Kahn 算法正常排入 topo_order_，不会被环路检测捕获
    for (auto& node : nodes_) {
        bool has_edge = false;
        BaseNode* raw = node.get();
        for (auto& edge : edges_) {
            if (edge->src_node == raw || edge->dst_node == raw) {
                has_edge = true;
                break;
            }
        }
        if (!has_edge) return false;  // 孤立节点
    }

    return true;
}

// ===================================================================
// ready: Ready 阶段三步穿插
// ===================================================================
bool Graph::ready() {
    // 回滚 Lambda
    auto rollbackReady = [this](size_t failed_index) {
        // 按拓扑逆序调用 onStop() 回滚；释放各节点自己持有的具体资源
        for (size_t n = failed_index + 1; n > 0; --n) {
            topo_order_[n - 1]->onStop();
        }

        // 不考虑顺序，全部 reset，Ready 失败后不再保留 CapsEvent 或临时队列
        for (auto& edge : edges_) {
            edge->queue.reset();
        }
    };

    // 主循环：拓扑遍历
    for (size_t i = 0; i < topo_order_.size(); ++i) {
        auto* node = topo_order_[i];

        // 步骤1：初始化自身资源
        if (!node->onReady()) {
            rollbackReady(i);
            return false;
        }

        // 步骤2：为此节点所有 SrcPad 的 Edge 创建 BoundedQueue
        createQueuesForNode(node);

        // 步骤3：发送/处理 CapsEvent
        if (!node->onStreamInfo()) {
            rollbackReady(i);
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
