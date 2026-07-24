#include "pipeline/core/Graph.h"

#include <queue>
#include <unordered_set>

namespace pipeline {

void Graph::addNode(std::unique_ptr<BaseNode> node) {
    nodes_.push_back(std::move(node));
}

bool Graph::link(BaseNode* src, const std::string& src_pad_name,
                 BaseNode* dst, const std::string& dst_pad_name,
                 MediaType hint_type)
{
    // 固定 Pad 优先；名称不存在时才让节点按 hint_type 决定是否创建动态 Pad
    SrcPad* src_pad = src->getSrcPad(src_pad_name);
    bool src_pad_created = false;
    if (src_pad) {
        if (src_pad->isConnected()) {
            return false;
        }
    } else {
        src_pad = src->requestSrcPad(src_pad_name, hint_type);
        if (!src_pad) {
            return false;
        }
        src_pad_created = true;
    }

    SinkPad* sink_pad = dst->getSinkPad(dst_pad_name);
    bool sink_pad_created = false;
    if (sink_pad) {
        if (sink_pad->isConnected()) {
            if (src_pad_created) {
                src->releaseSrcPad(src_pad);
            }
            return false;
        }
    } else {
        // 目标 SinkPad 的回滚顺序必须先于本次新建 SrcPad，保持 link 的事务性
        sink_pad = dst->requestSinkPad(dst_pad_name, hint_type);
        if (!sink_pad) {
            if (src_pad_created) {
                src->releaseSrcPad(src_pad);
            }
            return false;
        }
        sink_pad_created = true;
    }

    // 静态能力失败时，只有本次 request 创建的 Pad 才属于当前 link 的回滚范围
    if (!checkCapsCompatibility(src_pad->templateCaps(), sink_pad->templateCaps())) {
        if (sink_pad_created) {
            dst->releaseSinkPad(sink_pad);
        }
        if (src_pad_created) {
            src->releaseSrcPad(src_pad);
        }
        return false;
    }

    auto route = src_pad->route();
    if (!route) {
        if (sink_pad_created) {
            dst->releaseSinkPad(sink_pad);
        }
        if (src_pad_created) {
            src->releaseSrcPad(src_pad);
        }
        return false;
    }

    // 每条 Edge 都是 Route 的独立静态订阅者；成功前不连接两端 Pad，保证失败可回滚
    auto subscription = route->subscribe();
    if (!subscription) {
        if (sink_pad_created) {
            dst->releaseSinkPad(sink_pad);
        }
        if (src_pad_created) {
            src->releaseSrcPad(src_pad);
        }
        return false;
    }

    // Edge 同时保存 DAG 端点和数据面 Subscription；连接完成后 Pad 才正式占用
    auto edge = Edge::create(src, src_pad_name, dst, dst_pad_name);
    edge->subscription = std::move(subscription);
    src_pad->edge_ = edge.get();
    sink_pad->edge_ = edge.get();
    edges_.push_back(std::move(edge));
    return true;
}

bool Graph::build() {
    // Kahn 算法：in_degree 只依赖 Edge 的图端点，不依赖 Route 的数据面状态
    std::unordered_map<BaseNode*, int> in_degree;
    for (auto& node : nodes_) {
        in_degree[node.get()] = 0;
    }
    for (auto& edge : edges_) {
        ++in_degree[edge->dst_node];
    }

    std::queue<BaseNode*> ready;
    for (auto& [node, degree] : in_degree) {
        if (degree == 0) {
            ready.push(node);
        }
    }

    topo_order_.clear();
    while (!ready.empty()) {
        BaseNode* node = ready.front();
        ready.pop();
        topo_order_.push_back(node);

        for (auto& edge : edges_) {
            if (edge->src_node == node && --in_degree[edge->dst_node] == 0) {
                ready.push(edge->dst_node);
            }
        }
    }

    // 未被 Kahn 消耗完说明至少存在环，不能进入 Ready/Running
    if (topo_order_.size() != nodes_.size()) {
        return false;
    }

    // 孤立节点不会被环检测捕获，必须单独拒绝
    std::unordered_set<BaseNode*> connected;
    for (auto& edge : edges_) {
        connected.insert(edge->src_node);
        connected.insert(edge->dst_node);
    }
    for (auto& node : nodes_) {
        if (!connected.count(node.get())) {
            return false;
        }
    }

    return sealAllRoutes();
}

bool Graph::sealAllRoutes() {
    // 同源分叉 Pad 共享 Route；按原始指针去重，确保每条逻辑流仅 seal 一次
    std::unordered_set<OutputRoute*> sealed;
    for (auto& node : nodes_) {
        for (auto& pad : node->src_pads_) {
            auto route = pad->route();
            if (!route || !pad->isConnected() || sealed.count(route.get())) {
                continue;
            }
            if (!route->seal()) {
                return false;
            }
            sealed.insert(route.get());
        }
    }
    return true;
}

void Graph::cancelAllRoutes() {
    // cancel 是全局停止/Ready 回滚路径；同一 Route 只需取消一次即可唤醒全部订阅者
    std::unordered_set<OutputRoute*> cancelled;
    for (auto& node : nodes_) {
        for (auto& pad : node->src_pads_) {
            auto route = pad->route();
            if (route && cancelled.insert(route.get()).second) {
                route->cancel();
            }
        }
    }
}

bool Graph::ready() {
    // Ready 只建立不依赖上游格式的后端资源。CapsEvent 在 Running 中与 Buffer/EOS
    // 共用同一条 Route，因此格式配置和数据边界只有一套有序语义。
    auto rollbackReady = [this](size_t failed_index) {
        cancelAllRoutes();
        for (size_t n = failed_index + 1; n > 0; --n) {
            topo_order_[n - 1]->onStop();
        }
    };

    for (size_t i = 0; i < topo_order_.size(); ++i) {
        if (!topo_order_[i]->onReady()) {
            rollbackReady(i);
            return false;
        }
    }
    return true;
}

bool Graph::checkCapsCompatibility(const TemplateCaps& src, const TemplateCaps& dst) {
    return src.isCompatibleWith(dst);
}

} // namespace pipeline
