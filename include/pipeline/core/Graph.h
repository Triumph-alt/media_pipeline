#pragma once

#include "pipeline/core/BaseNode.h"
#include "pipeline/core/Edge.h"

#include <memory>
#include <string>
#include <vector>

namespace pipeline {

// ===================================================================
// Graph: 显式有向图，管理所有节点和边
//
// 职责：
//   - 维护邻接表（nodes_ + edges_）
//   - Build 阶段：静态 Caps 检查、环路检测、拓扑排序、孤立节点检测
//   - Ready 阶段：按拓扑顺序三步穿插（onReady → createQueues → onStreamInfo）
//
// DemuxNode 懒连接：
//   link() 时如果 src 是 DemuxNode，只记录 pending_links_，
//   不创建 Pad 和 Edge。DemuxNode::onReady() 里完成懒连接的 Pad/Edge 创建。
// ===================================================================
class Graph {
public:
    // 添加节点
    void addNode(std::unique_ptr<BaseNode> node);

    // 声明连接（Build 阶段）
    // DemuxNode：记录懒连接意图，不创建 Pad 和 Edge
    // 其他节点：立即创建 Pad，做 TemplateCaps 兼容性检查
    void link(BaseNode* src, const std::string& src_pad_name,
              BaseNode* dst, const std::string& dst_pad_name);

    // Build 阶段：完整校验
    // 1. 检查所有连接的 TemplateCaps 兼容性
    // 2. 环路检测（DFS）
    // 3. 拓扑排序（Kahn 算法），结果存入 topo_order_
    // 4. 孤立节点检测
    bool build();

    // Ready 阶段：按拓扑顺序三步穿插
    // 步骤1：node->onReady()
    // 步骤2：createQueuesForNode(node)
    // 步骤3：node->onStreamInfo()
    bool ready();

    // 访问
    const std::vector<BaseNode*>& topoOrder() const { return topo_order_; }
    const std::vector<std::unique_ptr<Edge>>& edges() const { return edges_; }

private:
    // 为此节点所有 SrcPad 对应的 Edge 创建 BoundedQueue（容量 8）
    void createQueuesForNode(BaseNode* node);

    // 检查 TemplateCaps 兼容性
    bool checkCapsCompatibility(const TemplateCaps& src, const TemplateCaps& dst);

    std::vector<std::unique_ptr<BaseNode>>  nodes_;
    std::vector<std::unique_ptr<Edge>>      edges_;
    std::vector<BaseNode*>                  topo_order_;
};

} // namespace pipeline
