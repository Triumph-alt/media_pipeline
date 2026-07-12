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
//   - Build 阶段：静态 Caps 检查、拓扑排序、环路检测、孤立节点检测
//   - Ready 阶段：按拓扑顺序三步穿插（onReady → createQueues → onStreamInfo）
//
// Pad 创建机制：
//   - 节点构造时声明固定 Pad（如 TransformNode 的 "in"）
//   - link() 时优先查找已有 Pad，找不到时调用 requestSrcPad/requestSinkPad
//   - 节点自己决定是否允许动态创建（分叉、DemuxNode 多路输出、MuxNode 多路输入）
//   - link() 具有事务语义：后续步骤失败时释放本次新建的 Pad，不留下残留状态
// ===================================================================
class Graph {
public:
    // 添加节点
    void addNode(std::unique_ptr<BaseNode> node);

    // 声明连接（Build 阶段）
    // 优先查找已有 Pad，找不到时调用节点的 requestSrcPad/requestSinkPad
    // 若后续失败（目标 Pad 已被占用、节点拒绝创建、TemplateCaps 不兼容），释放本次 link 新创建的 Pad，已有 Pad 保持不变
    bool link(BaseNode* src, const std::string& src_pad_name,
              BaseNode* dst, const std::string& dst_pad_name,
              MediaType hint_type = MediaType::CONTAINER);

    // Build 阶段：完整校验
    // 拓扑排序（Kahn 算法），结果存入 topo_order_
    // 环路检测：topo_order_.size() != nodes_.size() 说明有环
    // 孤立节点检测：单独遍历，找出入度出度均为 0 的节点
    bool build();

    // Ready 阶段：按拓扑顺序三步穿插
    // node->onReady()
    // createQueuesForNode(node)
    // node->onStreamInfo()
    bool ready();

    // 访问拓扑排序结果
    const std::vector<BaseNode*>& topoOrder() const { return topo_order_; }

    // flush 所有 Edge Queue（Pipeline::stop 使用）
    void flushAllQueues();

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
