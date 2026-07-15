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
//   - Ready 阶段：按拓扑顺序 onReady → onStreamInfo，完成 Route 容量确定和 Caps 协商
//
// Pad 创建机制：
//   - 节点构造时声明固定 Pad（如 TransformNode 的 "in"）
//   - link() 时优先查找已有 Pad，找不到时调用 requestSrcPad/requestSinkPad
//   - 节点自己决定是否允许动态创建（分叉、DemuxNode 多路输出、MuxNode 多路输入）
//   - link() 具有事务语义：后续步骤失败时释放本次新建的 Pad，不留下残留状态
// ===================================================================
class Graph {
public:
    // 取得 Node 所有权。仅允许 Pipeline 处于 NULL_STATE 时调用。
    void addNode(std::unique_ptr<BaseNode> node);

    // 建立一条静态图边：按名称查找或 request Pad、校验 TemplateCaps，
    // 在源 OutputRoute 上创建 Subscription，并将其随 Edge 连接到两端 Pad。
    // hint_type 只参与动态 Pad 的能力/Route 选择，不设置实际 actualType。
    bool link(BaseNode* src, const std::string& src_pad_name,
              BaseNode* dst, const std::string& dst_pad_name,
              MediaType hint_type = MediaType::CONTAINER);

    // 执行拓扑排序、环路/孤立节点校验，并 seal 全部 Route；成功后拓扑不可变。
    bool build();

    // 按拓扑顺序执行 onReady，创建队列，onStreamInfo；
    // 失败时 cancel Route 并逆拓扑回滚 onStop。
    bool ready();

    // build 成功后的 DAG 拓扑序：Pipeline 以逆序启动/停止节点线程。
    const std::vector<BaseNode*>& topoOrder() const { return topo_order_; }

    // stop/error：cancel 全部 Route，唤醒 publish/acquire 等待和 Route 通知的外部等待者。
    void cancelAllRoutes();

private:
    // Build 前的静态 TemplateCaps 交集校验。
    bool checkCapsCompatibility(const TemplateCaps& src, const TemplateCaps& dst);

    // 对每条已连接的逻辑 OutputRoute 仅 seal 一次。
    bool sealAllRoutes();

    // DAG 顶点所有权、DAG 边（含 RouteSubscription）以及 build 后的拓扑序。
    std::vector<std::unique_ptr<BaseNode>> nodes_;
    std::vector<std::unique_ptr<Edge>> edges_;
    std::vector<BaseNode*> topo_order_;
};

} // namespace pipeline
