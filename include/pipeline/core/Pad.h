#pragma once

#include "pipeline/core/Caps.h"
#include "pipeline/core/Event.h"
#include "pipeline/core/Types.h"

#include <optional>
#include <string>

namespace pipeline {

// 前向声明
class BaseNode;
class Edge;

// ===================================================================
// PadDir: Pad 方向
// ===================================================================
enum class PadDir {
    SRC,
    SINK,
};

// ===================================================================
// Pad 是节点的输入/输出端口，通过 Edge 间接访问 Queue，Pad 自身不持有队列
// 一个 SrcPad 只连一个 SinkPad，分叉通过节点创建多个 SrcPad 实现
//
// template_caps_ 是静态的能力集合，构造 / requestPad 时确立，声明"本 pad 允许承载的 MediaType" 的集合
// Build 阶段 Graph::link 用它做粗粒度交集兼容性检查，Ready 阶段 send/receiveCapsEvent 用它做精确校验
//
// actual_type_ 是运行时 Pad 承载的的类型，由 BaseNode::sendCapsEvent / receiveCapsEvent 内部设置
// 代表本 pad 实际承载的具体类型。Ready 之前查询得 nullopt；Ready之后所有已连接 pad 的 actualType() 必有值
// 
// 两者严格分层：runLoop 阶段的类型分发一律读 actualType()，绝不把能力集合的其中某一个成员单独作为"实际类型"来使用
// ===================================================================
class Pad {
public:
    const std::string& name() const { return name_; }
    PadDir             dir()  const { return dir_; }
    BaseNode*          node() const { return node_; }
    TemplateCaps       templateCaps() const { return template_caps_; }

    std::optional<MediaType> actualType() const { return actual_type_; }

    bool isConnected() const { return edge_ != nullptr; }
    Edge* edge() const { return edge_; }

protected:
    Pad(const std::string& name, PadDir dir, BaseNode* node, TemplateCaps caps)
        : name_(name), dir_(dir), node_(node), template_caps_(std::move(caps)) {}

    std::string    name_;
    PadDir         dir_;
    BaseNode*      node_;
    TemplateCaps   template_caps_;
    Edge*          edge_ = nullptr;   // 连接到的 Edge（一对一）

    friend class Graph;  // Graph::link 里写 edge_

private:
    // actual_type 的唯一设值时机是 BaseNode 的 send/receiveCapsEvent 内部，
    // CapsEvent 流经 pad 且通过 TemplateCaps 校验时，其他任何路径都不得直接设置
    void setActualType(MediaType t) { actual_type_ = t; }

    std::optional<MediaType> actual_type_;

    friend class BaseNode;  // 仅为 send/receiveCapsEvent 授权设置 actual_type_
};

// ===================================================================
// SrcPad: 输出端口
//
// 由 BaseNode::pushToDownstream 调用，不直接暴露给子类。
// 单路连接时用 pushBlocking（背压），多路分叉时用 tryPush（丢弃）。
// ===================================================================
class SrcPad : public Pad {
public:
    SrcPad(const std::string& name, BaseNode* node, TemplateCaps caps)
        : Pad(name, PadDir::SRC, node, std::move(caps)) {}

    // 阻塞 push：队列满时等待
    void pushBlocking(QueueItem item);

    // 非阻塞 push：队列满时返回 false
    bool tryPush(QueueItem item);
};

// ===================================================================
// SinkPad: 输入端口
//
// 阻塞 pop 用于 TransformNode/SinkNode 的主循环。
// 非阻塞 tryPop 和 peek 用于 MuxNode 的多路复用选择。
// ===================================================================
class SinkPad : public Pad {
public:
    SinkPad(const std::string& name, BaseNode* node, TemplateCaps caps)
        : Pad(name, PadDir::SINK, node, std::move(caps)) {}

    // 阻塞 pop：队列空时等待，flush 时返回 nullopt
    std::optional<QueueItem> popBlocking();

    // 非阻塞 pop：队列空时返回 nullopt
    std::optional<QueueItem> tryPop();

    // 查看队首但不取出（MuxNode 选最小 DTS 时使用）
    std::optional<QueueItem> peek() const;
};

} // namespace pipeline
