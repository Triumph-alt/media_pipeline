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
// Pad: 节点的输入/输出端口
//
// 每个 Pad 通过 Edge 间接访问 Queue，Pad 自身不持有队列。
// Pad 与 Edge 严格一对一：一个 SrcPad 只连一个 SinkPad，
// 分叉通过节点创建多个 SrcPad 实现。
// ===================================================================
class Pad {
public:
    const std::string& name() const { 
        return name_; 
    }

    PadDir dir() const { 
        return dir_; 
    }

    BaseNode* node() const { 
        return node_; 
    }

    TemplateCaps templateCaps() const { 
        return template_caps_; 
    }

    bool isConnected() const { 
        return edge_ != nullptr; 
    }

    Edge* edge() const {
        return edge_;
    }

protected:
    Pad(const std::string& name, PadDir dir, BaseNode* node, TemplateCaps caps)
        : name_(name), dir_(dir), node_(node), template_caps_(std::move(caps)) {}

    std::string name_;
    PadDir dir_;
    BaseNode* node_;
    TemplateCaps template_caps_;
    Edge* edge_ = nullptr;

    friend class Graph;
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
