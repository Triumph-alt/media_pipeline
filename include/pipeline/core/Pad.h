#pragma once

#include "pipeline/core/Caps.h"
#include "pipeline/core/OutputRoute.h"
#include "pipeline/core/Types.h"

#include <memory>
#include <optional>
#include <string>

namespace pipeline {

class BaseNode;
class Edge;
class Graph;

// ===================================================================
// PadDir: Pad 方向
// ===================================================================
enum class PadDir {
    SRC,
    SINK,
};

// ===================================================================
// Pad 是节点的输入/输出端口。每个 Pad 与一条 Edge 一对一连接；
// 节点分叉仍通过多个 SrcPad 表达，但同一逻辑输出的 SrcPad 可共享一个 OutputRoute

// template_caps_ 是静态的能力集合，声明"本 pad 允许承载的 MediaType" 的集合
// actual_type_ 是首份 Caps 在运行时选定的具体类型，之后保持冻结。
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

    std::string name_;
    PadDir dir_;
    BaseNode* node_;
    TemplateCaps template_caps_;
    Edge* edge_ = nullptr;

    friend class Graph;

private:
    // BaseNode 仅在首份 Caps 选定实际类型时写入；之后该值冻结。
    void setActualType(MediaType t) { actual_type_ = t; }

    std::optional<MediaType> actual_type_;

    friend class BaseNode;
};

// ===================================================================
// SrcPad: 一个静态下游连接，绑定到节点的某条逻辑 OutputRoute
// ===================================================================
class SrcPad : public Pad {
public:
    SrcPad(const std::string& name, BaseNode* node, TemplateCaps caps,
           std::shared_ptr<OutputRoute> route)
        : Pad(name, PadDir::SRC, node, std::move(caps)), route_(std::move(route)) {}

    std::shared_ptr<OutputRoute> route() const { return route_; }

private:
    std::shared_ptr<OutputRoute> route_;
};

// ===================================================================
// SinkPad: 通过 Edge 持有的 RouteSubscription acquire 数据
// ===================================================================
class SinkPad : public Pad {
public:
    SinkPad(const std::string& name, BaseNode* node, TemplateCaps caps)
        : Pad(name, PadDir::SINK, node, std::move(caps)) {}

    std::optional<RouteDelivery> acquireBlocking();
    std::optional<RouteDelivery> tryAcquire();
    std::optional<QueueItem> peek() const;
    void setRouteNotify(std::function<void()> callback);
};

} // namespace pipeline
