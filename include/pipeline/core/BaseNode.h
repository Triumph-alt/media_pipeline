#pragma once

#include "pipeline/core/Caps.h"
#include "pipeline/core/Event.h"
#include "pipeline/core/MessageBus.h"
#include "pipeline/core/Pad.h"
#include "pipeline/core/Types.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace pipeline {

// 前向声明
class Pipeline;
class Graph;

// ===================================================================
// BaseNode: 节点抽象基类
//
// 生命周期回调（由 Pipeline/Graph 调用）：
//   onReady()      — Ready 阶段第一步，初始化自身资源
//   onStreamInfo() — Ready 阶段第三步，发送/处理 CapsEvent
//   onStop()       — 资源释放（join 线程后调用）
//   runLoop()      — 工作线程主循环
//
// 数据分发（子类调用，不感知下游数量）：
//   pushToDownstream()    — 单路阻塞，多路 tryPush
//   sendEventDownstream() — EOS 阻塞，CapsEvent tryPush
//   sendCapsEvent()       — 向指定 SrcPad 发送 CapsEvent
//   postMessage()         — 统一上报 MessageBus
// ===================================================================

class BaseNode {
public:
    virtual ~BaseNode() = default;

    const std::string& name()  const { return name_; }
    NodeState          state() const { return state_; }
    virtual NodeType   nodeType() const = 0;

    // Pad 访问
    SrcPad*  getSrcPad(const std::string& name);
    SinkPad* getSinkPad(const std::string& name);
    const std::vector<std::unique_ptr<SrcPad>>&  srcPads()  const { return src_pads_; }
    const std::vector<std::unique_ptr<SinkPad>>& sinkPads() const { return sink_pads_; }

protected:
    // ===== 生命周期回调 =====

    // Ready 阶段第一步：初始化自身资源
    // Source/DemuxNode：打开设备/文件，探测流信息，但不发送 CapsEvent
    // Transform/SinkNode：基础初始化（此时尚未收到 CapsEvent）
    // 返回 true 成功，false 失败
    virtual bool onReady() = 0;

    // Ready 阶段第三步：发送/处理 CapsEvent（Queue 已就绪）
    // Source/DemuxNode：构造并发送 CapsEvent
    // TransformNode：取出上游 CapsEvent → 初始化处理器 → 发送输出 CapsEvent
    // SinkNode：取出 CapsEvent → 初始化渲染/播放资源
    // 默认实现返回 true（Sink 节点无需发送）
    virtual bool onStreamInfo() { return true; }

    // 资源释放（Pipeline join 完线程后按拓扑逆序调用）
    virtual void onStop() = 0;

    // 工作线程主循环（由 Pipeline 创建的线程调用）
    virtual void runLoop() = 0;

    // ===== 数据分发 =====

    // 推送 Buffer 到下游
    // src_pad_name 为空：推给所有 SrcPad（单路阻塞，多路 tryPush）
    // src_pad_name 非空：推给指定 SrcPad（DemuxNode 按流类型分发）
    void pushToDownstream(Buffer* buf, const std::string& src_pad_name = "");

    // 推送 Event 到所有下游
    // EOS 关键事件：阻塞 push，不允许丢失
    // CapsEvent 非关键事件：tryPush，满则丢弃
    void sendEventDownstream(const Event& event);

    // 向指定 SrcPad 的下游发送 CapsEvent（阻塞 push，不允许丢失）
    // 只负责发送，不存储。接收方在 onStreamInfo() 里自行存入 negotiated_caps_
    void sendCapsEvent(const std::string& src_pad_name, const CapsEvent& caps);

    // ===== 消息上报 =====

    // 统一上报 MessageBus
    void postMessage(MessageType type, const std::string& text, int code = 0);

    // ===== Pad 管理 =====

    SrcPad*  addSrcPad(const std::string& name, TemplateCaps caps);
    SinkPad* addSinkPad(const std::string& name, TemplateCaps caps);

    // ===== 懒连接支持 =====
    // DemuxNode 重写：link() 时只记录意图，onReady() 里再创建 Pad 和 Edge
    virtual bool needsLazyConnection() const { return false; }
    virtual void addPendingLink(const std::string& /*pad_name*/) {}

    // ===== 成员 =====
    std::string                              name_;
    NodeState                                state_ = NodeState::NULL_STATE;
    Pipeline*                                pipeline_ = nullptr;
    std::vector<std::unique_ptr<SrcPad>>     src_pads_;
    std::vector<std::unique_ptr<SinkPad>>    sink_pads_;
    std::unordered_map<std::string, CapsEvent> negotiated_caps_;
    bool                                     stop_requested_ = false;

    friend class Pipeline;
    friend class Graph;
};

// ===================================================================
// SourceNode: 采集源节点
//
// 无 SinkPad，只有 SrcPad。
// runLoop 循环调用 capture() 采集数据，push 到下游。
// capture() 返回 nullptr 表示 EOF。
// ===================================================================

class SourceNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::SOURCE; }

protected:
    void runLoop() override;

    // 子类实现：阻塞采集一帧数据，返回 nullptr 表示 EOF
    virtual Buffer* capture() = 0;
};

// ===================================================================
// SinkNode: 消费终端节点
//
// 只有 SinkPad，无 SrcPad。
// runLoop 从 SinkPad pop 数据并调用 consume()。
// 收到 EOS 时通过 postMessage 通知 Pipeline。
// ===================================================================

class SinkNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::SINK; }

protected:
    void runLoop() override;

    // 子类实现：消费一帧数据
    virtual void consume(Buffer* buf) = 0;

    // 子类可重写：处理事件（默认收到 EOS 时上报 Pipeline）
    virtual void onEvent(const Event& event);
};

// ===================================================================
// TransformNode: 处理节点
//
// 有 SinkPad 和 SrcPad。
// runLoop 从 SinkPad pop 数据，调用 process() 处理，push 到下游。
// ===================================================================

class TransformNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::TRANSFORM; }

protected:
    void runLoop() override;

    // 子类实现：一个输入，产出 0 到 N 个输出
    // DecodeNode：一个 packet → 0 到 N 帧
    // EncodeNode：一帧 → 0 到 1 个 packet
    virtual void process(Buffer* input, std::vector<Buffer*>& outputs) = 0;

    // 子类可重写：处理事件（默认透传给下游）
    virtual void onEvent(const Event& event);
};

} // namespace pipeline
