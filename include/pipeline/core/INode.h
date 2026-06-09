#pragma once

#include "Command.h"
#include "Pad.h"
#include "Types.h"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace pipeline {

class Pipeline;
class MessageBus;

// ===================================================================
// PendingLink：link() 记录的连接意图，play() Phase 3 才真正建立连接
// ===================================================================

struct PendingLink {
    INode* srcNode;
    std::string srcPadName;
    INode* sinkNode;
    std::string sinkPadName;
};

// ===================================================================
// INode：节点抽象基类
//
// 子类根据节点类型继承 SourceNode / TransformNode / SinkNode，
// 不要直接继承 INode。
// ===================================================================

class INode {
    friend class Pipeline;  // Pipeline 需要访问 handleCommand、m_pendingLinks 等

public:
    virtual ~INode() = default;

    // ===== 基本信息 =====
    const std::string& name() const { return m_name; }
    NodeState state() const { return m_state; }

    // ===== 参数 =====
    using ParamValue = pipeline::ParamValue;
    void setParam(const std::string& key, ParamValue value);
    template<typename T>
    T getParam(const std::string& key, T defaultValue = T{}) const;
    bool hasParam(const std::string& key) const;

    // ===== Pad =====
    std::vector<SrcPad*> srcPads() const;
    std::vector<SinkPad*> sinkPads() const;
    virtual SrcPad* getSrcPad(const std::string& name = "out");
    virtual SinkPad* getSinkPad(const std::string& name = "in");
    virtual SinkPad* requestSinkPad(const std::string& name);  // 默认返回 nullptr，MuxNode 重写

    // ===== 连接（延迟绑定：只记录意图，play() Phase 3 才真正连接）=====
    INode* link(INode* downstream,
                const std::string& srcPadName = "",
                const std::string& sinkPadName = "");

    // ===== 生命周期（由 Pipeline 调用）=====
    void probe();
    void ready();
    void createThread();
    void setState(NodeState s);
    void waitThreadExit();
    void null();

protected:
    explicit INode(const std::string& name);

    // 子类重写的虚函数
    virtual void onProbe() {}                     // 创建 Pad
    virtual void onReady() = 0;                   // 分配资源
    virtual void onNull() = 0;                    // 释放资源
    virtual void onPlaying() {}                   // 线程开始工作
    virtual void onPause() {}                     // 线程暂停
    virtual void onLink(SinkPad* pad, const StreamInfo& info) {}
    virtual void onEvent(std::shared_ptr<Event> event, Pad* from) {}

    // 命令响应（子类重写以处理自定义命令，先调基类）
    virtual void handleCommand(Command* cmd);

    // 工作线程主循环，子类可重写
    virtual void workerLoop();

    // 创建 Pad 的辅助方法
    SrcPad* createSrcPad(const std::string& name,
                         size_t maxBuffers = 5,
                         OverflowPolicy policy = OverflowPolicy::BLOCK);
    SinkPad* createSinkPad(const std::string& name,
                           size_t maxBuffers = 5,
                           OverflowPolicy policy = OverflowPolicy::BLOCK);

    // ===== 内部成员 =====
    std::string m_name;
    NodeState m_state = NodeState::NULL_STATE;
    Pipeline* m_pipeline = nullptr;
    MessageBus* m_bus = nullptr;
    std::unordered_map<std::string, ParamValue> m_params;
    std::vector<std::unique_ptr<SrcPad>> m_srcPads;
    std::vector<std::unique_ptr<SinkPad>> m_sinkPads;
    std::vector<PendingLink> m_pendingLinks;  // link() 记录的连接意图，play() 时消费
    std::thread m_workerThread;
    std::mutex m_stateMutex;
    std::condition_variable m_stateCV;
    bool m_stopRequested = false;
};

// getParam 实现
template<typename T>
T INode::getParam(const std::string& key, T defaultValue) const {
    auto it = m_params.find(key);
    if (it == m_params.end()) {
        return defaultValue;
    }
    
    auto* ptr = std::get_if<T>(&it->second);
    return ptr ? *ptr : defaultValue;
}

// ===================================================================
// SourceNode：只有 SrcPad，没有 SinkPad
//
// workerLoop 调用 generateData() 生成数据，push 到 SrcPad。
// isEOF() 返回 true 时发送 EOS。
// ===================================================================

class SourceNode : public INode {
protected:
    explicit SourceNode(const std::string& name) : INode(name) {}

    void workerLoop() override;
    virtual std::shared_ptr<Buffer> generateData() = 0;
    virtual bool isEOF() { return false; }
};

// ===================================================================
// TransformNode：有 SinkPad + SrcPad
//
// workerLoop 从 SinkPad pop → process() → push 到 SrcPad。
// ===================================================================

class TransformNode : public INode {
protected:
    explicit TransformNode(const std::string& name) : INode(name) {}

    void workerLoop() override;
    virtual std::shared_ptr<Buffer> process(std::shared_ptr<Buffer> input) = 0;
    virtual void handleEvent(std::shared_ptr<Event> event, SinkPad* from) {}
};

// ===================================================================
// SinkNode：只有 SinkPad，没有 SrcPad
//
// workerLoop 从 SinkPad pop → consume()。
// 收到 EOS 时调用 handleEOS()，然后通知 Pipeline。
// ===================================================================

class SinkNode : public INode {
protected:
    explicit SinkNode(const std::string& name) : INode(name) {}

    void workerLoop() override;
    virtual void consume(std::shared_ptr<Buffer> buffer) = 0;
    virtual void handleEOS() {}
};

} // namespace pipeline
