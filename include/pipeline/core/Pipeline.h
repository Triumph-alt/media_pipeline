#pragma once

#include "Buffer.h"
#include "Clock.h"
#include "Command.h"
#include "INode.h"
#include "MessageBus.h"
#include "Types.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace pipeline {

// ===================================================================
// Pipeline：管线管理器
//
// 持有所有节点、MemoryPool、Clock、MessageBus。
// 负责节点生命周期管理（7 阶段 play）、命令分发、EOS 追踪。
// ===================================================================

class Pipeline {
public:
    explicit Pipeline(const std::string& name);
    ~Pipeline();

    // ===== 添加节点 =====
    template<typename T, typename... Args>
    T* addNode(const std::string& name, Args&&... args);

    // ===== 启动（7 阶段流程）=====
    void play();

    // ===== 命令 =====
    void sendCommand(std::shared_ptr<Command> cmd);
    // 等待所有工作线程退出之后清理自身资源
    void stop() { sendCommand(std::make_shared<StopCommand>()); }

    // ===== 等待停止 =====
    void waitForStop();

    // ===== EOS =====
    void reportSinkEOS(INode* sink);

    // ===== 错误 =====
    void reportNodeError(INode* node, int code, const std::string& msg);

    // ===== 状态查询（轮询场景用）=====
    bool isAllEosReached() const { return m_eosCount == m_sinkCount; }
    bool isErrorOccurred() const { return m_errorOccurred; }

    // ===== 消息 =====
    void setMessageCallback(std::function<void(const Message&)> cb);

    // ===== 访问器 =====
    PipelineState state() const { return m_state; }
    MemoryPool* memoryPool() { return m_memoryPool.get(); }
    Clock* clock() { return m_clock.get(); }
    MessageBus* messageBus() { return m_bus.get(); }
    const std::string& name() const { return m_name; }

private:
    // Pipeline 自身对命令的响应
    void handleCommand(Command* cmd);

    // 7 阶段
    void phaseTopologicalSort();
    void phaseProbe();
    void phaseResolveLinks();
    void phaseValidate();
    void phaseReady();
    void phaseCreateThreads();
    void phaseStart();

    // 辅助
    void collectPendingLinks();
    void resolveLink(const PendingLink& link);
    bool validateTopology();

    std::string m_name;
    PipelineState m_state = PipelineState::NULL_STATE;

    std::vector<std::unique_ptr<INode>> m_nodes;
    std::vector<PendingLink> m_pendingLinks;
    std::vector<INode*> m_sortedNodes;  // 拓扑排序后的顺序

    std::unique_ptr<MemoryPool> m_memoryPool;
    std::unique_ptr<Clock> m_clock;
    std::unique_ptr<MessageBus> m_bus;

    int m_sinkCount = 0;
    int m_eosCount = 0;
    bool m_errorOccurred = false;

    std::mutex m_waitMutex;
    std::condition_variable m_waitCV;
};

// ===== addNode 实现 =====

template<typename T, typename... Args>
T* Pipeline::addNode(const std::string& name, Args&&... args) {
    auto node = std::make_unique<T>(name, std::forward<Args>(args)...);
    T* ptr = node.get();
    node->m_pipeline = this;
    node->m_bus = m_bus.get();
    m_nodes.push_back(std::move(node));
    return ptr;
}

} // namespace pipeline
