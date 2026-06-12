#include "pipeline/core/Pipeline.h"

#include <algorithm>
#include <cassert>
#include <queue>
#include <unordered_map>
#include <unordered_set>

namespace pipeline {

Pipeline::Pipeline(const std::string& name) : m_name(name) {
    m_bus = std::make_unique<MessageBus>();
    m_clock = std::make_unique<Clock>();

    // MemoryPool 使用默认配置
    MemoryPool::TierConfig configs[MemoryPool::TIER_COUNT] = {
        {  4 * 1024,         128  },
        { 64 * 1024,          64  },
        { 512 * 1024,         16  },
        {  4 * 1024 * 1024,    8  },
        { 16 * 1024 * 1024,    4  },
    };
    m_memoryPool = std::make_unique<MemoryPool>(configs);
    m_memoryPool->init();
}

Pipeline::~Pipeline() {
    if (m_state == PipelineState::PLAYING || m_state == PipelineState::PAUSED) {
        stop();
        waitForStop();
    }
}

// ===== 消息 =====

void Pipeline::setMessageCallback(std::function<void(const Message&)> cb) {
    m_bus->setCallback(std::move(cb));
}

// ===== EOS =====

void Pipeline::reportSinkEOS(INode* /*sink*/) {
    std::lock_guard lock(m_waitMutex);
    m_eosCount++;
    if (m_eosCount == m_sinkCount) {
        m_waitCV.notify_all();
    }
}

// ===== 错误 =====

void Pipeline::reportNodeError(INode* node, int code, const std::string& msg) {
    m_bus->post({Message::Type::ERROR, node, msg, code});
    {
        std::lock_guard lock(m_waitMutex);
        m_errorOccurred = true;
    }
    m_waitCV.notify_all();
}

// ===== play：7 阶段启动 =====

void Pipeline::play() {
    assert(m_state == PipelineState::NULL_STATE && "Pipeline already playing");

    m_state = PipelineState::READY;
    m_bus->post({Message::Type::STATE_CHANGED, nullptr, "Pipeline → READY"});

    phaseTopologicalSort();
    phaseProbe();
    phaseResolveLinks();

    // 连接阶段有错误则不继续
    if (m_errorOccurred) {
        m_state = PipelineState::ERROR;
        m_bus->post({Message::Type::ERROR, nullptr, "Pipeline build failed"});
        return;
    }

    phaseValidate();
    phaseReady();

    m_state = PipelineState::PAUSED;
    m_bus->post({Message::Type::STATE_CHANGED, nullptr, "Pipeline → PAUSED"});

    phaseCreateThreads();
    phaseStart();

    m_state = PipelineState::PLAYING;
    m_bus->post({Message::Type::STATE_CHANGED, nullptr, "Pipeline → PLAYING"});
}

// ===== 命令 =====

void Pipeline::sendCommand(std::shared_ptr<Command> cmd) {
    // 1. 分发给所有节点
    for (auto& node : m_nodes) {
        node->handleCommand(cmd.get());
    }
    // 2. Pipeline 自身处理
    handleCommand(cmd.get());
}

void Pipeline::handleCommand(Command* cmd) {
    switch (cmd->typeId()) {
    case CmdId::STOP: {
        // 等待所有节点线程退出
        for (auto& node : m_nodes) {
            node->waitThreadExit();
        }
        // 按逆拓扑顺序释放资源
        for (auto it = m_sortedNodes.rbegin(); it != m_sortedNodes.rend(); ++it) {
            (*it)->null();
        }
        m_state = PipelineState::STOPPED;
        m_bus->post({Message::Type::STATE_CHANGED, nullptr, "Pipeline → STOPPED"});
        // 唤醒 waitForStop()
        {
            std::lock_guard lock(m_waitMutex);
        }
        m_waitCV.notify_all();
        break;
    }
    default:
        break;
    }
}

void Pipeline::waitForStop() {
    std::unique_lock lock(m_waitMutex);
    m_waitCV.wait(lock, [this] {
        return m_state == PipelineState::STOPPED
            || m_eosCount == m_sinkCount
            || m_errorOccurred;
    });

    // 如果是 EOS 或错误唤醒的，由当前线程执行 stop()
    if (m_state != PipelineState::STOPPED) {
        lock.unlock();
        stop();
    }
}

// ===== 7 阶段实现 =====

// Phase 1：拓扑排序（基于 PendingLink 的边关系，BFS）
void Pipeline::phaseTopologicalSort() {
    // 建邻接表 + 计算入度
    std::unordered_map<INode*, std::vector<INode*>> adj;
    std::unordered_map<INode*, int> inDegree;

    for (auto& node : m_nodes) {
        inDegree[node.get()] = 0;
    }

    for (auto& node : m_nodes) {
        for (auto& link : node->m_pendingLinks) {
            adj[link.srcNode].push_back(link.sinkNode);
            inDegree[link.sinkNode]++;
        }
    }

    // BFS：入度为 0 的先入队
    std::queue<INode*> q;
    for (auto& [node, deg] : inDegree) {
        if (deg == 0) {
            q.push(node);
        }
    }

    m_sortedNodes.clear();
    while (!q.empty()) {
        INode* node = q.front();
        q.pop();
        m_sortedNodes.push_back(node);

        if (adj.count(node)) {
            for (auto* neighbor : adj[node]) {
                inDegree[neighbor]--;
                if (inDegree[neighbor] == 0) {
                    q.push(neighbor);
                }
            }
        }
    }

    // 孤立节点（无 PendingLink）追加到末尾
    for (auto& node : m_nodes) {
        if (std::find(m_sortedNodes.begin(), m_sortedNodes.end(), node.get())
            == m_sortedNodes.end()) {
            m_sortedNodes.push_back(node.get());
        }
    }
}

// Phase 2：Probe（按拓扑顺序，只探测不创建 Pad）
void Pipeline::phaseProbe() {
    for (auto* node : m_sortedNodes) {
        node->probe();
    }
}

// Phase 3：解析连接
void Pipeline::phaseResolveLinks() {
    collectPendingLinks();

    for (auto& link : m_pendingLinks) {
        resolveLink(link);
    }
}

// Phase 4：验证拓扑
void Pipeline::phaseValidate() {
    bool valid = validateTopology();
    if (!valid) {
        m_bus->post({Message::Type::ERROR, nullptr, "Topology validation failed"});
        m_state = PipelineState::ERROR;
    }
}

// Phase 5：Ready（按拓扑顺序 Source → Sink）
void Pipeline::phaseReady() {
    for (auto* node : m_sortedNodes) {
        node->ready();
    }
}

// Phase 6：创建线程（按逆拓扑顺序 Sink → Source）
void Pipeline::phaseCreateThreads() {
    for (auto it = m_sortedNodes.rbegin(); it != m_sortedNodes.rend(); ++it) {
        (*it)->createThread();
    }
}

// Phase 7：启动（按逆拓扑顺序 Sink → Source）
void Pipeline::phaseStart() {
    for (auto it = m_sortedNodes.rbegin(); it != m_sortedNodes.rend(); ++it) {
        (*it)->setState(NodeState::PLAYING);
    }
}

// ===== 辅助方法 =====

void Pipeline::collectPendingLinks() {
    m_pendingLinks.clear();
    // 按拓扑序收集，保证上游的 PendingLink 先处理
    for (auto* node : m_sortedNodes) {
        for (auto& link : node->m_pendingLinks) {
            m_pendingLinks.push_back(link);
        }
        node->m_pendingLinks.clear();
    }
}

void Pipeline::resolveLink(const PendingLink& link) {
    // 获取 SrcPad（先按 MediaType 查找，找不到则 request 创建）
    SrcPad* srcPad = link.srcNode->getSrcPad(link.mediaType);
    if (!srcPad) {
        srcPad = link.srcNode->requestSrcPad(link.mediaType);
    }
    if (!srcPad) {
        m_bus->post({Message::Type::ERROR, link.srcNode,
                     "Stream not available"});
        m_errorOccurred = true;
        return;
    }

    // 获取 SinkPad（先按 MediaType 查找，找不到则 request 创建）
    SinkPad* sinkPad = link.sinkNode->getSinkPad(link.mediaType);
    if (!sinkPad) {
        sinkPad = link.sinkNode->requestSinkPad(link.mediaType);
    }
    if (!sinkPad) {
        m_bus->post({Message::Type::ERROR, link.sinkNode,
                     "SinkPad creation failed"});
        m_errorOccurred = true;
        return;
    }

    // 建立连接
    srcPad->connect(sinkPad);

    // 触发下游节点的 onLink 回调
    if (srcPad->streamInfo().type != MediaType::UNKNOWN) {
        link.sinkNode->onLink(sinkPad, srcPad->streamInfo());
    }
}

bool Pipeline::validateTopology() {
    for (auto& node : m_nodes) {
        if (node->sinkPads().empty() && node->srcPads().empty()) {
            m_bus->post({Message::Type::WARNING, node.get(),
                         "Node has no pads"});
        }
    }

    // 统计有连接的 SinkNode 数量
    m_sinkCount = 0;
    for (auto* node : m_sortedNodes) {
        bool isSink = !node->sinkPads().empty() && node->srcPads().empty();
        if (isSink) {
            // 检查至少有一个 SinkPad 已连接
            for (auto* pad : node->sinkPads()) {
                if (pad->isConnected()) {
                    m_sinkCount++;
                    break;
                }
            }
        }
    }

    return true;
}

} // namespace pipeline
