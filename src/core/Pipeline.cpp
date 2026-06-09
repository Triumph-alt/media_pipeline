#include "pipeline/core/Pipeline.h"

#include <algorithm>
#include <cassert>
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

// Phase 1：拓扑排序（DFS，Source → Transform → Sink）
void Pipeline::phaseTopologicalSort() {
    // 简单实现：按 SourceNode → TransformNode → SinkNode 分组
    std::vector<INode*> sources, transforms, sinks;

    for (auto& node : m_nodes) {
        // 通过 Pad 数量判断节点类型
        bool hasSink = false;
        bool hasSrc = false;
        for (auto* pad : node->sinkPads()) {
            (void)pad;
            hasSink = true;
            break;
        }
        for (auto* pad : node->srcPads()) {
            (void)pad;
            hasSrc = true;
            break;
        }

        // 还没 probe，Pad 还没创建，用 requestSinkPad 和 getSrcPad 判断
        // SourceNode: 无 SinkPad，有 SrcPad
        // SinkNode: 有 SinkPad，无 SrcPad
        // TransformNode: 两者都有（或都没有，如 QueueNode）
        // 这里先简单按添加顺序，probe 之后再真正排序
        sources.push_back(node.get());
    }

    // 暂时按添加顺序，probe 后会重新排序
    m_sortedNodes = sources;
}

// Phase 2：Probe（按拓扑顺序 Source → Sink）
void Pipeline::phaseProbe() {
    for (auto* node : m_sortedNodes) {
        node->probe();
    }

    // probe 之后重新排序：SourceNode → TransformNode → SinkNode
    std::vector<INode*> sources, transforms, sinks;
    for (auto* node : m_sortedNodes) {
        bool hasSink = !node->sinkPads().empty();
        bool hasSrc = !node->srcPads().empty();

        if (!hasSink && hasSrc) {
            sources.push_back(node);
        } else if (hasSink && !hasSrc) {
            sinks.push_back(node);
        } else {
            transforms.push_back(node);
        }
    }

    m_sortedNodes.clear();
    m_sortedNodes.insert(m_sortedNodes.end(), sources.begin(), sources.end());
    m_sortedNodes.insert(m_sortedNodes.end(), transforms.begin(), transforms.end());
    m_sortedNodes.insert(m_sortedNodes.end(), sinks.begin(), sinks.end());

    // 统计 Sink 节点数
    m_sinkCount = static_cast<int>(sinks.size());
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
    for (auto& node : m_nodes) {
        for (auto& link : node->m_pendingLinks) {
            m_pendingLinks.push_back(link);
        }
        node->m_pendingLinks.clear();
    }
}

void Pipeline::resolveLink(const PendingLink& link) {
    // 获取 SrcPad
    SrcPad* srcPad = link.srcNode->getSrcPad(link.srcPadName);
    if (!srcPad) {
        m_bus->post({Message::Type::ERROR, link.srcNode,
                     "SrcPad not found: " + link.srcPadName});
        return;
    }

    // 获取 SinkPad（支持 requestSinkPad 动态创建）
    SinkPad* sinkPad = link.sinkNode->getSinkPad(link.sinkPadName);
    if (!sinkPad) {
        sinkPad = link.sinkNode->requestSinkPad(link.sinkPadName);
    }
    if (!sinkPad) {
        m_bus->post({Message::Type::ERROR, link.sinkNode,
                     "SinkPad not found: " + link.sinkPadName});
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
        // 检查 SinkNode 的 SinkPad 必须已连接
        if (node->sinkPads().empty() && node->srcPads().empty()) {
            m_bus->post({Message::Type::WARNING, node.get(),
                         "Node has no pads"});
        }
    }
    return true;
}

} // namespace pipeline
