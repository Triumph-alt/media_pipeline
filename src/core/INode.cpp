#include "pipeline/core/INode.h"
#include "pipeline/core/Pipeline.h"
#include "pipeline/core/Types.h"

namespace pipeline {

// ===================================================================
// INode 基类
// ===================================================================

INode::INode(const std::string& name) : m_name(name) {}

// ===== 参数 =====

void INode::setParam(const std::string& key, ParamValue value) {
    m_params[key] = std::move(value);
}

bool INode::hasParam(const std::string& key) const {
    return m_params.count(key) > 0;
}

// ===== Pad 查询 =====

std::vector<SrcPad*> INode::srcPads() const {
    std::vector<SrcPad*> result;
    for (auto& p : m_srcPads) {
        result.push_back(p.get());
    }
    return result;
}

std::vector<SinkPad*> INode::sinkPads() const {
    std::vector<SinkPad*> result;
    for (auto& p : m_sinkPads) {
        result.push_back(p.get());
    }
    return result;
}

SrcPad* INode::getSrcPad(const std::string& name) {
    for (auto& p : m_srcPads) {
        if (p->name() == name) {
            return p.get();
        }
    }
    return nullptr;
}

SinkPad* INode::getSinkPad(const std::string& name) {
    for (auto& p : m_sinkPads) {
        if (p->name() == name) {
            return p.get();
        }
    }
    return nullptr;
}

SrcPad* INode::getSrcPad(MediaType type) {
    for (auto& p : m_srcPads) {
        if (p->streamInfo().type == type) {
            return p.get();
        }
    }
    return nullptr;
}

SinkPad* INode::getSinkPad(MediaType type) {
    for (auto& p : m_sinkPads) {
        if (p->streamInfo().type == type) {
            return p.get();
        }
    }
    return nullptr;
}

// ===== Pad 按需创建（默认实现）=====

SrcPad* INode::requestSrcPad(MediaType type) {
    // 默认实现：检查是否已有同类型 Pad，有则复用
    SrcPad* existing = getSrcPad(type);
    if (existing) return existing;

    // 按 MediaType 自动生成名字
    std::string name;
    size_t maxBufs = 5;
    switch (type) {
    case MediaType::VIDEO: name = "video"; maxBufs = 5; break;
    case MediaType::AUDIO: name = "audio"; maxBufs = 20; break;
    default: name = "out"; break;
    }
    return createSrcPad(name, maxBufs);
}

SinkPad* INode::requestSinkPad(MediaType type) {
    // 默认实现：检查是否已有同类型 Pad，有则复用
    SinkPad* existing = getSinkPad(type);
    if (existing) return existing;

    std::string name;
    size_t maxBufs = 5;
    switch (type) {
    case MediaType::VIDEO: name = "video"; maxBufs = 5; break;
    case MediaType::AUDIO: name = "audio"; maxBufs = 20; break;
    default: name = "in"; break;
    }
    return createSinkPad(name, maxBufs);
}

// ===== Pad 创建 =====

SrcPad* INode::createSrcPad(const std::string& name,
                             size_t maxBuffers, OverflowPolicy policy) {
    auto pad = std::make_unique<SrcPad>(name, this, maxBuffers, policy);
    SrcPad* ptr = pad.get();
    m_srcPads.push_back(std::move(pad));
    return ptr;
}

SinkPad* INode::createSinkPad(const std::string& name,
                               size_t maxBuffers, OverflowPolicy policy) {
    auto pad = std::make_unique<SinkPad>(name, this, maxBuffers, policy);
    SinkPad* ptr = pad.get();
    m_sinkPads.push_back(std::move(pad));
    return ptr;
}

// ===== 连接：只记录意图，不建立物理连接 =====

INode* INode::link(INode* downstream, MediaType type) {
    PendingLink pending;
    pending.srcNode = this;
    pending.sinkNode = downstream;
    pending.mediaType = type;

    m_pendingLinks.push_back(std::move(pending));
    return downstream;
}

// ===== 生命周期 =====

void INode::probe() {
    onProbe();
}

void INode::ready() {
    onReady();
}

void INode::createThread() {
    m_workerThread = std::thread([this]() {
        // 线程创建后立即等待 PLAYING 状态
        std::unique_lock lock(m_stateMutex);
        m_stateCV.wait(lock, [this] {
            return m_state == NodeState::PLAYING || m_stopRequested;
        });
        if (m_stopRequested) {
            return;
        }
        lock.unlock();

        onPlaying();
        workerLoop();
    });
}

void INode::setState(NodeState s) {
    {
        std::lock_guard lock(m_stateMutex);
        m_state = s;
    }
    m_stateCV.notify_all();
    if (s == NodeState::PLAYING) {
        onPlaying();
    }
    if (s == NodeState::PAUSED) {
        onPause();
    }
}

void INode::waitThreadExit() {
    if (m_workerThread.joinable()) {
        m_workerThread.join();
    }
}

void INode::null() {
    onNull();
}

// ===== 命令响应 =====

void INode::handleCommand(Command* cmd) {
    switch (cmd->typeId()) {
    case CmdId::STOP:
        m_stopRequested = true;
        m_stateCV.notify_all();  // 唤醒可能在 wait 中的线程
        break;
    default:
        break;
    }
}

// ===== 默认 workerLoop =====

void INode::workerLoop() {
    // 基类默认实现：什么都不做，子类应该重写
}

// ===================================================================
// SourceNode
// ===================================================================

void SourceNode::workerLoop() {
    while (true) {
        // 检查停止标志
        {
            std::lock_guard lock(m_stateMutex);
            if (m_stopRequested) break;
        }

        auto buffer = generateData();

        if (buffer && !m_srcPads.empty()) {
            m_srcPads[0]->push(std::move(buffer));
        }

        if (isEOF()) {
            for (auto& pad : m_srcPads) {
                pad->pushEvent(Event::makeEOS());
            }
            break;
        }
    }
}

// ===================================================================
// TransformNode
// ===================================================================

void TransformNode::workerLoop() {
    while (true) {
        {
            std::lock_guard lock(m_stateMutex);
            if (m_stopRequested) {
                break;
            }
        }

        if (m_sinkPads.empty()) {
            break;
        }

        auto result = m_sinkPads[0]->pop(std::chrono::milliseconds(100));

        if (result.isEmpty()) {
            // 超时或 flush，继续循环检查 stopRequested
            continue;
        }

        if (result.hasEvent()) {
            handleEvent(result.event(), m_sinkPads[0].get());
            if (result.event()->type == Event::Type::EOS) {
                // 传播 EOS 到输出
                for (auto& pad : m_srcPads) {
                    pad->pushEvent(Event::makeEOS());
                }
                break;
            }
            continue;
        }

        if (result.hasBuffer()) {
            auto output = process(result.buffer());
            if (output && !m_srcPads.empty()) {
                m_srcPads[0]->push(std::move(output));
            }
        }
    }
}

// ===================================================================
// SinkNode
// ===================================================================

void SinkNode::workerLoop() {
    while (true) {
        {
            std::lock_guard lock(m_stateMutex);
            if (m_stopRequested) {
                break;
            }
        }

        if (m_sinkPads.empty()) {
            break;
        }

        auto result = m_sinkPads[0]->pop(std::chrono::milliseconds(100));
        if (result.isEmpty()) {
            continue;
        }

        if (result.hasEvent()) {
            if (result.event()->type == Event::Type::EOS) {
                handleEOS();
                m_pipeline->reportSinkEOS(this);
                break;
            }
            continue;
        }

        if (result.hasBuffer()) {
            consume(result.buffer());
        }
    }
}

} // namespace pipeline
