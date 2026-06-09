#include "pipeline/core/Pad.h"
#include "pipeline/core/Types.h"

namespace pipeline {

// ===================================================================
// Pad 基类
// ===================================================================

Pad::Pad(PadDirection dir, const std::string& name, INode* node,
         size_t maxBuffers, OverflowPolicy policy)
    : m_direction(dir), m_name(name), m_node(node) {
    m_queue = std::make_shared<BoundedQueue<QueueItem>>(maxBuffers, policy);
}

// 连接：建立双向引用，传递 StreamInfo，共享队列
void Pad::connect(Pad* peer) {
    m_peer = peer;
    peer->m_peer = this;

    // SrcPad 的队列作为数据通道，SinkPad 共享它
    if (m_direction == PadDirection::SRC) {
        peer->m_queue = m_queue;
    }

    // 如果 SrcPad 已有 StreamInfo，传给 SinkPad
    if (m_direction == PadDirection::SRC && m_streamInfo.type != MediaType::UNKNOWN) {
        peer->m_streamInfo = m_streamInfo;
    }
}

void Pad::disconnect() {
    if (m_peer) {
        m_peer->m_peer = nullptr;
        m_peer = nullptr;
    }
}

void Pad::flush() {
    m_queue->flush();
}

// ===================================================================
// SrcPad
// ===================================================================

SrcPad::SrcPad(const std::string& name, INode* node,
               size_t maxBuffers, OverflowPolicy policy)
    : Pad(PadDirection::SRC, name, node, maxBuffers, policy) {}

bool SrcPad::push(std::shared_ptr<Buffer> buffer,
                  std::chrono::milliseconds timeout) {
    return m_queue->push(QueueItem{std::move(buffer)}, timeout);
}

bool SrcPad::pushEvent(std::shared_ptr<Event> event) {
    return m_queue->push(QueueItem{std::move(event)});
}

bool SrcPad::canPush() const {
    return m_queue->canPush();
}

// ===================================================================
// SinkPad
// ===================================================================

SinkPad::SinkPad(const std::string& name, INode* node,
                 size_t maxBuffers, OverflowPolicy policy)
    : Pad(PadDirection::SINK, name, node, maxBuffers, policy) {}

PopResult SinkPad::pop(std::chrono::milliseconds timeout) {
    auto item = m_queue->pop(timeout);
    if (item.has_value()) {
        return PopResult(std::move(item.value()));
    }
    return PopResult();
}

std::shared_ptr<Buffer> SinkPad::peek() {
    auto item = m_queue->peek();
    if (item.has_value() && item.value().index() == 0) {
        return std::get<0>(item.value());
    }
    return nullptr;
}

} // namespace pipeline
