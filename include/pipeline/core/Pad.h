#pragma once

#include "BoundedQueue.h"
#include "Buffer.h"
#include "Event.h"
#include "StreamInfo.h"
#include "Types.h"

#include <memory>
#include <string>

namespace pipeline {

class INode;

// ===================================================================
// PopResult：pop() 的返回值，区分取出的是 Buffer 还是 Event
// ===================================================================

using QueueItem = std::variant<std::shared_ptr<Buffer>, std::shared_ptr<Event>>;

class PopResult {
public:
    PopResult() = default;
    explicit PopResult(QueueItem item) : m_item(std::move(item)) {}

    bool hasBuffer() const { return m_item.has_value() && m_item->index() == 0; }
    bool hasEvent() const { return m_item.has_value() && m_item->index() == 1; }
    bool isEmpty() const { return !m_item.has_value(); }

    std::shared_ptr<Buffer> buffer() const {
        return std::get<0>(m_item.value());
    }
    std::shared_ptr<Event> event() const {
        return std::get<1>(m_item.value());
    }

private:
    std::optional<QueueItem> m_item;
};

// ===================================================================
// Pad 基类：节点的输入/输出接口
//
// 每个 Pad 持有一个 BoundedQueue，SrcPad 的 push 就是往队列里放，
// SinkPad 的 pop 就是从队列里取。背压通过队列的 BLOCK 策略传导。
// ===================================================================

class Pad {
public:
    Pad(PadDirection dir, const std::string& name, INode* node,
        size_t maxBuffers, OverflowPolicy policy);
    virtual ~Pad() = default;

    PadDirection direction() const { return m_direction; }
    const std::string& name() const { return m_name; }
    INode* node() const { return m_node; }

    // 连接
    void connect(Pad* peer);
    void disconnect();
    bool isConnected() const { return m_peer != nullptr; }
    Pad* peer() const { return m_peer; }

    // StreamInfo
    void setStreamInfo(const StreamInfo& info) { m_streamInfo = info; }
    const StreamInfo& streamInfo() const { return m_streamInfo; }

    // flush：清空队列，唤醒阻塞中的线程
    void flush();

protected:
    // 自我身份认同
    PadDirection m_direction;                         // 端口的方向，SRC 或 SINK
    std::string m_name;                               // 端口的字符串名字
    INode* m_node;                                    // 指向拥有这个端口的节点对象的原始指针

    // 关系网建立
    Pad* m_peer = nullptr;                            // 指向对端端口的指针，建立图的拓扑连接

    // 核心物流数据通道
    StreamInfo m_streamInfo;                          // 当前这个端口正在跑的媒体流的格式元数据
    std::shared_ptr<BoundedQueue<QueueItem>> m_queue; // 管理着一个有界的高并发安全缓冲队列
};

// ===================================================================
// SrcPad：输出端口
//
// 上游节点调 push() 把数据送进队列，队列满时根据策略阻塞或丢帧。
// ===================================================================

class SrcPad : public Pad {
public:
    SrcPad(const std::string& name, INode* node,
           size_t maxBuffers = 5, OverflowPolicy policy = OverflowPolicy::BLOCK);

    bool push(std::shared_ptr<Buffer> buffer,
              std::chrono::milliseconds timeout = std::chrono::milliseconds::max());
    bool pushEvent(std::shared_ptr<Event> event);
    bool canPush() const;
};

// ===================================================================
// SinkPad：输入端口
//
// 下游节点调 pop() 从队列取数据，队列空时阻塞等待。
// ===================================================================

class SinkPad : public Pad {
public:
    SinkPad(const std::string& name, INode* node,
            size_t maxBuffers = 5, OverflowPolicy policy = OverflowPolicy::BLOCK);

    PopResult pop(std::chrono::milliseconds timeout = std::chrono::milliseconds::max());
    std::shared_ptr<Buffer> peek();
};

} // namespace pipeline
