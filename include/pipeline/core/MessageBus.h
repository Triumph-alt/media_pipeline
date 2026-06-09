#pragma once

#include <chrono>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <string>

namespace pipeline {

class INode;

// ===================================================================
// Message：节点报告的事件（错误、警告、状态变化等）
// ===================================================================

class Message {
public:
    enum class Type {
        ERROR,              // 错误
        WARNING,            // 警告
        STATE_CHANGED,      // 节点状态变化
        EOS,                // 某个 Sink 收到 EOS
        STREAM_INFO,        // 流信息通知
        BUFFERING,          // 缓冲状态（网络流用）
        CUSTOM,             // 用户自定义
    };

    Type type;
    INode* source = nullptr;        // 来源节点
    std::string text;               // 描述文本
    int code = 0;                   // 错误码
    int streamIndex = -1;
};

// ===================================================================
// MessageBus：消息总线
//
// 节点通过 post() 投递消息，用户通过回调或轮询接收。
// 线程安全：多节点可同时 post，用户线程同时 poll。
// ===================================================================

class MessageBus {
public:
    // 节点调用：投递消息
    void post(Message msg);

    // 用户设置回调（推荐方式，消息到达时立即触发）
    void setCallback(std::function<void(const Message&)> cb);

    // 用户轮询（可选方式，阻塞等待直到有消息或超时）
    std::optional<Message> poll(std::chrono::milliseconds timeout);

    // 非阻塞轮询
    std::optional<Message> tryPoll();

private:
    std::queue<Message> m_queue;
    std::mutex m_mutex;
    std::function<void(const Message&)> m_callback;
};

} // namespace pipeline
