#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <string>

namespace pipeline {

// 前向声明
class BaseNode;

// ===================================================================
// MessageType: 消息类型
//
// EOS     — Sink 节点收到 EOSEvent 后上报，Pipeline 用于 EOS 计数
// ERROR   — 节点出错后上报，Pipeline 设置 error_occurred_ 并唤醒 waitEOS
// WARNING — 可恢复的异常（如损坏的 packet、队列满丢帧），透传给用户观测
// INFO    — 一般信息（如初始化完成），透传给用户观测
// ===================================================================
enum class MessageType {
    EOS,
    ERROR,
    WARNING,
    INFO,
};

// ===================================================================
// MessageBus: 统一消息总线
//
// 所有节点通过 post() 上报消息，Pipeline 内部运行独立监听线程
// 通过 waitMessage() 阻塞接收并分发处理。
//
// 用户可通过 setObserver() 注册观测回调，只接收 WARNING 和 INFO。
// 回调在监听线程里执行，用户保证轻量。
// ===================================================================
class MessageBus {
public:
    struct Message {
        MessageType  type;
        BaseNode*    sender = nullptr;
        std::string  text;
        int          code   = 0;
    };

    using ObserverCallback = std::function<void(const Message&)>;

    // ===== 节点调用（线程安全）=====
    void post(Message msg);

    // ===== Pipeline 监听线程调用 =====
    // 阻塞等待下一条消息
    // running 为 false 且队列空时返回 nullopt，监听线程据此退出
    std::optional<Message> waitMessage(std::atomic<bool>& running);

    // ===== 用户侧 =====
    // 注册观测回调，只接收 WARNING 和 INFO
    void setObserver(ObserverCallback cb);

    // 唤醒可能阻塞在 waitMessage 上的线程（stop 时用）
    void notify();

    // 触发用户观测回调（Pipeline messageBusLoop 内部调用）
    // 只有 WARNING / INFO 消息走这条路
    // 注意：持有 mutex_ 时调用，回调内部不能再调 post()，否则死锁
    void notifyObserver(const Message& msg) {
        std::lock_guard lock(mutex_);
        if (observer_) observer_(msg);
    }

private:
    std::queue<Message>      queue_;
    std::mutex               mutex_;
    std::condition_variable  cv_;
    ObserverCallback         observer_ = nullptr;
};

} // namespace pipeline
