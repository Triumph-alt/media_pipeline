#pragma once

#include "pipeline/core/Caps.h"
#include "pipeline/core/Buffer.h"

#include <cstdint>
#include <string>
#include <variant>

namespace pipeline {

// ===================================================================
// Event: 事件系统（std::variant 实现）
//
// 事件与 Buffer 共享同一个 Edge Queue（QueueItem = variant<BufferRef, Event>），
// 保证事件和数据的严格顺序性。
//
// 只有两种事件：
//   CapsEvent — Ready 阶段顺流传递格式参数，tryPush（满则丢弃）
//   EOSEvent  — 数据流结束，关键事件，阻塞 push 不允许丢失
//
// 错误处理不走队列：出错节点通过 MessageBus 上报 ERROR 后退出 runLoop，
// Pipeline 收到 ERROR 消息后统一调用 stop() 清理。
// ===================================================================
struct EOSEvent {};

// Event 是值类型，每个下游各自收到一份独立副本
using Event = std::variant<CapsEvent, EOSEvent>;
using QueueItem = std::variant<BufferRef, Event>;

// EOS 是唯一的关键事件，必须阻塞 push 保证送达
inline bool isCriticalEvent(const Event& event) {
    return std::holds_alternative<EOSEvent>(event);
}

} // namespace pipeline
