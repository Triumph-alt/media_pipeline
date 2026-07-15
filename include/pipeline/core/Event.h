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
// Event 与 Buffer 作为 QueueItem 存入同一个 OutputRoute 有序日志，所有静态
// Subscription 以各自游标 acquire，从而看到完全一致的数据/事件顺序。
//
// 当前只有两种事件：
//   CapsEvent — Ready / onStreamInfo 阶段的流级格式协商事件。
//               每条逻辑 Route 只 publish 一次，所有静态订阅者分别 acquire/ack。
//   EOSEvent  — 运行期流结束事件。
//               每条逻辑 Route 只 publish 一次，并等待全部订阅者 ack。
//
// 错误处理不走队列：出错节点通过 MessageBus 上报 ERROR 后退出 runLoop，
// Pipeline 收到 ERROR 消息后统一调用 stop() 清理。
// ===================================================================
struct EOSEvent {};

// Event 是值类型，每个下游各自收到一份独立副本
using Event = std::variant<CapsEvent, EOSEvent>;
using QueueItem = std::variant<BufferRef, Event>;

} // namespace pipeline
