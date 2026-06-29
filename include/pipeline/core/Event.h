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
// 当前只有两种事件：
//   CapsEvent — Ready / onStreamInfo 阶段的流级格式协商事件。
//               每个 SrcPad 的 CapsEvent 可以不同，必须通过
//               BaseNode::sendCapsEvent(src_pad_name, caps) 发送到指定 SrcPad，
//               阻塞发送，不允许丢失。
//   EOSEvent  — 运行期流结束事件。
//               通过 BaseNode::sendEOSDownstream() 广播到所有下游，
//               阻塞发送，不允许丢失。
//
// 错误处理不走队列：出错节点通过 MessageBus 上报 ERROR 后退出 runLoop，
// Pipeline 收到 ERROR 消息后统一调用 stop() 清理。
// ===================================================================
struct EOSEvent {};

// Event 是值类型，每个下游各自收到一份独立副本
using Event = std::variant<CapsEvent, EOSEvent>;
using QueueItem = std::variant<BufferRef, Event>;

} // namespace pipeline
