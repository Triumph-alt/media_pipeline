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
//   CapsEvent：Running 阶段的完整流配置边界。每条逻辑 Route 可出现多次，
//              每一份都必须先于受其管辖的 Buffer。
//   EOSEvent：运行期流结束事件。
//             每条逻辑 Route 只 publish 一次，并等待全部订阅者 ack。
//
// 错误处理通过 MessageBus 上报 ERROR，Pipeline 收到 ERROR 后统一清理
// ===================================================================
struct EOSEvent {};

// Route 中的控制事件：Caps 是完整格式边界，EOS 是自然结束边界
using Event = std::variant<CapsEvent, EOSEvent>;

// QueueItem 同时是 OutputRoute 的实际传输类型和 Transform 的本地待发布序列元素。
// 本地序列与 Route 使用完全相同的表示，因而 Caps、Buffer、EOS 可在一个拥有型
// 序列中按唯一顺序发布：flush 输出 → EOS；BufferRef 在任何提前退出时仍由 RAII 释放。
using QueueItem = std::variant<BufferRef, Event>;

} // namespace pipeline
