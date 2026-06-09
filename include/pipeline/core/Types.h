#pragma once

#include <cstdint>
#include <string>
#include <variant>

extern "C" {
#include <libavutil/rational.h>
}

namespace pipeline {

// ===== Pipeline 整体状态 =====
enum class PipelineState : uint8_t {
    NULL_STATE,   // 未创建
    READY,        // 节点已创建、已连接、资源已分配
    PAUSED,       // 工作线程已创建，等待中
    PLAYING,      // 正常运行
    STOPPED,      // 已停止（终态，waitForStop 等待此状态）
    ERROR,        // 异常
};

// ===== 单个节点状态 =====
enum class NodeState : uint8_t {
    NULL_STATE,   // 资源未分配
    READY,        // 资源已分配，线程未创建
    PAUSED,       // 线程存在但等待中
    PLAYING,      // 正常运行
    ERROR,        // 异常
};

// 媒体类型：检查两边 mediaType 是否匹配，防止把视频 Pad 接到音频 Pad 上
enum class MediaType : uint8_t {
    UNKNOWN,
    VIDEO,
    AUDIO,
};

// Pad 连接时校验方向用
enum class PadDirection : uint8_t {
    SRC,
    SINK,
};

// ===== 队列溢出策略 =====
enum class OverflowPolicy : uint8_t {
    BLOCK,          // 阻塞上游 push
    DROP_OLDEST,    // 丢弃队列中最老的
    DROP_NEWEST,    // 丢弃刚到来的
};

// ===== 内存池分级 =====
enum class MemoryTier : uint8_t {
    TINY    = 0,    // < 4 KB
    SMALL   = 1,    // 4 KB ~ 64 KB
    MEDIUM  = 2,    // 64 KB ~ 512 KB
    LARGE   = 3,    // 512 KB ~ 4 MB
    HUGE    = 4,    // > 4 MB
    COUNT   = 5,    // 非池管理（fallback 或外部内存）
};

// ===== 节点参数值类型 =====
using ParamValue = std::variant<int, int64_t, float, bool, std::string, AVRational>;

} // namespace pipeline
