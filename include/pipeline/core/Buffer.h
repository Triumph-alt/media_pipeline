#pragma once

#include "Types.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavutil/frame.h>
#include <libavutil/rational.h>
}

namespace pipeline {

class MemoryPool;

// ===================================================================
// MemoryBlock：一块连续内存的管理单元
//
// 生命周期由 shared_ptr 管理，引用归零时自动触发释放。
// 释放逻辑不写在析构函数里，而是通过 shared_ptr 的自定义 deleter
// 调用 MemoryPool::release()，避免双重释放。
//
// 三种来源：
//   1. MemoryPool 分配 → m_pool 非空 → 释放时归还到池
//   2. 外部内存（V4L2 mmap / DMA-BUF）→ m_releaseCallback 非空 → 释放时调回调
//   3. 池耗尽 fallback → m_pool 为空且无回调 → 释放时直接 free()
// ===================================================================
class MemoryBlock {
    friend class MemoryPool;                 // 允许 Pool 访问私有构造函数和 m_ptr

    void* m_ptr = nullptr;                   // 指向实际数据内存
    size_t m_size = 0;                       // 用户请求的有效数据大小
    size_t m_capacity = 0;                   // 实际分配的块大小（可能 > m_size，因为按级别对齐）
    MemoryTier m_tier;                       // 所属内存级别（用于归还时找到对应的池）
    MemoryPool* m_pool = nullptr;            // 所属池（nullptr 表示非池管理）
    std::function<void()> m_releaseCallback; // 外部释放回调（V4L2 QBUF / close fd）

    // 私有构造：只能由 MemoryPool::alloc() 或 fromExternal() 创建
    MemoryBlock(void* ptr, size_t size, size_t capacity,
                MemoryTier tier, MemoryPool* pool)
        : m_ptr(ptr), m_size(size), m_capacity(capacity),
          m_tier(tier), m_pool(pool) {}

public:
    // 零拷贝工厂：包装外部已有的内存（如 V4L2 mmap 地址、DMA-BUF fd），
    // 不分配新内存，Buffer 释放时通过回调归还给外部。
    static std::shared_ptr<MemoryBlock> fromExternal(
        void* ptr, size_t size, std::function<void()> releaseCallback);

    void* data() { return m_ptr; }
    const void* data() const { return m_ptr; }
    size_t size() const { return m_size; }
    size_t capacity() const { return m_capacity; }
    MemoryTier tier() const { return m_tier; }

    // 调整有效数据大小（不超过已分配的 capacity）
    // 安全前提：只在 Buffer 还没被其他线程看到时调用（push 到队列之前）
    void resize(size_t newSize) {
        assert(newSize <= m_capacity);
        m_size = newSize;
    }

    // 析构为空：所有释放逻辑由 shared_ptr 的自定义 deleter 处理
    ~MemoryBlock() = default;
    MemoryBlock(const MemoryBlock&) = delete;
    MemoryBlock& operator=(const MemoryBlock&) = delete;
};


// ===================================================================
// MemoryPool：分级内存池
//
// 预分配 5 个级别的内存块（TINY 4KB ~ HUGE 16MB），alloc() 从对应
// 级别的空闲列表取块，release() 归还。池耗尽时 fallback 到 malloc。
//
// Pipeline 持有一个 MemoryPool 实例，节点通过 m_pipeline->memoryPool() 访问。
// ===================================================================
class MemoryPool {
public:
    // 每级池的配置：blockSize = 该级别的固定块大小，poolCapacity = 预分配数量
    struct TierConfig {
        size_t blockSize;
        size_t poolCapacity;
    };

    static constexpr size_t TIER_COUNT = static_cast<size_t>(MemoryTier::COUNT);

    // 每个级别的运行时状态
    struct Tier {
        std::vector<void*> freeList;    // 空闲块列表（init 时预填充）
        std::mutex mutex;               // 保护 freeList 和 allocatedCount
        size_t blockSize = 0;           // 该级别的固定块大小
        size_t allocatedCount = 0;      // 已分配出去的块数
        size_t poolCapacity = 0;        // 池容量上限
    };

    explicit MemoryPool(const TierConfig configs[TIER_COUNT]);
    ~MemoryPool();

    void init();   // 预分配所有块到 freeList
    std::shared_ptr<MemoryBlock> alloc(size_t size);  // 从对应级别取块，返回带自定义 deleter 的 shared_ptr
    void release(MemoryBlock* block);  // 归还块到池（由 shared_ptr deleter 调用）

    MemoryPool(const MemoryPool&) = delete;
    MemoryPool& operator=(const MemoryPool&) = delete;

private:
    static MemoryTier tierForSize(size_t size);

    Tier m_tiers[TIER_COUNT];
};

// ===================================================================
// Buffer：一帧数据的载体
//
// 持有 shared_ptr<MemoryBlock> 管理内存，引用归零时自动归还到池。
// 节点间传递 Buffer 只传 shared_ptr（引用计数 +1），不拷贝数据。
//
// 工厂方法说明：
//   fromAVPacket  — 从 FFmpeg 的 AVPacket 创建（编码后的数据，如 H.264 packet）
//   fromAVFrame   — 从 FFmpeg 的 AVFrame 创建（解码后的数据，如 YUV 帧 / PCM）
//   fromRawData   — 从原始字节创建（如 V4L2 采集的裸数据）
//   这些方法内部会从 MemoryPool 分配内存并拷贝一次数据，
//   之后 Buffer 在管线中流转时全程零拷贝。
// ===================================================================

class Buffer {
public:
    std::shared_ptr<MemoryBlock> data;  // 持有内存块，引用归零自动归还到池
    size_t size = 0;                    // 有效数据大小（可能 < data->capacity()）

    // 时间信息
    int64_t pts = AV_NOPTS_VALUE;       // 显示时间戳（Presentation Timestamp）
    int64_t dts = AV_NOPTS_VALUE;       // 解码时间戳（Decoding Timestamp）
    int64_t duration = 0;               // 持续时长
    AVRational time_base = {0, 1};      // 时间基（用于 pts 转微秒：pts * time_base = 秒）
    int streamIndex = -1;               // 区分同一源输出的音/视频流（DemuxNode 的输出 index）

    // 标志位
    enum Flag : uint32_t {
        NONE     = 0,
        KEYFRAME = 1 << 0,              // 关键帧（I 帧）
        EOS      = 1 << 1,              // 流结束标记
        DISCONT  = 1 << 2,              // 不连续（seek 后第一帧）
        HEADER   = 1 << 3,              // codec header（SPS/PPS）
    };
    uint32_t flags = NONE;

    bool isKeyFrame() const { return flags & KEYFRAME; }
    bool isEOS() const { return flags & EOS; }

    // 工厂方法：从 MemoryPool 分配内存，从 FFmpeg 结构体拷贝数据
    static std::shared_ptr<Buffer> fromAVPacket(const AVPacket* pkt,
                                                 AVRational tb,
                                                 MemoryPool* pool);
    static std::shared_ptr<Buffer> fromAVFrame(const AVFrame* frame,
                                                AVRational tb,
                                                MemoryPool* pool);
    static std::shared_ptr<Buffer> fromRawData(const void* data,
                                                size_t size,
                                                MemoryPool* pool);
};

} // namespace pipeline
