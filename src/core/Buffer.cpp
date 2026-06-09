#include "pipeline/core/Buffer.h"

#include <cstdlib>
#include <cstring>

extern "C" {
#include <libavutil/imgutils.h>
#include <libavutil/samplefmt.h>
}

namespace pipeline {

// ===================================================================
// MemoryBlock
// ===================================================================

// 零拷贝工厂：包装外部内存，自定义 deleter 只调回调 + delete 对象，
// 不走 MemoryPool::release()（因为 m_pool 为 nullptr）。
std::shared_ptr<MemoryBlock> MemoryBlock::fromExternal(
        void* ptr, size_t size, std::function<void()> releaseCallback) {
    auto* raw = new MemoryBlock(ptr, size, size, MemoryTier::COUNT, nullptr);
    raw->m_releaseCallback = std::move(releaseCallback);
    
    return std::shared_ptr<MemoryBlock>(raw, [](MemoryBlock* block) {
        if (block->m_releaseCallback) {
            block->m_releaseCallback();
        }
        delete block;
    });
}

// ===================================================================
// MemoryPool
// ===================================================================

// 默认配置：每级块大小和预分配数量
// 总预分配 ≈ 512KB + 4MB + 8MB + 32MB + 64MB ≈ 108MB（可通过 Pipeline setParam 调整）
static constexpr MemoryPool::TierConfig DEFAULT_CONFIGS[MemoryPool::TIER_COUNT] = {
    {  4 * 1024,         128  },   // TINY:   4KB × 128
    { 64 * 1024,          64  },   // SMALL:  64KB × 64
    { 512 * 1024,         16  },   // MEDIUM: 512KB × 16
    {  4 * 1024 * 1024,    8  },   // LARGE:  4MB × 8
    { 16 * 1024 * 1024,    4  },   // HUGE:   16MB × 4
};

// 根据请求的大小自动选择对应的内存级别
MemoryTier MemoryPool::tierForSize(size_t size) {
    if (size <= 4 * 1024) {
        return MemoryTier::TINY;
    } else if (size <= 64 * 1024) {
        return MemoryTier::SMALL;
    } else if (size <= 512 * 1024) {
        return MemoryTier::MEDIUM;
    } else if (size <= 4 * 1024 * 1024) {
        return MemoryTier::LARGE;
    }

    return MemoryTier::HUGE;
}

// 构造时只记录配置，不分配内存（init() 才分配）
MemoryPool::MemoryPool(const TierConfig configs[TIER_COUNT]) {
    for (size_t i = 0; i < TIER_COUNT; i++) {
        m_tiers[i].blockSize = configs[i].blockSize;
        m_tiers[i].poolCapacity = configs[i].poolCapacity;
    }
}

// 预分配所有块到各级别的 freeList
void MemoryPool::init() {
    for (size_t i = 0; i < TIER_COUNT; i++) {
        Tier& t = m_tiers[i];
        t.freeList.reserve(t.poolCapacity);
        for (size_t j = 0; j < t.poolCapacity; j++) {
            void* ptr = nullptr;
            posix_memalign(&ptr, 4096, t.blockSize);
            t.freeList.push_back(ptr);
        }
    }
}

// 分配流程：
//   1. 根据 size 确定级别
//   2. 从该级别 freeList 取块（有锁）
//   3. 池有空闲 → 取出，返回带自定义 deleter 的 shared_ptr
//   4. 池无空闲但未达上限 → 新分配一个（兜底）
//   5. 池耗尽 → fallback 到 malloc，tier 设为 COUNT
std::shared_ptr<MemoryBlock> MemoryPool::alloc(size_t size) {
    MemoryTier tier = tierForSize(size);
    Tier& t = m_tiers[static_cast<int>(tier)];

    void* ptr = nullptr;
    size_t capacity = 0;

    {
        std::lock_guard<std::mutex> lock(t.mutex);
        if (!t.freeList.empty()) {
            ptr = t.freeList.back();
            t.freeList.pop_back();
            capacity = t.blockSize;
            t.allocatedCount++;
        } else if (t.allocatedCount < t.poolCapacity) {
            posix_memalign(&ptr, 4096, t.blockSize);
            capacity = t.blockSize;
            t.allocatedCount++;
        }
    }

    if (ptr) {
        auto* raw = new MemoryBlock(ptr, size, capacity, tier, this);
        return std::shared_ptr<MemoryBlock>(raw, [this](MemoryBlock* block) {
            this->release(block);
        });
    }

    // 池耗尽 → fallback
    ptr = malloc(size);
    capacity = size;
    auto* raw = new MemoryBlock(ptr, size, capacity, MemoryTier::COUNT, nullptr);
    return std::shared_ptr<MemoryBlock>(raw, [](MemoryBlock* block) {
        ::free(block->m_ptr);
        delete block;
    });
}

// 释放流程（由 shared_ptr 的自定义 deleter 调用，不是析构函数）：
//   1. 有 releaseCallback → 外部内存，调回调，delete 对象
//   2. 有 m_pool → 池内块，归还到 freeList，delete 对象
//   3. 都没有 → fallback 块，直接 free(ptr) + delete 对象
void MemoryPool::release(MemoryBlock* block) {
    if (block->m_releaseCallback) {
        block->m_releaseCallback();
        delete block;
        return;
    }

    if (block->m_pool) {
        Tier& t = m_tiers[static_cast<int>(block->m_tier)];
        std::lock_guard<std::mutex> lock(t.mutex);
        t.freeList.push_back(block->m_ptr);
        t.allocatedCount--;
        delete block;
        return;
    }

    ::free(block->m_ptr);
    delete block;
}

MemoryPool::~MemoryPool() {
    for (size_t i = 0; i < TIER_COUNT; i++) {
        Tier& t = m_tiers[i];
        for (void* ptr : t.freeList) {
            ::free(ptr);
        }
        t.freeList.clear();
    }
}

// ===================================================================
// Buffer 工厂方法
// ===================================================================

// 计算一帧的字节大小（视频按像素格式，音频按采样格式）
static size_t calculateFrameSize(const AVFrame* frame) {
    if (frame->width > 0) {
        // 视频帧
        return av_image_get_buffer_size(
            static_cast<AVPixelFormat>(frame->format),
            frame->width, frame->height, 1);
    }
    // 音频帧
    int bytesPerSample = av_get_bytes_per_sample(static_cast<AVSampleFormat>(frame->format));
    return frame->nb_samples * frame->ch_layout.nb_channels * bytesPerSample;
}

static bool isPlanarFormat(int format) {
    // 尝试当视频格式查
    const AVPixFmtDescriptor* desc = av_pix_fmt_desc_get(static_cast<AVPixelFormat>(format));
    if (desc) {
        // 查到了，是视频格式，检查 flags 里有没有 PLANAR 标记
        return (desc->flags & AV_PIX_FMT_FLAG_PLANAR) != 0;
    }

    // 查不到，不是视频格式，当音频格式查
    return av_sample_fmt_is_planar(static_cast<AVSampleFormat>(format));
}

static void copyPlanarFrame(const AVFrame* frame, uint8_t* dst) {
    if (frame->width > 0) {
        // 视频：用 av_image_copy_to_buffer
        av_image_copy_to_buffer(
            dst,
            av_image_get_buffer_size(
                static_cast<AVPixelFormat>(frame->format),
                frame->width, frame->height, 1),
            const_cast<const uint8_t**>(frame->data),
            frame->linesize,
            static_cast<AVPixelFormat>(frame->format),
            frame->width, frame->height, 1);
    } else {
        // 音频 planar：逐 plane 拷贝
        int bytesPerSample = av_get_bytes_per_sample(static_cast<AVSampleFormat>(frame->format));
        size_t offset = 0;

        for (int ch = 0; ch < frame->ch_layout.nb_channels; ch++) {
            size_t planeSize = frame->nb_samples * bytesPerSample;
            memcpy(dst + offset, frame->data[ch], planeSize);
            offset += planeSize;
        }
    }
}

// 从 AVPacket 创建：编码后的数据（如 H.264 packet、AAC packet）
// 从 MemoryPool 分配内存，把 pkt->data 拷贝进去
std::shared_ptr<Buffer> Buffer::fromAVPacket(const AVPacket* pkt,
                                              AVRational tb,
                                              MemoryPool* pool) {
    auto buf = std::make_shared<Buffer>();

    auto block = pool->alloc(pkt->size);
    memcpy(block->data(), pkt->data, pkt->size);

    buf->data = block;
    buf->size = pkt->size;
    buf->pts = pkt->pts;
    buf->dts = pkt->dts;
    buf->duration = pkt->duration;
    buf->time_base = tb;
    buf->streamIndex = pkt->stream_index;
    buf->flags = (pkt->flags & AV_PKT_FLAG_KEY) ? Buffer::KEYFRAME : 0;

    return buf;
}

// 从 AVFrame 创建：解码后的数据（如 YUV420P 帧、PCM 音频）
// planar 格式需要逐 plane 拷贝，interleaved 格式一次 memcpy
std::shared_ptr<Buffer> Buffer::fromAVFrame(const AVFrame* frame,
                                             AVRational tb,
                                             MemoryPool* pool) {
    auto buf = std::make_shared<Buffer>();

    size_t frameSize = calculateFrameSize(frame);
    auto block = pool->alloc(frameSize);

    if (isPlanarFormat(frame->format)) {
        copyPlanarFrame(frame, static_cast<uint8_t*>(block->data()));
    } else {
        memcpy(block->data(), frame->data[0], frameSize);
    }

    buf->data = block;
    buf->size = frameSize;
    buf->pts = frame->pts;
    buf->time_base = tb;

    return buf;
}

std::shared_ptr<Buffer> Buffer::fromRawData(const void* src,
                                             size_t size,
                                             MemoryPool* pool) {
    auto buf = std::make_shared<Buffer>();

    auto block = pool->alloc(size);
    memcpy(block->data(), src, size);

    buf->data = block;
    buf->size = size;

    return buf;
}

} // namespace pipeline
