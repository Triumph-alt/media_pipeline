#pragma once

#include "pipeline/core/Types.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <variant>
#include <vector>

extern "C" {
#include <libavutil/avutil.h>
#include <libavutil/frame.h>
#include <libavutil/pixfmt.h>
#include <libavutil/rational.h>
#include <libavutil/samplefmt.h>
#include <libavcodec/codec_id.h>
#include <libavcodec/avcodec.h>
}

namespace pipeline {

class BufferRef;

// ===================================================================
// BufferMeta: Buffer 的逐帧格式元信息（按数据类型分三种）
// 部分基础格式字段与 CapsEvent 重复，但也携带 packet/frame 级属性
// 同时此字段也为后续运行时格式变化（STREAM_INFO_CHANGED）准备
// ===================================================================
struct VideoRawMeta {
    int width, height;
    AVPixelFormat pix_fmt;
};

struct AudioRawMeta {
    int sample_rate;
    int channels;
    int nb_samples;
    AVSampleFormat sample_fmt;
};

struct EncodedMeta {
    AVCodecID codec_id;
    int width = 0;
    int height = 0;
    int sample_rate = 0;
    int channels = 0;
    int flags = 0;
    std::vector<uint8_t> extradata;
};

using BufferMeta = std::variant<VideoRawMeta, AudioRawMeta, EncodedMeta>;

// ===================================================================
// Buffer: 框架内所有数据的载体
//
// 拥有独立的原子引用计数体系，与 FFmpeg 的 AVFrame/AVPacket 解耦。
// 节点从 FFmpeg 结构中拷贝数据填入 Buffer 后，立即释放原始 FFmpeg 结构。
//
// 生命周期管理：
//   - Buffer::ref()   增加引用计数
//   - Buffer::unref() 减少引用计数，归零时 delete this（析构释放 data）
//   - 通过 BufferRef RAII 包装自动管理
//
// 分叉策略：
//   - OutputRoute Entry 只保存一份 BufferRef
//   - 多个订阅者复制 BufferRef 句柄，共享发布后只读的 payload
//   - clone() 仅保留为显式深拷贝工具，不用于正式分叉传输
// ===================================================================
class Buffer {
public:
    // ===== 数据区 =====
    uint8_t* data = nullptr;
    size_t size = 0;

    // ===== 时间戳（微秒）=====
    int64_t pts = AV_NOPTS_VALUE;
    int64_t dts = AV_NOPTS_VALUE;
    int64_t duration = 0;

    // ===== 类型与元信息 =====
    MediaType media_type;
    BufferMeta meta;

    // ===== 引用计数 =====
    mutable std::atomic<int> ref_count{1};

    void ref() const {
        ref_count.fetch_add(1, std::memory_order_relaxed);
    }

    void unref() const {
        if (ref_count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            delete this;
        }
    }

    // 为异步只读持有者创建共享引用，不复制 payload。
    BufferRef share() const;

    // ===== 显式深拷贝工具（正式分叉传输不调用）=====
    Buffer* clone() const;

    // ===== 工厂方法：从 FFmpeg 结构拷入数据 =====
    // time_base 用于将 FFmpeg 时间戳转为微秒存入 Buffer
    static Buffer* fromAVPacket(const AVPacket* pkt, MediaType type,
                                AVRational time_base, AVCodecID codec_id = AV_CODEC_ID_NONE);
    static Buffer* fromAVFrame(const AVFrame* frame, MediaType type,
                               AVRational time_base,
                               AVRational framerate = {0, 1});

private:
    // 析构私有：只能通过 unref() 间接释放
    ~Buffer() { delete[] data; }
};

// ===================================================================
// BufferRef: Buffer 的 RAII 包装
//
// 自动管理引用计数，拷贝时 ref()，析构时 unref()。
// QueueItem = variant<BufferRef, Event>，BufferRef 必须可拷贝/可移动。
// ===================================================================
class BufferRef {
public:
    explicit BufferRef(const Buffer* buf = nullptr) : buf_(buf) {}

    ~BufferRef() {
        if (buf_) buf_->unref();
    }

    // 拷贝：增加引用
    BufferRef(const BufferRef& other) : buf_(other.buf_) {
        if (buf_) buf_->ref();
    }

    BufferRef& operator=(const BufferRef& other) {
        if (this != &other) {
            if (buf_) buf_->unref();
            buf_ = other.buf_;
            if (buf_) buf_->ref();
        }
        return *this;
    }

    // 移动：转移所有权
    BufferRef(BufferRef&& other) noexcept : buf_(other.buf_) {
        other.buf_ = nullptr;
    }

    BufferRef& operator=(BufferRef&& other) noexcept {
        if (this != &other) {
            if (buf_) buf_->unref();
            buf_ = other.buf_;
            other.buf_ = nullptr;
        }
        return *this;
    }

    // 访问
    const Buffer* get() const { return buf_; }
    const Buffer* operator->() const { return buf_; }
    explicit operator bool() const { return buf_ != nullptr; }

    // 生产者完成构造、发布前的内部可写访问。发布后的消费接口只暴露 const Buffer。
    Buffer* mutableGet() { return const_cast<Buffer*>(buf_); }

    // 显式深拷贝工具
    BufferRef clone() const {
        return BufferRef(buf_ ? buf_->clone() : nullptr);
    }

private:
    const Buffer* buf_;
};

inline BufferRef Buffer::share() const {
    ref();
    return BufferRef(this);
}

} // namespace pipeline
