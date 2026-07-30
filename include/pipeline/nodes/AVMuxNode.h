#pragma once

#include "pipeline/core/BaseNode.h"

#include <cstddef>
#include <cstdint>

struct AVFormatContext;
struct AVIOContext;

namespace pipeline {

// ===================================================================
// AVMuxNode: 基于 FFmpeg 的流式复用具体节点
//
// 首版只实现单视频 MPEG-TS。MuxNode 基类负责输入 Caps 汇合、全局 DTS 调度、
// CONTAINER Buffer 发布和 EOS；本类只把已排序的 encoded Buffer 写入 FFmpeg muxer。
// 自定义 AVIO callback 仅复制临时容器字节到基类 pending 输出，不直接操作 Route。
// ===================================================================
class AVMuxNode final : public MuxNode {
public:
    AVMuxNode(const std::string& name, MuxFormat format)
        : MuxNode(name, format) {}

private:
    SinkPad* requestSinkPad(const std::string& name, MediaType hint_type) override;

    static int writeCallback(void* opaque, const uint8_t* data, int size);

    bool allocateContext(MuxFormat format) override;
    bool addStream(const CapsEvent& caps, int* stream_index) override;
    bool writeHeader(MuxFormat format) override;
    bool writePacket(const Buffer* buf, int stream_index) override;
    bool writeTrailer() override;
    void closeContext() override;

    bool flushAvio();

    AVFormatContext* fmt_ctx_ = nullptr;
    AVIOContext* avio_ctx_ = nullptr;
};

} // namespace pipeline
