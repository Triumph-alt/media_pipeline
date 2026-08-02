#pragma once

#include "pipeline/core/BaseNode.h"

#include <cstddef>
#include <cstdint>
#include <vector>

struct AVFormatContext;
struct AVIOContext;

namespace pipeline {

// ===================================================================
// AVMuxNode: 基于 FFmpeg 的流式复用具体节点
//
// 当前支持 MPEG-TS、FLV 与 fragmented MP4（MuxFormat::MP4 固定 fMP4）
// MuxNode 基类负责输入 Caps 汇合、全局 DTS 调度、CONTAINER Buffer 发布和 EOS
// 本类只把已排序的 encoded Buffer 写入 FFmpeg muxer
//
// 自定义 AVIO 的 write 回调只复制容器字节，不直接操作 Route
// MPEG-TS/FLV：严格顺序写出，无 seek
// fMP4：固定 movflags=frag_keyframe+empty_moov+default_base_moof
//   moof 的 size 常先写 0 再 seek 回填；大 fragment 的 moof 会超过 AVIO
//   内部小缓冲，因此 fMP4 额外提供基于未提交 tail 的有限 seek
//   已 commit 到 pending/Route 的前缀不可再改写，也不是传统 moov 回写
// ===================================================================
class AVMuxNode final : public MuxNode {
public:
    AVMuxNode(const std::string& name, MuxFormat format)
        : MuxNode(name, format) {}

private:
    bool acceptsInputPad(MuxFormat format, MediaType hint_type) const override;

    static int writeCallback(void* opaque, const uint8_t* data, int size);
    static int64_t seekCallback(void* opaque, int64_t offset, int whence);

    bool allocateContext(MuxFormat format) override;
    bool addStream(const CapsEvent& caps, int* stream_index) override;
    bool writeHeader(MuxFormat format) override;
    bool writePacket(const Buffer* buf, int stream_index) override;
    bool writeTrailer() override;
    void closeContext() override;

    bool flushAvio();
    bool commitAvioTail();
    void resetAvioTail();
    int writeAvioTail(const uint8_t* data, int size);
    int64_t seekAvioTail(int64_t offset, int whence);

    AVFormatContext* fmt_ctx_ = nullptr;
    AVIOContext* avio_ctx_ = nullptr;
    MuxFormat backend_format_ = MuxFormat::MPEGTS;
    int64_t timestamp_origin_us_ = AV_NOPTS_VALUE;
    int64_t last_dts_us_ = AV_NOPTS_VALUE;

    // fMP4 未提交 AVIO tail：已 commit 前缀不可回写；仅支持在 tail 内 seek
    bool avio_tail_seek_enabled_ = false;
    int64_t avio_committed_ = 0;
    int64_t avio_pos_ = 0;
    int64_t avio_end_ = 0;
    std::vector<uint8_t> avio_tail_;
};

} // namespace pipeline
