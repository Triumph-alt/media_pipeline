#pragma once

#include "pipeline/core/BaseNode.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <sys/time.h>
#include <vector>

namespace pipeline {

// ===================================================================
// V4L2CaptureConfig: 固定协商的视频采集配置
//
// 首版只支持单平面、CPU 可访问的未压缩格式；运行期格式重配、DMA-BUF 和多平面
// V4L2 buffer 不在本节点当前边界内。pixel_format 为 0 时，由节点选择首个支持的格式；
// framerate 是构造期请求值，Ready 通过 G_PARM/S_PARM/G_PARM 保存驱动最终接受结果。
// ===================================================================
struct V4L2CaptureConfig {
    std::string device = "/dev/video0";
    uint32_t width = 640;
    uint32_t height = 480;
    uint32_t pixel_format = 0;
    AVRational framerate = {30, 1};
    uint32_t buffer_count = 4;
    int poll_timeout_ms = 50;
};

// ===================================================================
// V4L2CaptureNode: Linux V4L2 mmap 视频采集 Source
//
// onReady() 完成固定格式协商、mmap、QBUF 和 STREAMON。produce() 使用非阻塞 fd 和
// 有限超时 poll() 观察 stop；每次 DQBUF 后先深拷贝为框架紧密 Buffer，再立即 QBUF
// 归还驱动 buffer。采集 Source 没有自然 EOS。
// ===================================================================
class V4L2CaptureNode final : public SourceNode {
public:
    V4L2CaptureNode(const std::string& name, V4L2CaptureConfig config = {});

protected:
    bool onReady() override;
    void produce(std::vector<QueueItem>& outputs) override;
    void onStop() override;

private:
    struct MappedBuffer {
        void* data = nullptr;
        size_t length = 0;
    };

    bool openAndConfigureDevice();
    bool selectAndSetFormat();
    bool negotiateFrameRate();
    bool requestAndMapBuffers();
    bool queueDriverBuffer(uint32_t index);
    bool startStreaming();
    bool dequeueAndCopy(std::vector<QueueItem>& outputs);
    bool copyDequeuedBuffer(uint32_t index, const timeval& timestamp, uint32_t flags,
                            std::vector<QueueItem>& outputs);
    bool appendInitialCaps(std::vector<QueueItem>& outputs);
    void closeDevice();

    V4L2CaptureConfig config_;
    int fd_ = -1;
    std::vector<MappedBuffer> mapped_buffers_;
    uint32_t v4l2_pixel_format_ = 0;
    uint32_t bytes_per_line_ = 0;
    uint32_t image_size_ = 0;
    AVPixelFormat av_pixel_format_ = AV_PIX_FMT_NONE;
    AVRational negotiated_framerate_ = {0, 1};
    int64_t nominal_frame_duration_us_ = 0;
    uint64_t captured_frames_ = 0;
    bool streaming_ = false;
    bool initial_caps_emitted_ = false;
};

} // namespace pipeline
