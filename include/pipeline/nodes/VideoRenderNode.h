#pragma once

#include "pipeline/core/BaseNode.h"

#include <cstdint>

struct SwsContext;

namespace pipeline {

// ===================================================================
// VideoRenderNode: SDL3 视频渲染 Sink
//
// SDL VIDEO、Window、Renderer、Texture 全部由节点 worker 创建、使用和销毁。
// 视频格式由 Running Route 上的完整 CapsEvent 唯一描述；YUV420P/YUVJ420P 直传 SDL IYUV，
// 其他 CPU 可访问格式由本节点在 SDL 提交前经 swscale 转为紧密 YUV420P。
// 窗口默认适配当前显示器可用区域：Texture 保持视频原始宽高，窗口只缩小不放大，
// Present 时由 SDL 等比缩放到窗口；不要求应用层填写屏幕尺寸。
// ===================================================================
class VideoRenderNode final : public SinkNode {
public:
    explicit VideoRenderNode(const std::string& name);

    int renderedFrames() const { return rendered_frames_; }
    int droppedFrames() const { return dropped_frames_; }

protected:
    bool onReady() override;
    bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override;
    void runLoop() override;
    void consume(const Buffer* buf) override;
    void onDrain() override;
    void onStop() override;

private:
    // 仅供不创建 SDL 窗口的转换单元测试访问；生产路径仍只通过 onCaps()/consume() 调用。
    friend struct VideoRenderNodeTestAccess;

    bool openRenderer();
    void closeRenderer();
    bool ensureTexture(int width, int height);
    // onCaps 为非直传格式准备 swscale；实际逐帧转换仍在 consume 中执行。
    bool configureConversion(const CapsEvent& caps);
    void releaseConversion();
    bool convertFrameToYuv420p(const Buffer* buffer, const CapsEvent& caps,
                               uint8_t* output_data[4], int output_linesize[4]);
    bool failRender(const std::string& message);
    bool pollWindowCloseRequested();
    // 返回 true 表示当前帧应呈现；返回 false 表示当前帧应跳过或等待被 stop 打断。
    bool waitForPresentationTime(int64_t pts_us, int64_t duration_us);
    bool waitForStartupBarrier();
    // 查询主显示器可用区域；失败时不伪造屏幕尺寸，调用方回退为不限幅
    bool queryDisplayUsableSize(int* max_w, int* max_h) const;
    // 将视频宽高等比装入显示器上限，仅缩小不放大
    void fitSizeToDisplay(int video_w, int video_h, int* window_w, int* window_h) const;

    int width_ = 0;
    int height_ = 0;
    AVPixelFormat input_pix_fmt_ = AV_PIX_FMT_NONE;
    bool direct_yuv420p_ = false;

    // 非 YUV420P 输入只在节点 worker 内转换为 SDL IYUV 所需的紧密 YUV420P；
    // context 和缓冲随 Caps 重配替换，并在 worker 退出前与 SDL 资源一起释放。
    SwsContext* sws_ctx_ = nullptr;
    uint8_t* sws_buffer_ = nullptr;
    int sws_buffer_size_ = 0;
    int sws_linesize_[4]{};

    // 只允许节点工作线程访问；窗口关闭请求也在该线程轮询处理。
    void* window_ = nullptr;
    void* renderer_ = nullptr;
    void* texture_ = nullptr;
    int texture_width_ = 0;
    int texture_height_ = 0;
    // openRenderer 后记录的显示器可用宽高；0 表示未取到，窗口不限幅
    int display_max_w_ = 0;
    int display_max_h_ = 0;
    int window_width_ = 0;
    int window_height_ = 0;
    bool sdl_video_initialized_ = false;

    // 首帧在 Texture 上传后、SDL_RenderPresent 前到达本轮共同起跑栅栏。
    bool startup_barrier_arrived_ = false;
    bool startup_barrier_withdrawn_ = false;

    int rendered_frames_ = 0;
    int dropped_frames_ = 0;
};

} // namespace pipeline
