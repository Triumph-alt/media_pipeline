#pragma once

#include "pipeline/core/BaseNode.h"

#include <cstdint>

namespace pipeline {

// ===================================================================
// VideoRenderNode: SDL3 视频渲染 Sink
//
// SDL VIDEO、Window、Renderer、Texture 全部由节点 worker 创建、使用和销毁。
// 视频格式由 Running Route 上的完整 CapsEvent 唯一描述；当前只接受紧密 YUV420P，
// 非 YUV420P 的 swscale 路径作为后续独立工作。
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
    bool openRenderer();
    void closeRenderer();
    bool ensureTexture(int width, int height);
    bool failRender(const std::string& message);
    bool pollWindowCloseRequested();
    // 返回 true 表示当前帧应呈现；返回 false 表示当前帧应跳过或等待被 stop 打断。
    bool waitForPresentationTime(int64_t pts_us, int64_t duration_us);
    bool waitForStartupBarrier();

    int width_ = 0;
    int height_ = 0;

    // 只允许节点工作线程访问；窗口关闭请求也在该线程轮询处理。
    void* window_ = nullptr;
    void* renderer_ = nullptr;
    void* texture_ = nullptr;
    int texture_width_ = 0;
    int texture_height_ = 0;
    bool sdl_video_initialized_ = false;

    // 首帧在 Texture 上传后、SDL_RenderPresent 前到达本轮共同起跑栅栏。
    bool startup_barrier_arrived_ = false;
    bool startup_barrier_withdrawn_ = false;

    int rendered_frames_ = 0;
    int dropped_frames_ = 0;
};

} // namespace pipeline
