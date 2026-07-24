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

protected:
    bool onReady() override { return true; }
    bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override;
    void runLoop() override;
    void consume(const Buffer* buf) override;
    void onStop() override;

private:
    bool openRenderer();
    void closeRenderer();
    bool ensureTexture(int width, int height);
    bool failRender(const std::string& message);
    bool pollWindowCloseRequested();
    bool waitForPresentationTime(int64_t pts_us);

    int width_ = 0;
    int height_ = 0;

    // 只允许节点工作线程访问；窗口关闭请求也在该线程轮询处理。
    void* window_ = nullptr;
    void* renderer_ = nullptr;
    void* texture_ = nullptr;
    int texture_width_ = 0;
    int texture_height_ = 0;
    bool sdl_video_initialized_ = false;

    int rendered_frames_ = 0;
};

} // namespace pipeline
