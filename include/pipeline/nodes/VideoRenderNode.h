#pragma once

#include "pipeline/core/BaseNode.h"

#include <chrono>
#include <cstdint>
#include <string>

namespace pipeline {

// ===================================================================
// VideoRenderNode: SDL3 视频渲染节点（SinkNode 子类）
//
// SDL VIDEO、Window、Renderer 和 Texture 全部由节点工作线程创建、使用和
// 销毁；节点只处理自身窗口的关闭请求，其他 SDL 输入事件暂不属于本节点范围。
// ===================================================================
class VideoRenderNode final : public SinkNode {
public:
    explicit VideoRenderNode(const std::string& name);

    int renderedFrames() const { return rendered_frames_; }

protected:
    bool onReady() override { return true; }
    bool onStreamInfo() override;
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

    int width_  = 0;
    int height_ = 0;

    // 只允许节点工作线程访问；窗口关闭请求也在该线程轮询处理。
    void* window_   = nullptr;
    void* renderer_ = nullptr;
    void* texture_  = nullptr;
    int texture_width_  = 0;
    int texture_height_ = 0;
    bool sdl_video_initialized_ = false;

    // 纯视频阶段以首帧 PTS 为零点，按 steady clock 实时呈现。
    bool timing_started_ = false;
    int64_t first_pts_us_ = 0;
    std::chrono::steady_clock::time_point timing_start_;

    int rendered_frames_ = 0;
};

} // namespace pipeline
