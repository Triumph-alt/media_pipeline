#pragma once

#include "pipeline/core/BaseNode.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>

namespace pipeline {

enum class VideoRenderEvent {
    NONE,
    QUIT,
    ERROR,
};

enum class VideoPresentResult {
    NO_FRAME,
    WAITING,
    PRESENTED,
    ERROR,
};

// ===================================================================
// VideoRenderNode: SDL3 视频渲染节点（SinkNode 子类）
//
// Pipeline 工作线程中的 consume() 只把帧投递到容量为 1 的 mailbox，
// 并等待主线程完成该帧呈现，以此向 Decode/Demux 传递可靠背压。
//
// SDL 视频 API 有主线程约束，因此窗口、事件泵、纹理上传、Present 和
// 资源销毁全部由宿主 main 线程通过 *OnMainThread() 接口驱动。
// ===================================================================
class VideoRenderNode final : public SinkNode {
public:
    explicit VideoRenderNode(const std::string& name);

    bool openOnMainThread();
    VideoRenderEvent pollEventOnMainThread();
    VideoPresentResult presentOnMainThread();
    void cancelMailbox();
    void closeOnMainThread();

    const std::string& lastRenderError() const { return render_error_; }
    int renderedFrames() const { return rendered_frames_; }

protected:
    bool onReady() override { return true; }
    bool onStreamInfo() override;
    void consume(const Buffer* buf) override;
    void onStop() override;

private:
    bool ensureMainThread(const char* operation);
    bool failRender(const std::string& message);
    bool ensureTexture(int width, int height);
    void releasePendingFrame(const Buffer* expected);

    int width_  = 0;
    int height_ = 0;

    // SDL 资源只允许由 main 线程访问。
    void* window_   = nullptr;
    void* renderer_ = nullptr;
    void* texture_  = nullptr;
    int texture_width_  = 0;
    int texture_height_ = 0;
    bool sdl_video_initialized_ = false;

    // 容量为 1 的同步 mailbox。工作线程投递后等待 main 线程呈现完成。
    std::mutex mailbox_mutex_;
    std::condition_variable mailbox_cv_;
    BufferRef pending_frame_;
    bool mailbox_cancelled_ = false;

    // 纯视频阶段以首帧 PTS 为零点，按 steady clock 实时呈现。
    bool timing_started_ = false;
    int64_t first_pts_us_ = 0;
    std::chrono::steady_clock::time_point timing_start_;

    int rendered_frames_ = 0;
    std::string render_error_;
};

} // namespace pipeline
