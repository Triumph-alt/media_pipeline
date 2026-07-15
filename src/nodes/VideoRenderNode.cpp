#include "pipeline/nodes/VideoRenderNode.h"
#include "pipeline/core/Buffer.h"

extern "C" {
#include <SDL3/SDL.h>
#include <libavutil/pixfmt.h>
}

#include <chrono>
#include <cstdio>
#include <thread>
#include <variant>

namespace pipeline {

// ===================================================================
// 构造函数
// ===================================================================
VideoRenderNode::VideoRenderNode(const std::string& name)
    : SinkNode(name) {
    addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
}

// ===================================================================
// onStreamInfo: 只接收并记录初始 Caps
//
// SDL 视频对象必须在 main 线程创建，不能在 Pipeline::play() 的调用线程
// 之外被工作线程访问。纹理最终按首帧的逐帧 metadata 延迟创建。
// ===================================================================
bool VideoRenderNode::onStreamInfo() {
    if (!receiveCapsEvent("in")) {
        return false;
    }

    const CapsEvent& caps = negotiated_caps_["in"];
    width_ = caps.width;
    height_ = caps.height;

    fprintf(stderr, "[%s] stream info: %dx%d initial_pix_fmt=%d\n",
            name_.c_str(), width_, height_, caps.pix_fmt);
    return true;
}

// ===================================================================
// consume: 工作线程只投递帧到容量为 1 的 mailbox
//
// consume 返回前等待主线程呈现完当前帧，保证纯视频播放按显示速度向上游
// 传递背压，不让 Decode 在几秒内跑完整个文件。
// ===================================================================
void VideoRenderNode::consume(const Buffer* buf) {
    if (!buf || !buf->data || buf->size == 0) {
        return;
    }

    // consume() 得到的是发布后的只读 Buffer；mailbox 独立持有同一底层对象的引用。
    BufferRef frame = buf->share();

    std::unique_lock lock(mailbox_mutex_);
    while (pending_frame_ && !mailbox_cancelled_ && !stop_requested_.load()) {
        mailbox_cv_.wait_for(lock, std::chrono::milliseconds(10));
    }

    if (mailbox_cancelled_ || stop_requested_.load()) {
        return;
    }

    pending_frame_ = std::move(frame);
    mailbox_cv_.notify_all();

    // 只有主线程完成呈现并清空 pending_frame_ 后，才继续消费下一帧。
    while (pending_frame_ && !mailbox_cancelled_ && !stop_requested_.load()) {
        mailbox_cv_.wait_for(lock, std::chrono::milliseconds(10));
    }

    if (mailbox_cancelled_ || stop_requested_.load()) {
        pending_frame_ = BufferRef{};
        mailbox_cv_.notify_all();
    }
}

bool VideoRenderNode::ensureMainThread(const char* operation) {
    if (SDL_IsMainThread()) {
        return true;
    }

    render_error_ = std::string("VideoRenderNode: ") + operation +
                    " must be called on the SDL main thread";
    return false;
}

bool VideoRenderNode::failRender(const std::string& message) {
    render_error_ = message;
    return false;
}

// ===================================================================
// openOnMainThread: 创建 SDL 窗口和 Renderer
// ===================================================================
bool VideoRenderNode::openOnMainThread() {
    if (!ensureMainThread("openOnMainThread")) {
        return false;
    }
    if (window_ || renderer_) {
        return failRender("VideoRenderNode: renderer is already open");
    }

    {
        std::lock_guard lock(mailbox_mutex_);
        mailbox_cancelled_ = false;
        pending_frame_ = BufferRef{};
    }
    timing_started_ = false;
    rendered_frames_ = 0;
    render_error_.clear();

    if (!SDL_InitSubSystem(SDL_INIT_VIDEO)) {
        return failRender(std::string("VideoRenderNode: SDL_InitSubSystem failed: ") +
                          SDL_GetError());
    }
    sdl_video_initialized_ = true;

    int window_width = width_ > 0 ? width_ : 640;
    int window_height = height_ > 0 ? height_ : 360;
    window_ = SDL_CreateWindow("Media Pipeline", window_width, window_height, 0);
    if (!window_) {
        closeOnMainThread();
        return failRender(std::string("VideoRenderNode: SDL_CreateWindow failed: ") +
                          SDL_GetError());
    }

    renderer_ = SDL_CreateRenderer(static_cast<SDL_Window*>(window_), "software");
    if (!renderer_) {
        fprintf(stderr, "[%s] software renderer failed: %s, trying default\n",
                name_.c_str(), SDL_GetError());
        renderer_ = SDL_CreateRenderer(static_cast<SDL_Window*>(window_), nullptr);
    }
    if (!renderer_) {
        std::string error = std::string("VideoRenderNode: SDL_CreateRenderer failed: ") +
                            SDL_GetError();
        closeOnMainThread();
        return failRender(error);
    }

    fprintf(stderr, "[%s] renderer: %s (SDL main thread: %s)\n",
            name_.c_str(),
            SDL_GetRendererName(static_cast<SDL_Renderer*>(renderer_)),
            SDL_IsMainThread() ? "yes" : "no");
    return true;
}

// ===================================================================
// pollEventOnMainThread: main 线程事件泵
// ===================================================================
VideoRenderEvent VideoRenderNode::pollEventOnMainThread() {
    if (!ensureMainThread("pollEventOnMainThread")) {
        return VideoRenderEvent::ERROR;
    }

    SDL_Event event;
    while (SDL_PollEvent(&event)) {
        if (event.type == SDL_EVENT_QUIT ||
            event.type == SDL_EVENT_WINDOW_CLOSE_REQUESTED) {
            return VideoRenderEvent::QUIT;
        }
    }
    return VideoRenderEvent::NONE;
}

bool VideoRenderNode::ensureTexture(int width, int height) {
    if (texture_ && texture_width_ == width && texture_height_ == height) {
        return true;
    }

    if (texture_) {
        SDL_DestroyTexture(static_cast<SDL_Texture*>(texture_));
        texture_ = nullptr;
        texture_width_ = 0;
        texture_height_ = 0;
    }

    if (!SDL_SetWindowSize(static_cast<SDL_Window*>(window_), width, height)) {
        return failRender(std::string("VideoRenderNode: SDL_SetWindowSize failed: ") +
                          SDL_GetError());
    }

    texture_ = SDL_CreateTexture(
        static_cast<SDL_Renderer*>(renderer_),
        SDL_PIXELFORMAT_IYUV,
        SDL_TEXTUREACCESS_STREAMING,
        width, height);
    if (!texture_) {
        return failRender(std::string("VideoRenderNode: SDL_CreateTexture failed: ") +
                          SDL_GetError());
    }

    texture_width_ = width;
    texture_height_ = height;
    return true;
}

void VideoRenderNode::releasePendingFrame(const Buffer* expected) {
    std::lock_guard lock(mailbox_mutex_);
    if (pending_frame_.get() == expected) {
        pending_frame_ = BufferRef{};
        mailbox_cv_.notify_all();
    }
}

// ===================================================================
// presentOnMainThread: 按 PTS 调度并呈现一帧
//
// 第一阶段只诚实支持紧密排列的 YUV420P。其他格式留待正式 swscale
// 路径实现，不再把未知格式永久猜成 YUV420P。
// ===================================================================
VideoPresentResult VideoRenderNode::presentOnMainThread() {
    if (!ensureMainThread("presentOnMainThread")) {
        return VideoPresentResult::ERROR;
    }
    if (!window_ || !renderer_) {
        failRender("VideoRenderNode: renderer is not open");
        return VideoPresentResult::ERROR;
    }

    BufferRef frame;
    {
        std::lock_guard lock(mailbox_mutex_);
        if (mailbox_cancelled_ || !pending_frame_) {
            return VideoPresentResult::NO_FRAME;
        }
        frame = pending_frame_;
    }

    const Buffer* buf = frame.get();
    const auto* meta = std::get_if<VideoRawMeta>(&buf->meta);
    if (!meta) {
        failRender("VideoRenderNode: received buffer without VideoRawMeta");
        releasePendingFrame(buf);
        return VideoPresentResult::ERROR;
    }
    if (meta->width <= 0 || meta->height <= 0) {
        failRender("VideoRenderNode: invalid video frame dimensions");
        releasePendingFrame(buf);
        return VideoPresentResult::ERROR;
    }
    if (meta->pix_fmt != AV_PIX_FMT_YUV420P && meta->pix_fmt != AV_PIX_FMT_YUVJ420P) {
        failRender("VideoRenderNode: only YUV420P is supported before swscale is implemented");
        releasePendingFrame(buf);
        return VideoPresentResult::ERROR;
    }

    if (buf->pts != AV_NOPTS_VALUE) {
        auto now = std::chrono::steady_clock::now();
        if (!timing_started_) {
            timing_started_ = true;
            first_pts_us_ = buf->pts;
            timing_start_ = now;
        }

        int64_t relative_pts_us = buf->pts - first_pts_us_;
        int64_t elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            now - timing_start_).count();
        if (relative_pts_us > elapsed_us) {
            return VideoPresentResult::WAITING;
        }
    }

    const size_t y_size = static_cast<size_t>(meta->width) * meta->height;
    const int chroma_width = (meta->width + 1) / 2;
    const int chroma_height = (meta->height + 1) / 2;
    const size_t chroma_size = static_cast<size_t>(chroma_width) * chroma_height;
    const size_t required_size = y_size + 2 * chroma_size;
    if (buf->size < required_size) {
        failRender("VideoRenderNode: YUV420P frame buffer is truncated");
        releasePendingFrame(buf);
        return VideoPresentResult::ERROR;
    }

    if (!ensureTexture(meta->width, meta->height)) {
        releasePendingFrame(buf);
        return VideoPresentResult::ERROR;
    }

    const uint8_t* y_plane = buf->data;
    const uint8_t* u_plane = y_plane + y_size;
    const uint8_t* v_plane = u_plane + chroma_size;

    if (!SDL_UpdateYUVTexture(
            static_cast<SDL_Texture*>(texture_), nullptr,
            y_plane, meta->width,
            u_plane, chroma_width,
            v_plane, chroma_width)) {
        failRender(std::string("VideoRenderNode: SDL_UpdateYUVTexture failed: ") +
                   SDL_GetError());
        releasePendingFrame(buf);
        return VideoPresentResult::ERROR;
    }

    if (!SDL_SetRenderDrawColor(static_cast<SDL_Renderer*>(renderer_), 0, 0, 0, 255) ||
        !SDL_RenderClear(static_cast<SDL_Renderer*>(renderer_)) ||
        !SDL_RenderTexture(static_cast<SDL_Renderer*>(renderer_),
                           static_cast<SDL_Texture*>(texture_), nullptr, nullptr) ||
        !SDL_RenderPresent(static_cast<SDL_Renderer*>(renderer_))) {
        failRender(std::string("VideoRenderNode: SDL render failed: ") + SDL_GetError());
        releasePendingFrame(buf);
        return VideoPresentResult::ERROR;
    }

    releasePendingFrame(buf);

    ++rendered_frames_;
    if (rendered_frames_ % 100 == 1) {
        fprintf(stderr, "[%s] rendered %d frames (SDL main thread: %s)\n",
                name_.c_str(), rendered_frames_,
                SDL_IsMainThread() ? "yes" : "no");
    }
    return VideoPresentResult::PRESENTED;
}

// ===================================================================
// cancelMailbox: 允许 player 在 stop() 前唤醒工作线程
// ===================================================================
void VideoRenderNode::cancelMailbox() {
    std::lock_guard lock(mailbox_mutex_);
    mailbox_cancelled_ = true;
    pending_frame_ = BufferRef{};
    mailbox_cv_.notify_all();
}

// ===================================================================
// closeOnMainThread: 销毁全部 SDL 视频资源
// ===================================================================
void VideoRenderNode::closeOnMainThread() {
    if (!ensureMainThread("closeOnMainThread")) {
        return;
    }

    if (texture_) {
        SDL_DestroyTexture(static_cast<SDL_Texture*>(texture_));
        texture_ = nullptr;
    }
    texture_width_ = 0;
    texture_height_ = 0;

    if (renderer_) {
        SDL_DestroyRenderer(static_cast<SDL_Renderer*>(renderer_));
        renderer_ = nullptr;
    }
    if (window_) {
        SDL_DestroyWindow(static_cast<SDL_Window*>(window_));
        window_ = nullptr;
    }
    if (sdl_video_initialized_) {
        SDL_Quit();
        sdl_video_initialized_ = false;
    }
}

// ===================================================================
// onStop: 只取消 mailbox；SDL 销毁必须由 main 线程显式完成
// ===================================================================
void VideoRenderNode::onStop() {
    cancelMailbox();
}

} // namespace pipeline
