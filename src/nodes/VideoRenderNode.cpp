#include "pipeline/nodes/VideoRenderNode.h"
#include "pipeline/core/Buffer.h"

extern "C" {
#include <SDL3/SDL.h>
#include <libavutil/pixfmt.h>
}

#include <algorithm>
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
// onStreamInfo: Ready 阶段只接收并记录初始 Caps
//
// SDL 视频资源具有线程亲和性，统一延迟到节点工作线程的 runLoop() 创建
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
// runLoop: SDL 视频资源的完整生命周期都属于节点工作线程
// ===================================================================
void VideoRenderNode::runLoop() {
    if (!openRenderer()) {
        closeRenderer();
        return;
    }

    SinkNode::runLoop();
    closeRenderer();
}

bool VideoRenderNode::failRender(const std::string& message) {
    postMessage(MessageType::ERROR, message);
    return false;
}

bool VideoRenderNode::pollWindowCloseRequested() {
    SDL_Event event;
    const SDL_WindowID window_id =
        SDL_GetWindowID(static_cast<SDL_Window*>(window_));

    while (SDL_PollEvent(&event)) {
        if (event.type == SDL_EVENT_WINDOW_CLOSE_REQUESTED &&
            event.window.windowID == window_id) {
            stop_requested_.store(true);
            return true;
        }
    }
    return false;
}

bool VideoRenderNode::openRenderer() {
    timing_started_ = false;
    rendered_frames_ = 0;

    if (!SDL_InitSubSystem(SDL_INIT_VIDEO)) {
        return failRender(std::string("VideoRenderNode: SDL_InitSubSystem failed: ") +
                          SDL_GetError());
    }
    sdl_video_initialized_ = true;

    // 仅记录 SDL 对当前工作线程的判定，不把它作为运行前置条件
    fprintf(stderr, "[%s] SDL main thread according to SDL: %s\n",
            name_.c_str(), SDL_IsMainThread() ? "yes" : "no");

    const int window_width = width_ > 0 ? width_ : 640;
    const int window_height = height_ > 0 ? height_ : 360;
    window_ = SDL_CreateWindow("Media Pipeline", window_width, window_height, 0);
    if (!window_) {
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
        return failRender(std::string("VideoRenderNode: SDL_CreateRenderer failed: ") +
                          SDL_GetError());
    }

    fprintf(stderr, "[%s] renderer: %s\n", name_.c_str(),
            SDL_GetRendererName(static_cast<SDL_Renderer*>(renderer_)));
    return true;
}

void VideoRenderNode::closeRenderer() {
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
        SDL_QuitSubSystem(SDL_INIT_VIDEO);
        sdl_video_initialized_ = false;
    }
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

bool VideoRenderNode::waitForPresentationTime(int64_t pts_us) {
    if (pts_us == AV_NOPTS_VALUE) {
        return true;
    }

    if (!timing_started_) {
        timing_started_ = true;
        first_pts_us_ = pts_us;
        timing_start_ = std::chrono::steady_clock::now();
        return true;
    }

    const int64_t relative_pts_us = pts_us - first_pts_us_;
    while (!stop_requested_.load()) {
        const auto now = std::chrono::steady_clock::now();
        const int64_t elapsed_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                now - timing_start_).count();
        const int64_t remaining_us = relative_pts_us - elapsed_us;
        if (remaining_us <= 0) {
            return true;
        }

        const int64_t sleep_us = std::min<int64_t>(remaining_us, 1000);
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
    }

    return false;
}

// ===================================================================
// consume: 工作线程等待目标 PTS、处理自身窗口关闭请求后直接呈现当前帧
//
// 当前只支持紧密排列的 YUV420P。其他格式留待正式 swscale 路径实现
// SDL 事件队列由本节点工作线程消费；当前只处理自身窗口关闭请求。
// ===================================================================
void VideoRenderNode::consume(const Buffer* buf) {
    if (pollWindowCloseRequested()) {
        postMessage(MessageType::STOP_REQUESTED,
                    "VideoRenderNode: window close requested");
        return;
    }

    if (!buf || !buf->data || buf->size == 0) {
        failRender("VideoRenderNode: received an empty video buffer");
        return;
    }

    const auto* meta = std::get_if<VideoRawMeta>(&buf->meta);
    if (!meta) {
        failRender("VideoRenderNode: received buffer without VideoRawMeta");
        return;
    }
    if (meta->width <= 0 || meta->height <= 0) {
        failRender("VideoRenderNode: invalid video frame dimensions");
        return;
    }
    if (meta->pix_fmt != AV_PIX_FMT_YUV420P && meta->pix_fmt != AV_PIX_FMT_YUVJ420P) {
        failRender("VideoRenderNode: only YUV420P is supported before swscale is implemented");
        return;
    }

    const size_t y_size = static_cast<size_t>(meta->width) * meta->height;
    const int chroma_width = (meta->width + 1) / 2;
    const int chroma_height = (meta->height + 1) / 2;
    const size_t chroma_size = static_cast<size_t>(chroma_width) * chroma_height;
    const size_t required_size = y_size + 2 * chroma_size;
    if (buf->size < required_size) {
        failRender("VideoRenderNode: YUV420P frame buffer is truncated");
        return;
    }

    if (!waitForPresentationTime(buf->pts)) {
        return;
    }
    if (!ensureTexture(meta->width, meta->height)) {
        return;
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
        return;
    }

    if (!SDL_SetRenderDrawColor(static_cast<SDL_Renderer*>(renderer_), 0, 0, 0, 255) ||
        !SDL_RenderClear(static_cast<SDL_Renderer*>(renderer_)) ||
        !SDL_RenderTexture(static_cast<SDL_Renderer*>(renderer_),
                           static_cast<SDL_Texture*>(texture_), nullptr, nullptr) ||
        !SDL_RenderPresent(static_cast<SDL_Renderer*>(renderer_))) {
        failRender(std::string("VideoRenderNode: SDL render failed: ") + SDL_GetError());
        return;
    }

    ++rendered_frames_;
    if (rendered_frames_ % 100 == 1) {
        fprintf(stderr, "[%s] rendered %d frames\n",
                name_.c_str(), rendered_frames_);
    }
}

// SDL 视频资源已在工作线程退出前释放；join 后不再跨线程操作它们
void VideoRenderNode::onStop() {
}

} // namespace pipeline
