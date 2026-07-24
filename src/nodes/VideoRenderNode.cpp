#include "pipeline/nodes/VideoRenderNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Clock.h"
#include "pipeline/core/Pipeline.h"

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
// onCaps: 在 Running Route 中应用完整视频格式边界
// ===================================================================
bool VideoRenderNode::onCaps(const std::string&, const CapsEvent& caps,
                             std::vector<QueueItem>*) {
    if (caps.media_type != MediaType::VIDEO_RAW ||
        caps.width <= 0 || caps.height <= 0 ||
        (caps.pix_fmt != AV_PIX_FMT_YUV420P && caps.pix_fmt != AV_PIX_FMT_YUVJ420P)) {
        return failRender("VideoRenderNode: only complete YUV420P/YUVJ420P Caps are supported before swscale");
    }

    // Runtime Caps may arrive before this worker has created SDL resources. Record the complete format now;
    // consume() will create/recreate the Texture after openRenderer succeeds. If a later Caps arrives while
    // rendering, destroy the old Texture here so no stale dimensions survive the configuration boundary.
    if (texture_ && (texture_width_ != caps.width || texture_height_ != caps.height)) {
        SDL_DestroyTexture(static_cast<SDL_Texture*>(texture_));
        texture_ = nullptr;
        texture_width_ = 0;
        texture_height_ = 0;
    }
    width_ = caps.width;
    height_ = caps.height;
    fprintf(stderr, "[%s] applied video caps: %dx%d pix_fmt=%d\n",
            name_.c_str(), width_, height_, caps.pix_fmt);
    return true;
}

// ===================================================================
// runLoop: SDL 视频资源的完整生命周期都属于节点工作线程
// ===================================================================
void VideoRenderNode::runLoop() {
    if (openRenderer()) {
        SinkNode::runLoop();
    }

    // 无论初始化失败、自然 EOS、ERROR、窗口关闭还是主动 stop，
    // 都从这个统一尾部释放线程亲和资源，再清理 std::thread 上的 SDL TLS。
    closeRenderer();
    SDL_CleanupTLS();
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
            return true;
        }
    }
    return false;
}

bool VideoRenderNode::openRenderer() {
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

// ===================================================================
// waitForPresentationTime: 以主时钟为参照控制呈现节奏
//
// 主时钟语义（§9）：已锚定时返回"当前估计已到达可听输出端的媒体时间位置"
// 有音频由 AudioPlayNode 用真实消费进度驱动；无音频由本节点首帧一次性锚定
//   - 未锚定：立即呈现不同步；无音频时本帧锚定墙钟回退，有音频则等音频锚定
//   - 已锚定：超前则取消感知地等到呈现时刻；落后则立即呈现追帧
//   - 丢帧策略本轮不实现，留到接音频接线阶段单独定阈值
// ===================================================================
bool VideoRenderNode::waitForPresentationTime(int64_t pts_us) {
    // 视频帧没有 PTS，无法与音频比较，立即呈现
    if (pts_us == AV_NOPTS_VALUE) {
        return true;
    }

    // 查询当前主时钟，有音频时 pos 就是 AudioPlayNode 估算的当前音频播放位置
    Clock* clock = pipeline_->clock();
    int64_t pos = clock->getPositionUs();

    // 如果 Clock 还没锚定
    if (pos == Clock::kUnanchored) {
        if (!clock->hasAudio()) {
            // 纯视频，没有音频提供主时钟，使用首个视频帧的 PTS 锚定，视频按墙钟节奏播放
            clock->anchorOnce(pts_us);
        }
        // 有音频，但音频尚未锚定，立即显示
        return true;
    }

    // 计算视频帧领先还是落后
    int64_t remaining_us = pts_us - pos;

    // 只要视频仍然超前，就继续等待
    while (remaining_us > 0 && !stop_requested_.load()) {
        // 每次最多睡 1ms
        const int64_t sleep_us = std::min<int64_t>(remaining_us, 1000);
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));

        // 每次醒来重新读取 Audio Clock
        pos = clock->getPositionUs();
        if (pos == Clock::kUnanchored) {
            // 防御：时钟被重置，直接呈现
            return true;
        }
        remaining_us = pts_us - pos;
    }

    return !stop_requested_.load();
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

    const CapsEvent& caps = active_caps_.at("in");
    if (buf->media_type != MediaType::VIDEO_RAW ||
        !std::holds_alternative<VideoRawMeta>(buf->meta)) {
        failRender("VideoRenderNode: received Buffer that does not match VIDEO_RAW active Caps");
        return;
    }

    // VideoRaw Buffer 当前约定为由 Buffer::fromAVFrame 生成的紧密连续 YUV420P payload；
    // width/height/pix_fmt 只从最近成功应用的 active Caps 读取，不能再双重相信 BufferMeta。
    const int width = caps.width;
    const int height = caps.height;
    const size_t y_size = static_cast<size_t>(width) * height;
    const int chroma_width = (width + 1) / 2;
    const int chroma_height = (height + 1) / 2;
    const size_t chroma_size = static_cast<size_t>(chroma_width) * chroma_height;
    const size_t required_size = y_size + 2 * chroma_size;
    if (buf->size < required_size) {
        failRender("VideoRenderNode: YUV420P frame buffer is truncated");
        return;
    }

    if (!waitForPresentationTime(buf->pts)) {
        return;
    }
    if (!ensureTexture(width, height)) {
        return;
    }

    const uint8_t* y_plane = buf->data;
    const uint8_t* u_plane = y_plane + y_size;
    const uint8_t* v_plane = u_plane + chroma_size;

    if (!SDL_UpdateYUVTexture(
            static_cast<SDL_Texture*>(texture_), nullptr,
            y_plane, width,
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
