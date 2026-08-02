#include "pipeline/nodes/VideoRenderNode.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Clock.h"
#include "pipeline/core/Pipeline.h"

extern "C" {
#include <SDL3/SDL.h>
#include <libavutil/imgutils.h>
#include <libavutil/mem.h>
#include <libavutil/pixfmt.h>
#include <libswscale/swscale.h>
}

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iterator>
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

bool VideoRenderNode::onReady() {
    if (!pipeline_->clock()->registerStartupParticipant()) {
        return failRender("VideoRenderNode: failed to register startup participant");
    }
    startup_barrier_arrived_ = false;
    startup_barrier_withdrawn_ = false;
    return true;
}

// ===================================================================
// onCaps: 在 Running Route 中应用完整视频格式边界
// ===================================================================
bool VideoRenderNode::onCaps(const std::string&, const CapsEvent& caps,
                             std::vector<QueueItem>*) {
    // 验证 VideoRender 最低限度需要的 Caps 字段
    if (caps.media_type != MediaType::VIDEO_RAW ||
        caps.width <= 0 || caps.height <= 0 || caps.pix_fmt == AV_PIX_FMT_NONE) {
        return failRender("VideoRenderNode: VIDEO_RAW Caps require width, height and pix_fmt");
    }

    // 根据真实输入格式准备本地消费路径
    if (!configureConversion(caps)) {
        return false;
    }

    // 若后续 Caps 改变尺寸且已有 Texture，则销毁旧 Texture，由下一帧按新尺寸延迟创建
    if (texture_ && (texture_width_ != caps.width || texture_height_ != caps.height)) {
        SDL_DestroyTexture(static_cast<SDL_Texture*>(texture_));
        texture_ = nullptr;
        texture_width_ = 0;
        texture_height_ = 0;
    }
    width_ = caps.width;
    height_ = caps.height;
    fprintf(stderr, "[%s] applied video caps: %dx%d pix_fmt=%d%s\n",
            name_.c_str(), width_, height_, caps.pix_fmt,
            direct_yuv420p_ ? " (direct IYUV)" : " (swscale to IYUV)");
    return true;
}

bool VideoRenderNode::configureConversion(const CapsEvent& caps) {
    const bool direct_yuv420p = caps.pix_fmt == AV_PIX_FMT_YUV420P ||
                                caps.pix_fmt == AV_PIX_FMT_YUVJ420P;
    if (direct_yuv420p) {
        // 直传格式不保留旧的转换资源，防止格式切回时让已失效的 sws 缓冲跨越 Caps 边界。
        releaseConversion();
        input_pix_fmt_ = caps.pix_fmt;
        direct_yuv420p_ = true;
        return true;
    }

    const int buffer_size = av_image_get_buffer_size(AV_PIX_FMT_YUV420P,
                                                     caps.width, caps.height, 1);
    if (buffer_size <= 0) {
        return failRender("VideoRenderNode: invalid YUV420P conversion buffer size");
    }

    // 输入格式由当前真实 Caps 决定，输出格式固定为 SDL_PIXELFORMAT_IYUV 所需的 YUV420P
    SwsContext* replacement_context = sws_getContext(
        caps.width, caps.height, caps.pix_fmt,
        caps.width, caps.height, AV_PIX_FMT_YUV420P,
        SWS_BILINEAR, nullptr, nullptr, nullptr);
    if (!replacement_context) {
        return failRender("VideoRenderNode: sws_getContext failed for input pix_fmt");
    }

    uint8_t* replacement_buffer = static_cast<uint8_t*>(av_malloc(buffer_size));
    if (!replacement_buffer) {
        sws_freeContext(replacement_context);
        return failRender("VideoRenderNode: YUV420P conversion buffer allocation failed");
    }

    uint8_t* replacement_planes[4]{};
    int replacement_linesize[4]{};
    if (av_image_fill_arrays(replacement_planes, replacement_linesize, replacement_buffer,
                             AV_PIX_FMT_YUV420P, caps.width, caps.height, 1) < 0) {
        av_free(replacement_buffer);
        sws_freeContext(replacement_context);
        return failRender("VideoRenderNode: failed to describe YUV420P conversion buffer");
    }

    // 所有 replacement 都已成功构造后再替换旧资源，失败不会破坏仍有效的上一份 Caps 配置
    releaseConversion();
    sws_ctx_ = replacement_context;
    sws_buffer_ = replacement_buffer;
    sws_buffer_size_ = buffer_size;
    std::copy(std::begin(replacement_linesize), std::end(replacement_linesize),
              std::begin(sws_linesize_));
    input_pix_fmt_ = caps.pix_fmt;
    direct_yuv420p_ = false;
    return true;
}

void VideoRenderNode::releaseConversion() {
    if (sws_ctx_) {
        sws_freeContext(sws_ctx_);
        sws_ctx_ = nullptr;
    }
    av_free(sws_buffer_);
    sws_buffer_ = nullptr;
    sws_buffer_size_ = 0;
    std::fill(std::begin(sws_linesize_), std::end(sws_linesize_), 0);
}

bool VideoRenderNode::convertFrameToYuv420p(const Buffer* buffer, const CapsEvent& caps,
                                             uint8_t* output_data[4], int output_linesize[4]) {
    uint8_t* source_data[4]{};
    int source_linesize[4]{};
    // Buffer::fromAVFrame 使用 align=1 保存紧密连续 payload；按当前 active Caps 重建平面
    // 不依赖 Y/U/V 的手工偏移，NV12、YUV422P、RGB 和高位深格式才能被正确解释
    const int source_size = av_image_fill_arrays(source_data, source_linesize, buffer->data,
                                                 caps.pix_fmt, caps.width, caps.height, 1);
    if (source_size <= 0 || static_cast<size_t>(source_size) != buffer->size) {
        return failRender("VideoRenderNode: video buffer size does not match active Caps pixel layout");
    }

    if (direct_yuv420p_) {
        // YUV420P/YUVJ420P 已是 SDL IYUV 兼容的平面布局，直接沿用紧密 source planes
        std::copy(std::begin(source_data), std::end(source_data), output_data);
        std::copy(std::begin(source_linesize), std::end(source_linesize), output_linesize);
        return true;
    }

    if (!sws_ctx_ || !sws_buffer_ || caps.pix_fmt != input_pix_fmt_) {
        return failRender("VideoRenderNode: missing swscale configuration for active Caps");
    }

    const int output_size = av_image_fill_arrays(
        output_data, output_linesize, sws_buffer_, AV_PIX_FMT_YUV420P,
        caps.width, caps.height, 1);
    if (output_size <= 0 || output_size != sws_buffer_size_) {
        return failRender("VideoRenderNode: YUV420P conversion output layout is invalid");
    }
    if (!std::equal(output_linesize, output_linesize + 4, sws_linesize_)) {
        return failRender("VideoRenderNode: YUV420P conversion output layout changed unexpectedly");
    }

    const uint8_t* source_planes[4] = {
        source_data[0], source_data[1], source_data[2], source_data[3]
    };
    const int converted_height = sws_scale(sws_ctx_, source_planes, source_linesize,
                                           0, caps.height, output_data, output_linesize);
    if (converted_height != caps.height) {
        return failRender("VideoRenderNode: sws_scale did not convert a complete frame");
    }
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

bool VideoRenderNode::queryDisplayUsableSize(int* max_w, int* max_h) const {
    if (!max_w || !max_h) {
        return false;
    }
    *max_w = 0;
    *max_h = 0;

    // 优先主显示器；失败再退到 displays 列表首项
    SDL_DisplayID display = SDL_GetPrimaryDisplay();
    if (display == 0) {
        int count = 0;
        SDL_DisplayID* displays = SDL_GetDisplays(&count);
        if (!displays || count <= 0) {
            if (displays) {
                SDL_free(displays);
            }
            return false;
        }
        display = displays[0];
        SDL_free(displays);
    }

    SDL_Rect bounds{};
    // usable 会去掉任务栏等占位；不可用时退回整屏 bounds
    if (!SDL_GetDisplayUsableBounds(display, &bounds) &&
        !SDL_GetDisplayBounds(display, &bounds)) {
        return false;
    }
    if (bounds.w <= 0 || bounds.h <= 0) {
        return false;
    }
    *max_w = bounds.w;
    *max_h = bounds.h;
    return true;
}

void VideoRenderNode::fitSizeToDisplay(int video_w, int video_h,
                                       int* window_w, int* window_h) const {
    if (!window_w || !window_h || video_w <= 0 || video_h <= 0) {
        if (window_w) {
            *window_w = 0;
        }
        if (window_h) {
            *window_h = 0;
        }
        return;
    }

    // 未取到显示器上限时不限幅，保持与视频同尺寸（旧行为）
    if (display_max_w_ <= 0 || display_max_h_ <= 0 ||
        (video_w <= display_max_w_ && video_h <= display_max_h_)) {
        *window_w = video_w;
        *window_h = video_h;
        return;
    }

    // 整数等比缩小：比较 video_w/max_w 与 video_h/max_h，取更紧的一边
    // video_w * max_h > video_h * max_w  ⇔  宽边先触顶
    if (static_cast<int64_t>(video_w) * display_max_h_ >
        static_cast<int64_t>(video_h) * display_max_w_) {
        *window_w = display_max_w_;
        *window_h = static_cast<int>(
            (static_cast<int64_t>(video_h) * display_max_w_) / video_w);
    } else {
        *window_h = display_max_h_;
        *window_w = static_cast<int>(
            (static_cast<int64_t>(video_w) * display_max_h_) / video_h);
    }
    if (*window_w < 1) {
        *window_w = 1;
    }
    if (*window_h < 1) {
        *window_h = 1;
    }
}

bool VideoRenderNode::openRenderer() {
    rendered_frames_ = 0;
    dropped_frames_ = 0;
    display_max_w_ = 0;
    display_max_h_ = 0;
    window_width_ = 0;
    window_height_ = 0;

    if (!SDL_InitSubSystem(SDL_INIT_VIDEO)) {
        return failRender(std::string("VideoRenderNode: SDL_InitSubSystem failed: ") +
                          SDL_GetError());
    }
    sdl_video_initialized_ = true;

    // 仅记录 SDL 对当前工作线程的判定，不把它作为运行前置条件
    fprintf(stderr, "[%s] SDL main thread according to SDL: %s\n",
            name_.c_str(), SDL_IsMainThread() ? "yes" : "no");

    // VIDEO 初始化后再查显示器；失败只打日志，后续窗口不限幅
    if (queryDisplayUsableSize(&display_max_w_, &display_max_h_)) {
        fprintf(stderr, "[%s] display usable bounds: %dx%d\n",
                name_.c_str(), display_max_w_, display_max_h_);
    } else {
        fprintf(stderr,
                "[%s] display usable bounds unavailable (%s); window will follow video size\n",
                name_.c_str(), SDL_GetError());
        display_max_w_ = 0;
        display_max_h_ = 0;
    }

    // 建窗时尚无 Caps；用占位尺寸，首帧 ensureTexture 再按视频+显示器适配
    const int placeholder_w = 640;
    const int placeholder_h = 360;
    int window_width = 0;
    int window_height = 0;
    fitSizeToDisplay(placeholder_w, placeholder_h, &window_width, &window_height);
    window_ = SDL_CreateWindow("Media Pipeline", window_width, window_height, 0);
    if (!window_) {
        return failRender(std::string("VideoRenderNode: SDL_CreateWindow failed: ") +
                          SDL_GetError());
    }
    window_width_ = window_width;
    window_height_ = window_height;

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
    display_max_w_ = 0;
    display_max_h_ = 0;
    window_width_ = 0;
    window_height_ = 0;

    // 转换资源也只属于 VideoRender worker，须在该线程退出前释放
    releaseConversion();

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
    // Texture 始终是视频原始宽高；窗口尺寸单独按显示器适配，二者解耦
    int fitted_w = 0;
    int fitted_h = 0;
    fitSizeToDisplay(width, height, &fitted_w, &fitted_h);

    const bool texture_ok =
        texture_ && texture_width_ == width && texture_height_ == height;
    const bool window_ok =
        window_width_ == fitted_w && window_height_ == fitted_h;

    if (texture_ok && window_ok) {
        return true;
    }

    if (!window_ok) {
        if (!SDL_SetWindowSize(static_cast<SDL_Window*>(window_), fitted_w, fitted_h)) {
            return failRender(std::string("VideoRenderNode: SDL_SetWindowSize failed: ") +
                              SDL_GetError());
        }
        window_width_ = fitted_w;
        window_height_ = fitted_h;
        fprintf(stderr, "[%s] window fit: video %dx%d -> window %dx%d (display max %dx%d)\n",
                name_.c_str(), width, height, fitted_w, fitted_h,
                display_max_w_, display_max_h_);
    }

    if (texture_ok) {
        return true;
    }

    // 已有 Texture 但视频尺寸变了，销毁后按新尺寸重建
    if (texture_) {
        SDL_DestroyTexture(static_cast<SDL_Texture*>(texture_));
        texture_ = nullptr;
        texture_width_ = 0;
        texture_height_ = 0;
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
// ===================================================================
bool VideoRenderNode::waitForPresentationTime(int64_t pts_us, int64_t duration_us) {
    // 视频帧没有 PTS，无法与主时钟比较，立即呈现。
    if (pts_us == AV_NOPTS_VALUE) {
        return true;
    }

    Clock* clock = pipeline_->clock();
    int64_t pos = clock->getPositionUs();

    if (pos == Clock::kUnanchored) {
        // 纯视频由首帧建立墙钟回退；有音频但尚未锚定时保持现有立即呈现策略。
        if (!clock->hasAudio()) {
            clock->anchorOnce(pts_us);
        }
        return true;
    }

    int64_t remaining_us = pts_us - pos;
    while (remaining_us > 0 && !stop_requested_.load()) {
        const int64_t sleep_us = std::min<int64_t>(remaining_us, 1000);
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));

        pos = clock->getPositionUs();
        if (pos == Clock::kUnanchored) {
            return true;
        }
        remaining_us = pts_us - pos;
    }

    if (stop_requested_.load()) {
        return false;
    }

    // duration 未知时退化为 40ms；低帧率使用更大的帧时长，避免在展示区间仍有效时过早丢帧。
    const int64_t late_threshold_us = std::max<int64_t>(duration_us > 0 ? duration_us : 0, 40000);
    if (remaining_us < -late_threshold_us) {
        ++dropped_frames_;
        return false;
    }

    return true;
}

bool VideoRenderNode::waitForStartupBarrier() {
    Clock* clock = pipeline_->clock();
    while (!stop_requested_.load()) {
        const Clock::StartupBarrierWaitResult result = clock->arriveAndWaitForStartupFor(
            std::chrono::milliseconds(10), startup_barrier_arrived_);
        if (result == Clock::StartupBarrierWaitResult::RELEASED) {
            return true;
        }
        if (result == Clock::StartupBarrierWaitResult::CANCELLED) {
            return false;
        }

        // 栅栏尚未凑齐时仍维持 VideoRender 现有的窗口关闭响应：不能因为等待音频首段
        // PCM 而停止轮询 SDL 事件，否则用户在起播前关闭窗口会失去 STOP_REQUESTED 通道。
        if (pollWindowCloseRequested()) {
            postMessage(MessageType::STOP_REQUESTED,
                        "VideoRenderNode: window close requested");
            return false;
        }
    }
    return false;
}

// ===================================================================
// consume: 工作线程等待目标 PTS、处理自身窗口关闭请求后直接呈现当前帧
//
// active Caps 如实描述 Decode 产出的紧密像素布局；YUV420P/YUVJ420P 直传 SDL，
// 其他格式先经本节点的 swscale 转为 IYUV，再复用同一上传和 Present 路径。
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

    // active Caps 是 Buffer 紧密存储布局的唯一权威；转换 helper 依据它重建 source planes，
    // 并统一给出 SDL IYUV 所需的 Y/U/V 平面与 stride。
    const int width = caps.width;
    const int height = caps.height;
    // 先完成同步判断：已过期帧不值得再执行 swscale，正常晚帧丢弃仍由 SinkNode ack 当前 Delivery。
    if (!waitForPresentationTime(buf->pts, buf->duration)) {
        return;
    }

    uint8_t* yuv420p_data[4]{};
    int yuv420p_linesize[4]{};
    if (!convertFrameToYuv420p(buf, caps, yuv420p_data, yuv420p_linesize)) {
        return;
    }

    if (!ensureTexture(width, height)) {
        return;
    }

    if (!SDL_UpdateYUVTexture(
            static_cast<SDL_Texture*>(texture_), nullptr,
            yuv420p_data[0], yuv420p_linesize[0],
            yuv420p_data[1], yuv420p_linesize[1],
            yuv420p_data[2], yuv420p_linesize[2])) {
        failRender(std::string("VideoRenderNode: SDL_UpdateYUVTexture failed: ") +
                   SDL_GetError());
        return;
    }

    // 首帧已完成内部资源准备和 Texture 上传，但尚未产生可见输出；在首次 Present 前与
    // AudioPlay 的首次 SDL 提交汇合，释放后仍按原有渲染路径执行。
    if (!startup_barrier_arrived_ && !waitForStartupBarrier()) {
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

void VideoRenderNode::onDrain() {
    // 若视频流在首帧可呈现前自然结束，它不再阻塞 AudioPlay 的共同起跑
    if (!startup_barrier_arrived_ && !startup_barrier_withdrawn_) {
        pipeline_->clock()->withdrawStartupParticipant();
        startup_barrier_withdrawn_ = true;
    }
}

// SDL 视频资源已在工作线程退出前释放；join 后不再跨线程操作它们
void VideoRenderNode::onStop() {
}

} // namespace pipeline
