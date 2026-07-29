#include "pipeline/nodes/V4L2CaptureNode.h"

#include "pipeline/core/Buffer.h"

extern "C" {
#include <libavutil/error.h>
#include <libavutil/imgutils.h>
}

#include <fcntl.h>
#include <linux/videodev2.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>

namespace pipeline {
namespace {

constexpr uint32_t kDefaultBufferCount = 4;
constexpr int kDefaultPollTimeoutMs = 50;

std::string systemError(const char* operation) {
    return std::string(operation) + ": " + std::strerror(errno);
}

std::string fourccToString(uint32_t fourcc) {
    char name[5] = {
        static_cast<char>(fourcc & 0xff),
        static_cast<char>((fourcc >> 8) & 0xff),
        static_cast<char>((fourcc >> 16) & 0xff),
        static_cast<char>((fourcc >> 24) & 0xff),
        '\0'
    };
    return name;
}

AVPixelFormat toAvPixelFormat(uint32_t v4l2_format) {
    switch (v4l2_format) {
        case V4L2_PIX_FMT_YUYV:
            return AV_PIX_FMT_YUYV422;
        case V4L2_PIX_FMT_UYVY:
            return AV_PIX_FMT_UYVY422;
        case V4L2_PIX_FMT_NV12:
            return AV_PIX_FMT_NV12;
        case V4L2_PIX_FMT_YUV420:
            return AV_PIX_FMT_YUV420P;
        case V4L2_PIX_FMT_RGB24:
            return AV_PIX_FMT_RGB24;
        case V4L2_PIX_FMT_BGR24:
            return AV_PIX_FMT_BGR24;
        default:
            return AV_PIX_FMT_NONE;
    }
}

bool isSupportedV4L2Format(uint32_t v4l2_format) {
    return toAvPixelFormat(v4l2_format) != AV_PIX_FMT_NONE;
}

} // namespace

V4L2CaptureNode::V4L2CaptureNode(const std::string& name, V4L2CaptureConfig config)
    : SourceNode(name), config_(std::move(config)) {
    // V4L2Capture 固定只生产 VIDEO_RAW；后续 SrcPad 只能作为这条逻辑视频流的同源分叉
    addSrcPad("out_0", TemplateCaps{{MediaType::VIDEO_RAW}});
}

bool V4L2CaptureNode::onReady() {
    initial_caps_emitted_ = false;
    return openAndConfigureDevice();
}

bool V4L2CaptureNode::openAndConfigureDevice() {
    if (config_.device.empty()) {
        postMessage(MessageType::ERROR, "V4L2CaptureNode: device path is empty");
        return false;
    }
    if (config_.width == 0 || config_.height == 0) {
        postMessage(MessageType::ERROR, "V4L2CaptureNode: requested width and height must be positive");
        return false;
    }
    if (config_.framerate.num <= 0 || config_.framerate.den <= 0) {
        postMessage(MessageType::ERROR, "V4L2CaptureNode: requested framerate must be positive");
        return false;
    }

    // 非阻塞 fd 配合有限超时 poll，使 worker 即使无帧也能周期性观察 Pipeline stop
    fd_ = open(config_.device.c_str(), O_RDWR | O_NONBLOCK | O_CLOEXEC);
    if (fd_ < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: open '" + config_.device + "' failed: " +
                    std::strerror(errno));
        return false;
    }

    v4l2_capability capability{};
    if (ioctl(fd_, VIDIOC_QUERYCAP, &capability) < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: VIDIOC_QUERYCAP failed: " + systemError("ioctl"));
        return false;
    }
    if (!(capability.capabilities & V4L2_CAP_VIDEO_CAPTURE) ||
        !(capability.capabilities & V4L2_CAP_STREAMING)) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: device must support single-plane VIDEO_CAPTURE and STREAMING");
        return false;
    }

    if (!selectAndSetFormat() || !negotiateFrameRate() ||
        !requestAndMapBuffers() || !startStreaming()) {
        return false;
    }

    fprintf(stderr,
            "[%s] V4L2 ready: %ux%u %s bytesperline=%u sizeimage=%u "
            "framerate=%d/%d buffers=%zu\n",
            name_.c_str(), config_.width, config_.height,
            fourccToString(v4l2_pixel_format_).c_str(), bytes_per_line_, image_size_,
            negotiated_framerate_.num, negotiated_framerate_.den, mapped_buffers_.size());
    return true;
}

bool V4L2CaptureNode::selectAndSetFormat() {
    uint32_t requested_format = config_.pixel_format;
    if (requested_format == 0) {
        // 没有明确指定时只从本节点能忠实转换为 VIDEO_RAW Caps 的格式里选择
        // 拒绝把 MJPEG 等压缩 payload 伪装为 RAW 视频数据
        for (uint32_t index = 0; ; ++index) {
            v4l2_fmtdesc description{};
            description.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
            description.index = index;
            if (ioctl(fd_, VIDIOC_ENUM_FMT, &description) < 0) {
                if (errno == EINVAL) {
                    break;
                }
                postMessage(MessageType::ERROR,
                            "V4L2CaptureNode: VIDIOC_ENUM_FMT failed: " + systemError("ioctl"));
                return false;
            }
            if (isSupportedV4L2Format(description.pixelformat)) {
                requested_format = description.pixelformat;
                break;
            }
        }
        if (requested_format == 0) {
            postMessage(MessageType::ERROR,
                        "V4L2CaptureNode: device exposes no supported CPU-accessible raw format");
            return false;
        }
    }

    if (!isSupportedV4L2Format(requested_format)) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: requested pixel format '" + fourccToString(requested_format) +
                    "' is not supported by this node");
        return false;
    }

    v4l2_format format{};
    format.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    format.fmt.pix.width = config_.width;
    format.fmt.pix.height = config_.height;
    format.fmt.pix.pixelformat = requested_format;
    format.fmt.pix.field = V4L2_FIELD_NONE;
    if (ioctl(fd_, VIDIOC_S_FMT, &format) < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: VIDIOC_S_FMT failed: " + systemError("ioctl"));
        return false;
    }

    if (format.type != V4L2_BUF_TYPE_VIDEO_CAPTURE ||
        !isSupportedV4L2Format(format.fmt.pix.pixelformat) ||
        format.fmt.pix.width == 0 || format.fmt.pix.height == 0 ||
        format.fmt.pix.bytesperline == 0 || format.fmt.pix.sizeimage == 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: device returned an unsupported or incomplete capture format");
        return false;
    }

    if (format.fmt.pix.width > static_cast<uint32_t>(std::numeric_limits<int>::max()) ||
        format.fmt.pix.height > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: negotiated dimensions exceed framework integer limits");
        return false;
    }

    const AVPixelFormat av_format = toAvPixelFormat(format.fmt.pix.pixelformat);
    const int tight_size = av_image_get_buffer_size(
        av_format, static_cast<int>(format.fmt.pix.width), static_cast<int>(format.fmt.pix.height), 1);
    if (tight_size <= 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: negotiated format has no valid tight FFmpeg image layout");
        return false;
    }

    config_.width = format.fmt.pix.width;
    config_.height = format.fmt.pix.height;
    v4l2_pixel_format_ = format.fmt.pix.pixelformat;
    bytes_per_line_ = format.fmt.pix.bytesperline;
    image_size_ = format.fmt.pix.sizeimage;
    av_pixel_format_ = av_format;
    return true;
}

bool V4L2CaptureNode::negotiateFrameRate() {
    v4l2_streamparm parameters{};
    parameters.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    if (ioctl(fd_, VIDIOC_G_PARM, &parameters) < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: VIDIOC_G_PARM failed: " + systemError("ioctl"));
        return false;
    }

    // timeperframe 不受支持时，驱动没有可供协商的 nominal 帧率；继续采集但保留未知结果，
    // 后续 Buffer.duration 为 0，真实呈现节奏仍只由有效设备 PTS 决定。
    if (!(parameters.parm.capture.capability & V4L2_CAP_TIMEPERFRAME)) {
        negotiated_framerate_ = AVRational{0, 1};
        nominal_frame_duration_us_ = 0;
        fprintf(stderr, "[%s] V4L2 device does not support TIMEPERFRAME negotiation\n",
                name_.c_str());
        return true;
    }

    // V4L2 使用 time-per-frame，而配置使用 frames-per-second，故分子分母需要互换。
    parameters.parm.capture.timeperframe.numerator =
        static_cast<uint32_t>(config_.framerate.den);
    parameters.parm.capture.timeperframe.denominator =
        static_cast<uint32_t>(config_.framerate.num);
    if (ioctl(fd_, VIDIOC_S_PARM, &parameters) < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: VIDIOC_S_PARM failed: " + systemError("ioctl"));
        return false;
    }

    // 不信任 S_PARM 入参或返回缓存，重新 G_PARM 取得驱动最终接受的 nominal timeperframe。
    parameters = {};
    parameters.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    if (ioctl(fd_, VIDIOC_G_PARM, &parameters) < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: final VIDIOC_G_PARM failed: " + systemError("ioctl"));
        return false;
    }

    const v4l2_fract accepted = parameters.parm.capture.timeperframe;
    if (accepted.numerator == 0 || accepted.denominator == 0 ||
        accepted.numerator > static_cast<uint32_t>(std::numeric_limits<int>::max()) ||
        accepted.denominator > static_cast<uint32_t>(std::numeric_limits<int>::max())) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: driver returned an invalid accepted timeperframe");
        return false;
    }

    negotiated_framerate_ = AVRational{
        static_cast<int>(accepted.denominator),
        static_cast<int>(accepted.numerator)
    };
    nominal_frame_duration_us_ = av_rescale_q(
        1,
        AVRational{static_cast<int>(accepted.numerator),
                   static_cast<int>(accepted.denominator)},
        AVRational{1, 1000000});
    if (nominal_frame_duration_us_ <= 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: accepted timeperframe cannot be represented in microseconds");
        return false;
    }

    fprintf(stderr,
            "[%s] V4L2 framerate: requested=%d/%d accepted=%d/%d "
            "timeperframe=%u/%u\n",
            name_.c_str(), config_.framerate.num, config_.framerate.den,
            negotiated_framerate_.num, negotiated_framerate_.den,
            accepted.numerator, accepted.denominator);
    return true;
}

bool V4L2CaptureNode::requestAndMapBuffers() {
    v4l2_requestbuffers request{};
    request.count = config_.buffer_count == 0 ? kDefaultBufferCount : config_.buffer_count;
    request.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    request.memory = V4L2_MEMORY_MMAP;
    if (ioctl(fd_, VIDIOC_REQBUFS, &request) < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: VIDIOC_REQBUFS failed: " + systemError("ioctl"));
        return false;
    }
    if (request.count < 2) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: driver allocated fewer than two mmap capture buffers");
        return false;
    }

    mapped_buffers_.assign(request.count, MappedBuffer{});
    for (uint32_t index = 0; index < request.count; ++index) {
        v4l2_buffer buffer{};
        buffer.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
        buffer.memory = V4L2_MEMORY_MMAP;
        buffer.index = index;
        if (ioctl(fd_, VIDIOC_QUERYBUF, &buffer) < 0) {
            postMessage(MessageType::ERROR,
                        "V4L2CaptureNode: VIDIOC_QUERYBUF failed: " + systemError("ioctl"));
            return false;
        }

        void* data = mmap(nullptr, buffer.length, PROT_READ | PROT_WRITE, MAP_SHARED,
                          fd_, static_cast<off_t>(buffer.m.offset));
        if (data == MAP_FAILED) {
            postMessage(MessageType::ERROR,
                        "V4L2CaptureNode: mmap capture buffer failed: " + systemError("mmap"));
            return false;
        }
        mapped_buffers_[index] = MappedBuffer{data, buffer.length};

        if (!queueDriverBuffer(index)) {
            return false;
        }
    }
    return true;
}

bool V4L2CaptureNode::queueDriverBuffer(uint32_t index) {
    if (index >= mapped_buffers_.size()) {
        postMessage(MessageType::ERROR, "V4L2CaptureNode: driver buffer index is out of range");
        return false;
    }

    v4l2_buffer buffer{};
    buffer.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    buffer.memory = V4L2_MEMORY_MMAP;
    buffer.index = index;
    if (ioctl(fd_, VIDIOC_QBUF, &buffer) < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: VIDIOC_QBUF failed: " + systemError("ioctl"));
        return false;
    }
    return true;
}

bool V4L2CaptureNode::startStreaming() {
    v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    if (ioctl(fd_, VIDIOC_STREAMON, &type) < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: VIDIOC_STREAMON failed: " + systemError("ioctl"));
        return false;
    }
    streaming_ = true;
    return true;
}

void V4L2CaptureNode::produce(std::vector<QueueItem>& outputs) {
    if (fd_ < 0 || !streaming_) {
        postMessage(MessageType::ERROR, "V4L2CaptureNode: produce called without an active capture stream");
        return;
    }

    pollfd descriptor{};
    descriptor.fd = fd_;
    descriptor.events = POLLIN | POLLPRI;
    const int timeout = config_.poll_timeout_ms > 0 ? config_.poll_timeout_ms : kDefaultPollTimeoutMs;
    const int result = poll(&descriptor, 1, timeout);
    if (result == 0 || (result < 0 && errno == EINTR)) {
        // 有限超时或信号中断都不是设备 EOF；返回给 SourceNode 后由下一轮检查 stop 标志。
        return;
    }
    if (result < 0) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: poll failed: " + systemError("poll"));
        return;
    }
    if (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        postMessage(MessageType::ERROR, "V4L2CaptureNode: capture device poll reported an error");
        return;
    }
    if (!(descriptor.revents & (POLLIN | POLLPRI))) {
        return;
    }

    dequeueAndCopy(outputs);
}

bool V4L2CaptureNode::dequeueAndCopy(std::vector<QueueItem>& outputs) {
    v4l2_buffer buffer{};
    buffer.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    buffer.memory = V4L2_MEMORY_MMAP;
    if (ioctl(fd_, VIDIOC_DQBUF, &buffer) < 0) {
        if (errno == EAGAIN) {
            return true;
        }
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: VIDIOC_DQBUF failed: " + systemError("ioctl"));
        return false;
    }
    if (buffer.index >= mapped_buffers_.size()) {
        postMessage(MessageType::ERROR, "V4L2CaptureNode: driver returned an invalid dequeued buffer index");
        return false;
    }

    // V4L2_BUF_FLAG_ERROR 只表示当前 buffer 内容可能损坏，属于可恢复的单帧错误；丢弃当前
    // 帧并重新入队，不能把一次坏帧扩大成实时设备和整个 Pipeline 的致命 ERROR。
    if (buffer.flags & V4L2_BUF_FLAG_ERROR) {
        return queueDriverBuffer(buffer.index);
    }
    if (image_size_ > mapped_buffers_[buffer.index].length) {
        const bool requeued = queueDriverBuffer(buffer.index);
        if (!requeued) {
            return false;
        }
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: mapped driver buffer is shorter than negotiated sizeimage");
        return false;
    }

    // DQBUF 后的 mmap buffer 必须在本轮离开前归还驱动。无论框架深拷贝成功与否都尝试 QBUF，
    // 从而保证 Route 背压或单帧处理错误不会耗尽有限的驱动 capture buffer。
    const bool copied = copyDequeuedBuffer(buffer.index, buffer.timestamp, buffer.flags, outputs);
    const bool requeued = queueDriverBuffer(buffer.index);
    return copied && requeued;
}

bool V4L2CaptureNode::appendInitialCaps(std::vector<QueueItem>& outputs) {
    if (initial_caps_emitted_) {
        return true;
    }

    CapsEvent caps;
    caps.media_type = MediaType::VIDEO_RAW;
    caps.width = static_cast<int>(config_.width);
    caps.height = static_cast<int>(config_.height);
    caps.pix_fmt = av_pixel_format_;
    outputs.emplace_back(Event{caps});
    initial_caps_emitted_ = true;
    return true;
}

bool V4L2CaptureNode::copyDequeuedBuffer(uint32_t index, const timeval& timestamp,
                                          uint32_t flags, std::vector<QueueItem>& outputs) {
    if (index >= mapped_buffers_.size() || !mapped_buffers_[index].data) {
        postMessage(MessageType::ERROR, "V4L2CaptureNode: dequeued buffer has no mmap storage");
        return false;
    }

    const int tight_size = av_image_get_buffer_size(
        av_pixel_format_, static_cast<int>(config_.width), static_cast<int>(config_.height), 1);
    if (tight_size <= 0) {
        postMessage(MessageType::ERROR, "V4L2CaptureNode: cannot calculate tight frame size");
        return false;
    }

    const uint8_t* source_base = static_cast<const uint8_t*>(mapped_buffers_[index].data);
    const uint64_t luma_bytes = static_cast<uint64_t>(bytes_per_line_) * config_.height;
    if (luma_bytes > mapped_buffers_[index].length) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: mapped buffer is shorter than negotiated image rows");
        return false;
    }

    // V4L2 单平面 mmap payload 可能包含每行 padding，不能直接用紧密 av_image_fill_arrays()
    // 计算 chroma 起点，否则 NV12/YUV420P 的第二平面会错误落在未跳过 luma padding 的位置
    const uint8_t* source_data[4]{};
    int source_linesize[4]{};
    source_data[0] = source_base;
    source_linesize[0] = static_cast<int>(bytes_per_line_);
    uint64_t required_payload = luma_bytes;

    if (av_pixel_format_ == AV_PIX_FMT_NV12) {
        // NV12 的交错 UV plane 与 luma 有相同的字节行跨度、高度减半
        const uint64_t chroma_bytes = static_cast<uint64_t>(bytes_per_line_) * (config_.height / 2);
        required_payload += chroma_bytes;
        source_data[1] = source_base + luma_bytes;
        source_linesize[1] = static_cast<int>(bytes_per_line_);
    } else if (av_pixel_format_ == AV_PIX_FMT_YUV420P) {
        // YUV420P 的 U/V 每行各占一半字节；luma padding 仍须完整跨过后才到 U plane
        if ((bytes_per_line_ & 1U) != 0U) {
            postMessage(MessageType::ERROR,
                        "V4L2CaptureNode: YUV420P requires an even negotiated bytesperline");
            return false;
        }
        const uint32_t chroma_stride = bytes_per_line_ / 2;
        const uint32_t chroma_height = config_.height / 2;
        const uint64_t chroma_bytes = static_cast<uint64_t>(chroma_stride) * chroma_height;
        required_payload += chroma_bytes * 2;
        source_data[1] = source_base + luma_bytes;
        source_data[2] = source_data[1] + chroma_bytes;
        source_linesize[1] = static_cast<int>(chroma_stride);
        source_linesize[2] = static_cast<int>(chroma_stride);
    }

    if (required_payload > mapped_buffers_[index].length) {
        postMessage(MessageType::ERROR,
                    "V4L2CaptureNode: mapped buffer is shorter than negotiated pixel layout");
        return false;
    }

    auto* raw = new Buffer();
    raw->data = new uint8_t[static_cast<size_t>(tight_size)];
    raw->size = static_cast<size_t>(tight_size);
    raw->media_type = MediaType::VIDEO_RAW;
    raw->meta = VideoRawMeta{};
    // duration 只承载驱动最终接受的 nominal timeperframe；实际采集时刻仍以每帧有效 PTS 为准。
    raw->duration = nominal_frame_duration_us_;
    const uint32_t timestamp_type = flags & V4L2_BUF_FLAG_TIMESTAMP_MASK;
    // 只有明确来自 CLOCK_MONOTONIC 的设备时间戳才能进入框架 PTS；UNKNOWN 的时钟域未定，
    // COPY 则继承外部来源的时钟域，二者都保留 AV_NOPTS_VALUE，不能与 steady_clock 混用。
    if (timestamp_type == V4L2_BUF_FLAG_TIMESTAMP_MONOTONIC &&
        timestamp.tv_sec >= 0 && timestamp.tv_usec >= 0 && timestamp.tv_usec < 1000000) {
        const uint64_t seconds = static_cast<uint64_t>(timestamp.tv_sec);
        const uint64_t microseconds = static_cast<uint64_t>(timestamp.tv_usec);
        if (seconds <= (static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - microseconds) /
                           1000000ULL) {
            raw->pts = static_cast<int64_t>(seconds * 1000000ULL + microseconds);
        }
    }

    uint8_t* destination_data[4]{};
    int destination_linesize[4]{};
    if (av_image_fill_arrays(destination_data, destination_linesize, raw->data,
                             av_pixel_format_, static_cast<int>(config_.width),
                             static_cast<int>(config_.height), 1) < 0) {
        BufferRef(raw);
        postMessage(MessageType::ERROR, "V4L2CaptureNode: cannot describe framework frame layout");
        return false;
    }
    av_image_copy(destination_data, destination_linesize,
                  const_cast<const uint8_t* const*>(source_data), source_linesize,
                  av_pixel_format_, static_cast<int>(config_.width), static_cast<int>(config_.height));

    if (!appendInitialCaps(outputs)) {
        BufferRef(raw);
        return false;
    }
    outputs.emplace_back(BufferRef(raw));
    return true;
}

void V4L2CaptureNode::onStop() {
    closeDevice();
}

void V4L2CaptureNode::closeDevice() {
    if (fd_ >= 0 && streaming_) {
        v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
        if (ioctl(fd_, VIDIOC_STREAMOFF, &type) < 0 && errno != ENODEV) {
            fprintf(stderr, "[%s] V4L2 VIDIOC_STREAMOFF failed during cleanup: %s\n",
                    name_.c_str(), std::strerror(errno));
        }
        streaming_ = false;
    }

    for (auto& mapped : mapped_buffers_) {
        if (mapped.data && mapped.length > 0) {
            munmap(mapped.data, mapped.length);
        }
        mapped = {};
    }
    mapped_buffers_.clear();

    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

} // namespace pipeline
