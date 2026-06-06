// FFmpegDemux.cpp
#include "FFmpegDemux.h"

#include <cassert>
#include <cstring>

extern "C" {
#include <libavformat/avformat.h>
#include <libavutil/mathematics.h>
}

namespace pipeline {

static int64_t to_microseconds(int64_t ts, AVRational time_base) {
    if (ts == AV_NOPTS_VALUE) {
        return -1;
    }

    return av_rescale_q(ts, time_base, AVRational{1, 1000000});
}

FFmpegDemux::FFmpegDemux(const std::string& uri)
    : uri_(uri)
{
    assert(!uri_.empty());
}

FFmpegDemux::~FFmpegDemux() = default;

bool FFmpegDemux::fill_buffer(Buffer* buf, AVPacket* pkt,
                            int64_t time_base_num,
                            int64_t time_base_den) {
    if (static_cast<size_t>(pkt->size) > buf->size) {
        return false;
    }

    std::memcpy(buf->cpu_ptr, pkt->data, pkt->size);
    buf->size = static_cast<size_t>(pkt->size);

    AVRational tb{static_cast<int>(time_base_num), static_cast<int>(time_base_den)};
    buf->pts = to_microseconds(pkt->pts, tb);
    buf->dts = to_microseconds(pkt->dts, tb);

    return true;
}

void FFmpegDemux::run() {
    assert(pool_ && "pool_ is null");
    // index 0 = 视频输出，index 1 = 音频输出
    assert(!output_queues_.empty() && output_queues_[0] && "video output queue is null");

    // 打开媒体源
    AVFormatContext* fmt_ctx = nullptr;
    int ret = avformat_open_input(&fmt_ctx, uri_.c_str(), nullptr, nullptr);
    if (ret < 0) {
        // TODO: 替换为项目日志系统
        return;
    }

    ret = avformat_find_stream_info(fmt_ctx, nullptr);
    if (ret < 0) {
        avformat_close_input(&fmt_ctx);
        return;
    }

    // 找音视频流 index
    int video_idx = -1;
    int audio_idx = -1;
    for (unsigned i = 0; i < fmt_ctx->nb_streams; ++i) {
        AVMediaType type = fmt_ctx->streams[i]->codecpar->codec_type;
        if (video_idx < 0 && type == AVMEDIA_TYPE_VIDEO) {
            video_idx = static_cast<int>(i);
        }
        if (audio_idx < 0 && type == AVMEDIA_TYPE_AUDIO) {
            audio_idx = static_cast<int>(i);
        }
    }

    // 循环读包
    AVPacket* pkt = av_packet_alloc();
    if (!pkt) {
        avformat_close_input(&fmt_ctx);
        return;
    }

    while (!stop_requested_.load(std::memory_order_acquire)) {
        ret = av_read_frame(fmt_ctx, pkt);
        if (ret == AVERROR_EOF) {
            break;
        }
        if (ret < 0) {
            break;
        }

        // 判断是音频包还是视频包
        // index 0 = 视频，index 1 = 音频
        std::shared_ptr<Queue<Buffer*>> target_queue;
        AVRational time_base{};

        if (pkt->stream_index == video_idx) {
            target_queue = output_queues_[0];
            time_base = fmt_ctx->streams[video_idx]->time_base;
        } else if (pkt->stream_index == audio_idx &&
                   output_queues_.size() > 1 &&
                   output_queues_[1]) {
            target_queue = output_queues_[1];
            time_base = fmt_ctx->streams[audio_idx]->time_base;
        } else {
            av_packet_unref(pkt);
            continue;
        }

        // 从池里取一块 Buffer，把 AVPacket 数据拷进去
        Buffer* buf = pool_->acquire();
        if (!fill_buffer(buf, pkt, time_base.num, time_base.den)) {
            // Buffer 太小（不应该发生，块大小由上层配置保证）
            pool_->release(buf);
            av_packet_unref(pkt);
            continue;
        }

        target_queue->push(buf);
        av_packet_unref(pkt);
    }

    av_packet_free(&pkt);
    avformat_close_input(&fmt_ctx);
}

} // namespace pipeline
