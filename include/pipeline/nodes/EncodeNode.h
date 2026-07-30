#pragma once

#include "pipeline/core/BaseNode.h"

#include <string>
#include <vector>

struct AVCodec;
struct AVCodecContext;
struct AVFrame;
struct AVPacket;
struct SwsContext;

namespace pipeline {

// ===================================================================
// EncodeConfig: 本次编码实例的静态配置
//
// codec_name / framerate 在构造时固定，不属于运行期 RAW Caps。width、height 和输入
// pix_fmt 则必须从 Running 输入 Caps 获得，因而 AVCodecContext 只能在 onCaps() 打开。
// ===================================================================
struct EncodeConfig {
    std::string codec_name;
    AVRational framerate = {0, 1};
};

// ===================================================================
// EncodeNode: Running VIDEO_RAW Caps 驱动的 FFmpeg 视频编码 Transform
//
// 第一个 Packet 到达后才有机会确认真实的 encoded configuration。节点强制 FFmpeg
// GLOBAL_HEADER：首个 Packet 前必须取得稳定 extradata，随后严格发布 Caps → Packet；
// 无法在首包前确定 configuration 的 encoder 配置被拒绝，避免无界暂存实时数据。
// ===================================================================
class EncodeNode final : public TransformNode {
public:
    EncodeNode(const std::string& name, EncodeConfig config);

protected:
    bool onReady() override;
    void onStop() override;
    bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override;
    void process(const Buffer* input, std::vector<QueueItem>& outputs) override;
    void onEOS(std::vector<QueueItem>& outputs) override;

private:
    bool configureEncoder(const CapsEvent& caps);
    bool chooseOutputPixelFormat(AVPixelFormat input_format, AVPixelFormat* output_format) const;
    bool configureConversion(const CapsEvent& caps, AVPixelFormat output_format);
    void releaseEncoder();
    void releaseConversion();

    bool buildEncoderFrame(const Buffer* input, AVFrame** frame) const;
    bool sendFrameAndDrain(AVFrame* frame, std::vector<QueueItem>& outputs);
    bool drainEncoder(std::vector<QueueItem>& outputs);
    bool appendEncodedPacket(const AVPacket* packet, std::vector<QueueItem>& outputs);
    bool appendOutputCapsBeforePackets(std::vector<QueueItem>& outputs);
    bool isOutputConfigurationReady(const AVPacket* packet);

    const EncodeConfig config_;
    const AVCodec* codec_ = nullptr;
    AVCodecContext* ctx_ = nullptr;
    SwsContext* sws_ctx_ = nullptr;
    CapsEvent input_caps_;
    AVPixelFormat encoder_pix_fmt_ = AV_PIX_FMT_NONE;
    uint64_t input_frames_ = 0;
    uint64_t output_packets_ = 0;
    bool flushed_ = false;
    bool output_caps_emitted_ = false;
};

} // namespace pipeline
