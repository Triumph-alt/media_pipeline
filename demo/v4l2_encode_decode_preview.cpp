#include "pipeline/core/Pipeline.h"
#include "pipeline/nodes/DecodeNode.h"
#include "pipeline/nodes/EncodeNode.h"
#include "pipeline/nodes/V4L2CaptureNode.h"
#include "pipeline/nodes/VideoRenderNode.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <string>
#include <thread>

namespace {

volatile std::sig_atomic_t g_interrupted = 0;

void sigintHandler(int) {
    g_interrupted = 1;
}

bool parsePositiveUint32(const char* text, uint32_t* value) {
    if (!text || !value || *text == '\0') {
        return false;
    }

    char* end = nullptr;
    const unsigned long parsed = std::strtoul(text, &end, 10);
    if (*end != '\0' || parsed == 0 || parsed > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *value = static_cast<uint32_t>(parsed);
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc > 6) {
        fprintf(stderr,
                "Usage: %s [device] [width] [height] [encoder] [framerate]\n",
                argv[0]);
        return 1;
    }

    pipeline::V4L2CaptureConfig capture_config;
    std::string encoder_name = "libx264";
    uint32_t framerate = 30;
    if (argc >= 2) {
        capture_config.device = argv[1];
    }
    if (argc >= 3 && !parsePositiveUint32(argv[2], &capture_config.width)) {
        fprintf(stderr, "invalid capture width: %s\n", argv[2]);
        return 1;
    }
    if (argc >= 4 && !parsePositiveUint32(argv[3], &capture_config.height)) {
        fprintf(stderr, "invalid capture height: %s\n", argv[3]);
        return 1;
    }
    if (argc >= 5) {
        encoder_name = argv[4];
    }
    if (argc >= 6 && !parsePositiveUint32(argv[5], &framerate)) {
        fprintf(stderr, "invalid capture/encoder framerate: %s\n", argv[5]);
        return 1;
    }
    capture_config.framerate = AVRational{static_cast<int>(framerate), 1};

    pipeline::EncodeConfig encode_config;
    encode_config.codec_name = encoder_name;
    encode_config.framerate = AVRational{static_cast<int>(framerate), 1};

    // 信号处理属于应用层；handler 只设置标志，主线程负责请求完整 Pipeline stop。
    std::signal(SIGINT, sigintHandler);

    pipeline::Pipeline pipeline;
    auto* capture = pipeline.addNode<pipeline::V4L2CaptureNode>("vcap", capture_config);
    auto* encode = pipeline.addNode<pipeline::EncodeNode>("vencode", encode_config);
    auto* decode = pipeline.addNode<pipeline::DecodeNode>("vdecode");
    auto* render = pipeline.addNode<pipeline::VideoRenderNode>("vrender");

    if (!capture || !encode || !decode || !render ||
        !pipeline.link(capture, "out_0", encode, "in", pipeline::MediaType::VIDEO_RAW) ||
        !pipeline.link(encode, "out_0", decode, "in", pipeline::MediaType::VIDEO_ENCODED) ||
        !pipeline.link(decode, "out_0", render, "in", pipeline::MediaType::VIDEO_RAW) ||
        !pipeline.build()) {
        fprintf(stderr, "V4L2 encode/decode preview pipeline setup failed\n");
        return 1;
    }
    if (!pipeline.play()) {
        fprintf(stderr, "V4L2 encode/decode preview start failed: %s\n",
                pipeline.lastError().c_str());
        return 1;
    }

    fprintf(stderr,
            "previewing %s through encoder '%s' at requested %ux%u/%u fps; "
            "close the preview window or press Ctrl+C to stop\n",
            capture_config.device.c_str(), encoder_name.c_str(),
            capture_config.width, capture_config.height, framerate);

    std::atomic<bool> pipeline_done{false};
    std::thread wait_thread([&pipeline, &pipeline_done]() {
        // 实时采集没有自然 EOS；窗口关闭、节点 ERROR 或外部 stop 结束这条链路。
        pipeline.waitEOS();
        pipeline_done.store(true);
    });

    bool interrupted = false;
    while (!pipeline_done.load()) {
        if (g_interrupted != 0) {
            fprintf(stderr, "encode/decode preview interrupted\n");
            interrupted = true;
            pipeline.stop();
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (wait_thread.joinable()) {
        wait_thread.join();
    }

    const std::string error = pipeline.lastError();
    if (!error.empty()) {
        fprintf(stderr, "V4L2 encode/decode preview failed: %s\n", error.c_str());
        return 1;
    }

    if (!interrupted) {
        fprintf(stderr,
                "V4L2 encode/decode preview stopped: rendered %d frames, dropped %d late frames\n",
                render->renderedFrames(), render->droppedFrames());
    }
    return 0;
}
