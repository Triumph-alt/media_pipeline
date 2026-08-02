#include "pipeline/core/Pipeline.h"
#include "pipeline/nodes/AVMuxNode.h"
#include "pipeline/nodes/EncodeNode.h"
#include "pipeline/nodes/FileSinkNode.h"
#include "pipeline/nodes/V4L2CaptureNode.h"

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
    if (argc < 3 || argc > 8) {
        fprintf(stderr,
                "Usage: %s <device> <output.ts> [width] [height] [encoder] "
                "[framerate] [--overwrite]\n",
                argv[0]);
        return 1;
    }

    pipeline::V4L2CaptureConfig capture_config;
    capture_config.device = argv[1];
    pipeline::FileSinkConfig file_config;
    file_config.path = argv[2];
    std::string encoder_name = "libx264";
    uint32_t framerate = 30;

    int value_argc = argc;
    if (argc > 3 && std::string(argv[argc - 1]) == "--overwrite") {
        file_config.overwrite = true;
        --value_argc;
    }
    if (value_argc > 7) {
        fprintf(stderr, "too many positional arguments\n");
        return 1;
    }
    if (value_argc >= 4 && !parsePositiveUint32(argv[3], &capture_config.width)) {
        fprintf(stderr, "invalid capture width: %s\n", argv[3]);
        return 1;
    }
    if (value_argc >= 5 && !parsePositiveUint32(argv[4], &capture_config.height)) {
        fprintf(stderr, "invalid capture height: %s\n", argv[4]);
        return 1;
    }
    if (value_argc >= 6) {
        encoder_name = argv[5];
    }
    if (value_argc >= 7 && !parsePositiveUint32(argv[6], &framerate)) {
        fprintf(stderr, "invalid capture/encoder framerate: %s\n", argv[6]);
        return 1;
    }
    capture_config.framerate = AVRational{static_cast<int>(framerate), 1};

    pipeline::EncodeConfig encode_config;
    encode_config.codec_name = encoder_name;
    encode_config.framerate = AVRational{static_cast<int>(framerate), 1};

    std::signal(SIGINT, sigintHandler);

    pipeline::Pipeline pipeline;
    auto* capture = pipeline.addNode<pipeline::V4L2CaptureNode>("vcap", capture_config);
    auto* encode = pipeline.addNode<pipeline::EncodeNode>("vencode", encode_config);
    auto* mux = pipeline.addNode<pipeline::AVMuxNode>("ts_mux", pipeline::MuxFormat::MPEGTS);
    auto* file = pipeline.addNode<pipeline::FileSinkNode>("file", file_config);

    if (!capture || !encode || !mux || !file ||
        !pipeline.link(capture, "out_0", encode, "in", pipeline::MediaType::VIDEO_RAW) ||
        !pipeline.link(encode, "out_0", mux, "video_in", pipeline::MediaType::VIDEO_ENCODED) ||
        !pipeline.link(mux, "out_0", file, "in", pipeline::MediaType::CONTAINER) ||
        !pipeline.build()) {
        fprintf(stderr, "V4L2 MPEG-TS recording pipeline setup failed\n");
        return 1;
    }
    if (!pipeline.play()) {
        fprintf(stderr, "V4L2 MPEG-TS recording start failed: %s\n",
                pipeline.lastError().c_str());
        return 1;
    }

    fprintf(stderr,
            "recording %s to %s as MPEG-TS through '%s'; press Ctrl+C to stop\n",
            capture_config.device.c_str(), file_config.path.c_str(), encoder_name.c_str());

    std::atomic<bool> pipeline_done{false};
    std::thread wait_thread([&pipeline, &pipeline_done]() {
        pipeline.waitEOS();
        pipeline_done.store(true);
    });

    while (!pipeline_done.load()) {
        if (g_interrupted != 0) {
            fprintf(stderr,
                    "recording interrupted; realtime Source stop does not emit EOS or Mux trailer\n");
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
        fprintf(stderr, "V4L2 MPEG-TS recording failed: %s\n", error.c_str());
        return 1;
    }
    return 0;
}
