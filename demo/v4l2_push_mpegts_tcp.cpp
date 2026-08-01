#include "pipeline/core/Pipeline.h"
#include "pipeline/nodes/AVMuxNode.h"
#include "pipeline/nodes/EncodeNode.h"
#include "pipeline/nodes/TcpSinkNode.h"
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

bool parsePort(const char* text, uint16_t* port) {
    if (!text || !port || *text == '\0') {
        return false;
    }
    char* end = nullptr;
    const unsigned long parsed = std::strtoul(text, &end, 10);
    if (*end != '\0' || parsed == 0 || parsed > 65535UL) {
        return false;
    }
    *port = static_cast<uint16_t>(parsed);
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 4 || argc > 8) {
        fprintf(stderr,
                "Usage: %s <device> <host> <port> [width] [height] [encoder] "
                "[framerate]\n"
                "Receiver example:\n"
                "  ffplay -x 640 -y 480 -fflags nobuffer -flags low_delay "
                "tcp://<host>:<port>?listen\n",
                argv[0]);
        return 1;
    }

    pipeline::V4L2CaptureConfig capture_config;
    capture_config.device = argv[1];

    pipeline::TcpSinkConfig tcp_config;
    tcp_config.host = argv[2];
    if (!parsePort(argv[3], &tcp_config.port)) {
        fprintf(stderr, "invalid TCP port: %s\n", argv[3]);
        return 1;
    }

    std::string encoder_name = "libx264";
    uint32_t framerate = 30;

    if (argc >= 5 && !parsePositiveUint32(argv[4], &capture_config.width)) {
        fprintf(stderr, "invalid capture width: %s\n", argv[4]);
        return 1;
    }
    if (argc >= 6 && !parsePositiveUint32(argv[5], &capture_config.height)) {
        fprintf(stderr, "invalid capture height: %s\n", argv[5]);
        return 1;
    }
    if (argc >= 7) {
        encoder_name = argv[6];
    }
    if (argc >= 8 && !parsePositiveUint32(argv[7], &framerate)) {
        fprintf(stderr, "invalid capture/encoder framerate: %s\n", argv[7]);
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
    auto* tcp = pipeline.addNode<pipeline::TcpSinkNode>("tcp", tcp_config);

    if (!capture || !encode || !mux || !tcp ||
        !pipeline.link(capture, "out_0", encode, "in", pipeline::MediaType::VIDEO_RAW) ||
        !pipeline.link(encode, "out_0", mux, "video_in", pipeline::MediaType::VIDEO_ENCODED) ||
        !pipeline.link(mux, "out_0", tcp, "in", pipeline::MediaType::CONTAINER) ||
        !pipeline.build()) {
        fprintf(stderr, "V4L2 MPEG-TS TCP push pipeline setup failed\n");
        return 1;
    }
    if (!pipeline.play()) {
        fprintf(stderr, "V4L2 MPEG-TS TCP push start failed: %s\n",
                pipeline.lastError().c_str());
        return 1;
    }

    fprintf(stderr,
            "pushing %s as MPEG-TS over tcp://%s:%u through '%s'; "
            "press Ctrl+C to stop\n",
            capture_config.device.c_str(), tcp_config.host.c_str(),
            static_cast<unsigned>(tcp_config.port), encoder_name.c_str());

    std::atomic<bool> pipeline_done{false};
    std::thread wait_thread([&pipeline, &pipeline_done]() {
        pipeline.waitEOS();
        pipeline_done.store(true);
    });

    while (!pipeline_done.load()) {
        if (g_interrupted != 0) {
            fprintf(stderr,
                    "push interrupted; realtime Source stop does not emit EOS or Mux trailer\n");
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
        fprintf(stderr, "V4L2 MPEG-TS TCP push failed: %s\n", error.c_str());
        return 1;
    }
    return 0;
}
