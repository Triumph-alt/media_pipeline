#include "pipeline/core/Pipeline.h"
#include "pipeline/nodes/AVDemuxNode.h"
#include "pipeline/nodes/DecodeNode.h"
#include "pipeline/nodes/VideoRenderNode.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <thread>

namespace {

volatile std::sig_atomic_t g_interrupted = 0;

void sigintHandler(int) {
    g_interrupted = 1;
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <video_file>\n", argv[0]);
        return 1;
    }

    std::signal(SIGINT, sigintHandler);

    pipeline::Pipeline pipeline;
    auto* demux = pipeline.addNode<pipeline::AVDemuxNode>("demux", argv[1]);
    auto* decode = pipeline.addNode<pipeline::DecodeNode>("vdecode");
    auto* render = pipeline.addNode<pipeline::VideoRenderNode>("vrender");

    if (!pipeline.link(demux, "video_0", decode, "in", pipeline::MediaType::VIDEO_ENCODED) ||
        !pipeline.link(decode, "out_0", render, "in", pipeline::MediaType::VIDEO_RAW) ||
        !pipeline.build()) {
        fprintf(stderr, "video-only pipeline setup failed\n");
        return 1;
    }
    if (!pipeline.play()) {
        fprintf(stderr, "play failed: %s\n", pipeline.lastError().c_str());
        return 1;
    }

    std::atomic<bool> playback_done{false};
    std::thread eos_thread([&pipeline, &playback_done]() {
        pipeline.waitEOS();
        playback_done.store(true);
    });

    while (!playback_done.load()) {
        if (g_interrupted != 0) {
            fprintf(stderr, "interrupted\n");
            pipeline.stop();
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (eos_thread.joinable()) {
        eos_thread.join();
    }

    const std::string error = pipeline.lastError();
    if (!error.empty()) {
        fprintf(stderr, "playback failed: %s\n", error.c_str());
        return 1;
    }

    fprintf(stderr, "video-only playback finished: rendered %d frames, dropped %d late frames\n",
            render->renderedFrames(), render->droppedFrames());
    return 0;
}
