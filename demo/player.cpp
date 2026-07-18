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
        fprintf(stderr, "Usage: %s <media_file>\n", argv[0]);
        return 1;
    }

    // 信号处理属于应用层；handler 只设置 async-signal-safe 标志。
    std::signal(SIGINT, sigintHandler);

    pipeline::Pipeline pipeline;

    auto* demux   = pipeline.addNode<pipeline::AVDemuxNode>("demux", argv[1]);
    auto* vdecode = pipeline.addNode<pipeline::DecodeNode>("vdecode");
    auto* vrender = pipeline.addNode<pipeline::VideoRenderNode>("vrender");

    if (!pipeline.link(demux, "video_0", vdecode, "in",
                       pipeline::MediaType::VIDEO_ENCODED)) {
        fprintf(stderr, "link(demux video_0 -> vdecode) failed\n");
        return 1;
    }
    if (!pipeline.link(vdecode, "out_0", vrender, "in",
                       pipeline::MediaType::VIDEO_RAW)) {
        fprintf(stderr, "link(vdecode out_0 -> vrender) failed\n");
        return 1;
    }

    if (!pipeline.build()) {
        fprintf(stderr, "build failed\n");
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

    bool user_requested_stop = false;
    while (!playback_done.load()) {
        if (g_interrupted != 0) {
            fprintf(stderr, "interrupted\n");
            user_requested_stop = true;
            pipeline.stop();
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (eos_thread.joinable()) {
        eos_thread.join();
    }

    std::string pipeline_error = pipeline.lastError();
    if (!pipeline_error.empty()) {
        fprintf(stderr, "playback failed: %s\n", pipeline_error.c_str());
        return 1;
    }

    if (!user_requested_stop) {
        fprintf(stderr, "playback finished: rendered %d frames\n",
                vrender->renderedFrames());
    }
    return 0;
}
