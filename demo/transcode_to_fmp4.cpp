#include "pipeline/core/Pipeline.h"
#include "pipeline/nodes/AVDemuxNode.h"
#include "pipeline/nodes/AVMuxNode.h"
#include "pipeline/nodes/DecodeNode.h"
#include "pipeline/nodes/EncodeNode.h"
#include "pipeline/nodes/FileSinkNode.h"

#include <cstdio>
#include <cstdlib>
#include <limits>
#include <string>

namespace {

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
    if (argc < 3 || argc > 6) {
        fprintf(stderr,
                "Usage: %s <input> <output.mp4> [encoder] [framerate] [--overwrite]\n",
                argv[0]);
        return 1;
    }

    pipeline::FileSinkConfig file_config;
    file_config.path = argv[2];
    std::string encoder_name = "libx264";
    uint32_t framerate = 30;

    int value_argc = argc;
    if (argc > 3 && std::string(argv[argc - 1]) == "--overwrite") {
        file_config.overwrite = true;
        --value_argc;
    }
    if (value_argc > 5) {
        fprintf(stderr, "too many positional arguments\n");
        return 1;
    }
    if (value_argc >= 4) {
        encoder_name = argv[3];
    }
    if (value_argc >= 5 && !parsePositiveUint32(argv[4], &framerate)) {
        fprintf(stderr, "invalid encoder framerate: %s\n", argv[4]);
        return 1;
    }

    // 与 transcode_to_flv 同拓扑：视频 Decode→重编码，输入 AAC encoded 旁路进同一 fMP4 Mux
    pipeline::EncodeConfig encode_config;
    encode_config.codec_name = encoder_name;
    encode_config.framerate = AVRational{static_cast<int>(framerate), 1};

    pipeline::Pipeline pipeline;
    auto* demux = pipeline.addNode<pipeline::AVDemuxNode>("demux", argv[1]);
    auto* decode = pipeline.addNode<pipeline::DecodeNode>("decode");
    auto* encode = pipeline.addNode<pipeline::EncodeNode>("encode", encode_config);
    auto* mux = pipeline.addNode<pipeline::AVMuxNode>("fmp4_mux", pipeline::MuxFormat::MP4);
    auto* file = pipeline.addNode<pipeline::FileSinkNode>("file", file_config);

    if (!demux || !decode || !encode || !mux || !file ||
        !pipeline.link(demux, "video_0", decode, "in", pipeline::MediaType::VIDEO_ENCODED) ||
        !pipeline.link(demux, "audio_0", mux, "audio_in", pipeline::MediaType::AUDIO_ENCODED) ||
        !pipeline.link(decode, "out_0", encode, "in", pipeline::MediaType::VIDEO_RAW) ||
        !pipeline.link(encode, "out_0", mux, "video_in", pipeline::MediaType::VIDEO_ENCODED) ||
        !pipeline.link(mux, "out_0", file, "in", pipeline::MediaType::CONTAINER) ||
        !pipeline.build()) {
        fprintf(stderr, "fMP4 transcode pipeline setup failed\n");
        return 1;
    }
    if (!pipeline.play()) {
        fprintf(stderr, "fMP4 transcode start failed: %s\n", pipeline.lastError().c_str());
        return 1;
    }

    pipeline.waitEOS();
    const std::string error = pipeline.lastError();
    if (!error.empty()) {
        fprintf(stderr, "fMP4 transcode failed: %s\n", error.c_str());
        return 1;
    }

    fprintf(stderr, "fMP4 transcode completed with natural EOS: %s\n",
            file_config.path.c_str());
    return 0;
}
