#include "pipeline/core/Pipeline.h"
#include "pipeline/nodes/AVDemuxNode.h"
#include "pipeline/nodes/AVMuxNode.h"
#include "pipeline/nodes/DecodeNode.h"
#include "pipeline/nodes/EncodeNode.h"
#include "pipeline/nodes/TcpSinkNode.h"

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
    if (argc < 4 || argc > 6) {
        fprintf(stderr,
                "Usage: %s <input> <host> <port> [encoder] [framerate]\n"
                "Receiver example:\n"
                "  ffmpeg -y -i tcp://<host>:<port>?listen -c copy /tmp/out.ts\n",
                argv[0]);
        return 1;
    }

    pipeline::TcpSinkConfig tcp_config;
    tcp_config.host = argv[2];
    if (!parsePort(argv[3], &tcp_config.port)) {
        fprintf(stderr, "invalid TCP port: %s\n", argv[3]);
        return 1;
    }

    std::string encoder_name = "libx264";
    uint32_t framerate = 30;
    if (argc >= 5) {
        encoder_name = argv[4];
    }
    if (argc >= 6 && !parsePositiveUint32(argv[5], &framerate)) {
        fprintf(stderr, "invalid encoder framerate: %s\n", argv[5]);
        return 1;
    }

    pipeline::EncodeConfig encode_config;
    encode_config.codec_name = encoder_name;
    encode_config.framerate = AVRational{static_cast<int>(framerate), 1};

    pipeline::Pipeline pipeline;
    auto* demux = pipeline.addNode<pipeline::AVDemuxNode>("demux", argv[1]);
    auto* decode = pipeline.addNode<pipeline::DecodeNode>("decode");
    auto* encode = pipeline.addNode<pipeline::EncodeNode>("encode", encode_config);
    auto* mux = pipeline.addNode<pipeline::AVMuxNode>("ts_mux", pipeline::MuxFormat::MPEGTS);
    auto* tcp = pipeline.addNode<pipeline::TcpSinkNode>("tcp", tcp_config);

    // 首版 MPEG-TS 推流验收只接视频重编码；音频旁路留给后续多路 TS 验收
    if (!demux || !decode || !encode || !mux || !tcp ||
        !pipeline.link(demux, "video_0", decode, "in", pipeline::MediaType::VIDEO_ENCODED) ||
        !pipeline.link(decode, "out_0", encode, "in", pipeline::MediaType::VIDEO_RAW) ||
        !pipeline.link(encode, "out_0", mux, "video_in", pipeline::MediaType::VIDEO_ENCODED) ||
        !pipeline.link(mux, "out_0", tcp, "in", pipeline::MediaType::CONTAINER) ||
        !pipeline.build()) {
        fprintf(stderr, "MPEG-TS TCP transcode pipeline setup failed\n");
        return 1;
    }
    if (!pipeline.play()) {
        fprintf(stderr, "MPEG-TS TCP transcode start failed: %s\n",
                pipeline.lastError().c_str());
        return 1;
    }

    fprintf(stderr, "pushing re-encoded video from %s as MPEG-TS to tcp://%s:%u\n",
            argv[1], tcp_config.host.c_str(), static_cast<unsigned>(tcp_config.port));

    pipeline.waitEOS();
    const std::string error = pipeline.lastError();
    if (!error.empty()) {
        fprintf(stderr, "MPEG-TS TCP transcode failed: %s\n", error.c_str());
        return 1;
    }

    fprintf(stderr, "MPEG-TS TCP transcode completed with natural EOS\n");
    return 0;
}
