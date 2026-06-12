#include "pipeline/core/Pipeline.h"
#include "pipeline/core/INode.h"
#include "pipeline/core/Event.h"
#include "pipeline/nodes/DemuxNode.h"
#include "pipeline/nodes/DecodeNode.h"

#include <cassert>
#include <cstdio>
#include <chrono>

using namespace pipeline;

// ===== 测试用 SinkNode：打印收到的数据 =====

class PrintSinkNode : public SinkNode {
public:
    PrintSinkNode(const std::string& name) : SinkNode(name) {}

    void onProbe() override {
        // 不创建 Pad，由 requestSinkPad 按需创建
    }

    void onReady() override {}
    void onNull() override {}

    SinkPad* requestSinkPad(MediaType type) override {
        SinkPad* existing = getSinkPad("in");
        if (existing) return existing;
        return createSinkPad("in");
    }

protected:
    void consume(std::shared_ptr<Buffer> buffer) override {
        m_count++;
        printf("[sink] stream=%d  pts=%ld  size=%zu\n",
               buffer->streamIndex,
               buffer->pts,
               buffer->size);
    }

    void handleEOS() override {
        printf("[sink] EOS reached, total frames: %d\n", m_count);
    }

private:
    int m_count = 0;
};

int main() {
    printf("=== DecodeNode Test ===\n\n");

    Pipeline pipeline("test-decode");

    pipeline.setMessageCallback([](const Message& msg) {
        const char* typeStr = "UNKNOWN";
        switch (msg.type) {
        case Message::Type::ERROR:         typeStr = "ERROR"; break;
        case Message::Type::WARNING:       typeStr = "WARNING"; break;
        case Message::Type::STATE_CHANGED: typeStr = "STATE"; break;
        case Message::Type::EOS:           typeStr = "EOS"; break;
        case Message::Type::STREAM_INFO:   typeStr = "STREAM"; break;
        default: break;
        }
        printf("[Bus] %s: %s\n", typeStr, msg.text.c_str());
    });

    // DemuxNode → DecodeNode(video) → Sink
    //           → DecodeNode(audio) → Sink
    auto* demux  = pipeline.addNode<DemuxNode>("demux");
    demux->setParam("url", std::string("/home/thomasweide/视频/test.mp4"));

    auto* decV   = pipeline.addNode<DecodeNode>("dec-v");
    auto* decA   = pipeline.addNode<DecodeNode>("dec-a");
    auto* sinkV  = pipeline.addNode<PrintSinkNode>("sink-v");
    auto* sinkA  = pipeline.addNode<PrintSinkNode>("sink-a");

    // 新的 link API：只传 MediaType
    demux->link(decV, MediaType::VIDEO);
    demux->link(decA, MediaType::AUDIO);
    decV->link(sinkV, MediaType::VIDEO);
    decA->link(sinkA, MediaType::AUDIO);

    printf("--- play ---\n");
    pipeline.play();

    printf("--- waiting for stop ---\n");
    pipeline.waitForStop();

    printf("\n--- done ---\n");
    return 0;
}
