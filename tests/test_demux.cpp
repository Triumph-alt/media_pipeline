#include "pipeline/core/Pipeline.h"
#include "pipeline/core/INode.h"
#include "pipeline/core/Event.h"
#include "pipeline/nodes/DemuxNode.h"

#include <cassert>
#include <cstdio>
#include <chrono>

using namespace pipeline;

// ===== 测试用 SinkNode：打印收到的 packet 信息 =====

class PrintSinkNode : public SinkNode {
public:
    PrintSinkNode(const std::string& name) : SinkNode(name) {}

    void onProbe() override {
        createSinkPad("in");
    }

    void onReady() override {}

    void onNull() override {}

protected:
    void consume(std::shared_ptr<Buffer> buffer) override {
        m_count++;
        printf("[sink] stream=%d  pts=%ld  dts=%ld  size=%zu  key=%d\n",
               buffer->streamIndex,
               buffer->pts,
               buffer->dts,
               buffer->size,
               buffer->isKeyFrame());
    }

    void handleEOS() override {
        printf("[sink] EOS reached, total packets: %d\n", m_count);
    }

private:
    int m_count = 0;
};

int main() {
    printf("=== DemuxNode Test ===\n\n");

    Pipeline pipeline("test-demux");

    pipeline.setMessageCallback([](const Message& msg) {
        const char* typeStr = "UNKNOWN";
        switch (msg.type) {
        case Message::Type::ERROR:         typeStr = "ERROR"; break;
        case Message::Type::WARNING:       typeStr = "WARNING"; break;
        case Message::Type::STATE_CHANGED: typeStr = "STATE"; break;
        case Message::Type::EOS:           typeStr = "EOS"; break;
        default: break;
        }
        printf("[Bus] %s: %s\n", typeStr, msg.text.c_str());
    });

    // 创建 DemuxNode，读取测试文件
    auto* demux = pipeline.addNode<DemuxNode>("demux");
    demux->setParam("url", std::string("/home/thomasweide/视频/test.mp4"));

    // 创建两个 Sink 分别接收音频和视频
    auto* videoSink = pipeline.addNode<PrintSinkNode>("vsink");
    auto* audioSink = pipeline.addNode<PrintSinkNode>("asink");

    // 连接: demux video_0 → vsink, demux audio_0 → asink
    demux->link(videoSink, "video_0", "in");
    demux->link(audioSink, "audio_0", "in");

    printf("--- play ---\n");
    pipeline.play();

    printf("--- waiting for stop ---\n");
    pipeline.waitForStop();

    printf("\n--- done ---\n");
    return 0;
}
