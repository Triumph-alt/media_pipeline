#include "pipeline/core/Pipeline.h"
#include "pipeline/core/INode.h"

#include <cassert>
#include <cstdio>
#include <thread>
#include <chrono>

using namespace pipeline;

// ===== 测试用 SourceNode：生成 5 帧数据后 EOF =====

class TestSourceNode : public SourceNode {
public:
    TestSourceNode(const std::string& name) : SourceNode(name) {}

    void onProbe() override {
        createSrcPad("out");
        printf("[%s] probe: created src pad\n", m_name.c_str());
    }

    void onReady() override {
        printf("[%s] ready\n", m_name.c_str());
    }

    void onNull() override {
        printf("[%s] null: resources released\n", m_name.c_str());
    }

protected:
    std::shared_ptr<Buffer> generateData() override {
        if (m_frameCount >= 5) return nullptr;

        auto buf = Buffer::fromRawData(&m_frameCount, sizeof(int),
                                        m_pipeline->memoryPool());
        buf->pts = m_frameCount * 33333;  // 30fps, ~33ms per frame
        buf->streamIndex = 0;
        m_frameCount++;

        printf("[%s] generate frame %d\n", m_name.c_str(), m_frameCount);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return buf;
    }

    bool isEOF() override { return m_frameCount >= 5; }

private:
    int m_frameCount = 0;
};

// ===== 测试用 SinkNode：打印收到的帧 =====

class TestSinkNode : public SinkNode {
public:
    TestSinkNode(const std::string& name) : SinkNode(name) {}

    void onProbe() override {
        createSinkPad("in");
        printf("[%s] probe: created sink pad\n", m_name.c_str());
    }

    void onReady() override {
        printf("[%s] ready\n", m_name.c_str());
    }

    void onNull() override {
        printf("[%s] null: resources released\n", m_name.c_str());
    }

protected:
    void consume(std::shared_ptr<Buffer> buffer) override {
        int frame = *static_cast<const int*>(buffer->data->data());
        printf("[%s] consume frame %d (pts=%ld)\n",
               m_name.c_str(), frame, buffer->pts);
    }

    void handleEOS() override {
        printf("[%s] EOS reached\n", m_name.c_str());
    }
};

int main() {
    printf("=== Pipeline Integration Test ===\n\n");

    // 1. 创建 Pipeline
    Pipeline pipeline("test");

    // 2. 设置消息回调
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

    // 3. 添加节点
    auto* src  = pipeline.addNode<TestSourceNode>("source");
    auto* sink = pipeline.addNode<TestSinkNode>("sink");

    // 4. 连接
    src->link(sink, "out", "in");

    // 5. 启动
    printf("\n--- play ---\n");
    pipeline.play();

    // 6. 等待停止
    printf("\n--- waiting for stop ---\n");
    pipeline.waitForStop();

    printf("\n--- done ---\n");
    return 0;
}
