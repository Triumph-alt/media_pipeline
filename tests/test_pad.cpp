#include "pipeline/core/INode.h"
#include "pipeline/core/Pipeline.h"

#include <cassert>
#include <cstdio>
#include <thread>
#include <chrono>

using namespace pipeline;

// 测试用 SourceNode：生成指定数量的帧
class CountSourceNode : public SourceNode {
public:
    CountSourceNode(const std::string& name, int count)
        : SourceNode(name), m_maxCount(count) {}

    void onProbe() override {}
    void onReady() override {}
    void onNull() override {}

    SrcPad* requestSrcPad(MediaType type) override {
        SrcPad* existing = getSrcPad("out");
        if (existing) return existing;
        return createSrcPad("out");
    }

protected:
    std::shared_ptr<Buffer> generateData() override {
        if (m_count >= m_maxCount) return nullptr;
        auto buf = Buffer::fromRawData(&m_count, sizeof(int),
                                        m_pipeline->memoryPool());
        buf->pts = m_count;
        buf->streamIndex = 0;
        m_count++;
        return buf;
    }
    bool isEOF() override { return m_count >= m_maxCount; }

private:
    int m_count = 0;
    int m_maxCount;
};

// 测试用 SinkNode：记录收到的帧数和最后一个 pts
class RecordSinkNode : public SinkNode {
public:
    RecordSinkNode(const std::string& name) : SinkNode(name) {}

    int receivedCount = 0;
    int64_t lastPts = -1;

    void onProbe() override {}
    void onReady() override {}
    void onNull() override {}

    SinkPad* requestSinkPad(MediaType type) override {
        SinkPad* existing = getSinkPad("in");
        if (existing) return existing;
        return createSinkPad("in");
    }

protected:
    void consume(std::shared_ptr<Buffer> buffer) override {
        receivedCount++;
        lastPts = buffer->pts;
    }
    void handleEOS() override {}
};

// 测试 1：play() 后 Pad 已创建且已连接
void testPadConnectSharesQueue() {
    printf("  testPadConnectSharesQueue...");

    Pipeline p("test");
    auto* src = p.addNode<CountSourceNode>("src", 1);
    auto* sink = p.addNode<RecordSinkNode>("sink");

    src->link(sink, MediaType::VIDEO);
    p.play();
    p.waitForStop();

    // play() 之后 Pad 已经创建并连接
    SrcPad* srcPad = src->getSrcPad("out");
    SinkPad* sinkPad = sink->getSinkPad("in");

    assert(srcPad != nullptr);
    assert(sinkPad != nullptr);
    assert(srcPad->isConnected());
    assert(sinkPad->isConnected());
    assert(srcPad->peer() == sinkPad);
    assert(sinkPad->peer() == srcPad);

    printf(" OK\n");
}

// 测试 2：数据从 Source 流到 Sink（5 帧）
void testPadPushPop() {
    printf("  testPadPushPop...");

    Pipeline p("test");
    auto* src = p.addNode<CountSourceNode>("src", 5);
    auto* sink = p.addNode<RecordSinkNode>("sink");

    src->link(sink, MediaType::VIDEO);
    p.play();
    p.waitForStop();

    assert(sink->receivedCount == 5);
    assert(sink->lastPts == 4);

    printf(" OK\n");
}

// 测试 3：EOS 正确传播——Source EOF 后 Sink 收到 EOS 并退出
void testPadEventPropagation() {
    printf("  testPadEventPropagation...");

    Pipeline p("test");
    auto* src = p.addNode<CountSourceNode>("src", 3);
    auto* sink = p.addNode<RecordSinkNode>("sink");

    src->link(sink, MediaType::VIDEO);
    p.play();
    p.waitForStop();

    // 3 帧全部收到，说明 EOS 正确触发了退出
    assert(sink->receivedCount == 3);

    printf(" OK\n");
}

// 测试 4：多帧数据流通——Source → Sink
void testPipelineDataFlow() {
    printf("  testPipelineDataFlow...");

    Pipeline p("test");
    auto* src = p.addNode<CountSourceNode>("src", 10);
    auto* sink = p.addNode<RecordSinkNode>("sink");

    src->link(sink, MediaType::VIDEO);

    p.play();
    p.waitForStop();

    assert(sink->receivedCount == 10);
    assert(sink->lastPts == 9);  // pts 从 0 开始

    printf(" OK\n");
}

// 测试 5：stop 后节点正确释放资源
void testPadFlush() {
    printf("  testPadFlush...");

    Pipeline p("test");
    auto* src = p.addNode<CountSourceNode>("src", 100);
    auto* sink = p.addNode<RecordSinkNode>("sink");

    src->link(sink, MediaType::VIDEO);
    p.play();

    // 给一点时间让数据流通
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // stop 触发 flush + 资源释放
    p.stop();
    p.waitForStop();

    // stop 后 sink 的 onNull 应该被调用（资源已释放）
    // 如果没有崩溃说明 flush 和清理逻辑正常
    assert(sink->receivedCount > 0);

    printf(" OK\n");
}

int main() {
    printf("=== Pad Tests ===\n");

    testPadConnectSharesQueue();
    testPadPushPop();
    testPadEventPropagation();
    testPipelineDataFlow();
    testPadFlush();

    printf("\nAll tests passed.\n");
    return 0;
}
