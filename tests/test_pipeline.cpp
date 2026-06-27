#include "pipeline/core/Pipeline.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Caps.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <thread>

using namespace pipeline;

// ===================================================================
// Mock 节点
// ===================================================================

class MockSource : public SourceNode {
public:
    MockSource(const std::string& name, int frame_count, MediaType type)
        : SourceNode(name), max_frames_(frame_count), type_(type) {}

    int generated() const { return count_; }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    Buffer* capture() override {
        if (count_++ >= max_frames_) return nullptr;
        auto* buf = new Buffer();
        buf->data = new uint8_t[4]{1, 2, 3, 4};
        buf->size = 4;
        buf->media_type = type_;
        buf->pts = count_ * 33000;
        return buf;
    }

private:
    int max_frames_;
    MediaType type_;
    int count_ = 0;
};

class MockTransform : public TransformNode {
public:
    MockTransform(const std::string& name) : TransformNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW, MediaType::AUDIO_RAW}});
    }
    int processed() const { return count_; }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    void process(Buffer* input, std::vector<Buffer*>& outputs) override {
        count_++;
        auto* out = new Buffer();
        out->data = new uint8_t[input->size];
        memcpy(out->data, input->data, input->size);
        out->size = input->size;
        out->media_type = input->media_type;
        out->pts = input->pts;
        outputs.push_back(out);
    }

private:
    int count_ = 0;
};

class MockSink : public SinkNode {
public:
    MockSink(const std::string& name) : SinkNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW, MediaType::AUDIO_RAW}});
    }
    int received() const { return count_; }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void consume(Buffer* buf) override { count_++; }

private:
    int count_ = 0;
};

class MockSourceWithCaps : public SourceNode {
public:
    MockSourceWithCaps(const std::string& name, int frame_count)
        : SourceNode(name), max_frames_(frame_count) {}

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onStreamInfo() override {
        CapsEvent caps;
        caps.media_type = MediaType::VIDEO_ENCODED;
        caps.codec_id = AV_CODEC_ID_H264;
        caps.width = 1920;
        caps.height = 1080;
        sendCapsEvent("out", caps);
        return true;
    }

    Buffer* capture() override {
        if (count_++ >= max_frames_) return nullptr;
        auto* buf = new Buffer();
        buf->data = new uint8_t[8]{1, 2, 3, 4, 5, 6, 7, 8};
        buf->size = 8;
        buf->media_type = MediaType::VIDEO_ENCODED;
        return buf;
    }

private:
    int max_frames_;
    int count_ = 0;
};

class MockTransformWithCaps : public TransformNode {
public:
    MockTransformWithCaps(const std::string& name) : TransformNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED}});
    }
    bool got_caps() const { return got_caps_; }
    int processed() const { return count_; }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onStreamInfo() override {
        auto item = sink_pads_[0]->popBlocking();
        if (item && std::holds_alternative<Event>(*item)) {
            auto& event = std::get<Event>(*item);
            if (std::holds_alternative<CapsEvent>(event)) {
                negotiated_caps_["in"] = std::get<CapsEvent>(event);
                got_caps_ = true;
            }
        }
        return true;
    }

    void process(Buffer* input, std::vector<Buffer*>& outputs) override {
        count_++;
        auto* out = new Buffer();
        out->data = new uint8_t[input->size];
        memcpy(out->data, input->data, input->size);
        out->size = input->size;
        out->media_type = MediaType::VIDEO_RAW;
        outputs.push_back(out);
    }

private:
    int count_ = 0;
    bool got_caps_ = false;
};

// ===================================================================
// 组件级测试
// ===================================================================

static void test_template_caps() {
    printf("  test_template_caps...");
    fflush(stdout);

    TemplateCaps video_raw{{MediaType::VIDEO_RAW}};
    TemplateCaps video_enc{{MediaType::VIDEO_ENCODED}};
    TemplateCaps audio_raw{{MediaType::AUDIO_RAW}};
    TemplateCaps multi{{MediaType::VIDEO_RAW, MediaType::AUDIO_RAW}};

    assert(video_raw.isCompatibleWith(video_raw));
    assert(!video_raw.isCompatibleWith(video_enc));
    assert(!video_raw.isCompatibleWith(audio_raw));
    assert(multi.isCompatibleWith(video_raw));
    assert(multi.isCompatibleWith(audio_raw));
    assert(!multi.isCompatibleWith(video_enc));

    printf(" OK\n");
}

static void test_buffer_refcount() {
    printf("  test_buffer_refcount...");
    fflush(stdout);

    auto* buf = new Buffer();
    buf->data = new uint8_t[4]{1, 2, 3, 4};
    buf->size = 4;
    buf->media_type = MediaType::VIDEO_RAW;

    assert(buf->ref_count.load() == 1);
    buf->ref();
    assert(buf->ref_count.load() == 2);
    buf->unref();
    assert(buf->ref_count.load() == 1);
    buf->unref();  // ref_count → 0，释放

    printf(" OK\n");
}

static void test_buffer_clone() {
    printf("  test_buffer_clone...");
    fflush(stdout);

    auto* buf = new Buffer();
    buf->data = new uint8_t[4]{10, 20, 30, 40};
    buf->size = 4;
    buf->media_type = MediaType::VIDEO_RAW;
    buf->pts = 12345;

    auto* clone = buf->clone();
    assert(clone != buf);
    assert(clone->size == 4);
    assert(memcmp(clone->data, buf->data, 4) == 0);
    assert(clone->pts == 12345);
    assert(clone->ref_count.load() == 1);

    buf->unref();
    clone->unref();

    printf(" OK\n");
}

static void test_buffer_ref() {
    printf("  test_buffer_ref...");
    fflush(stdout);

    auto* buf = new Buffer();
    buf->data = new uint8_t[4]{1, 2, 3, 4};
    buf->size = 4;
    buf->media_type = MediaType::VIDEO_RAW;

    BufferRef ref1(buf);
    assert(buf->ref_count.load() == 1);

    {
        BufferRef ref2(ref1);
        assert(buf->ref_count.load() == 2);
    }
    assert(buf->ref_count.load() == 1);

    printf(" OK\n");
}

static void test_bounded_queue_basic() {
    printf("  test_bounded_queue_basic...");
    fflush(stdout);

    BoundedQueue q(4);
    assert(q.empty());
    assert(q.size() == 0);
    assert(!q.full());

    auto* buf = new Buffer();
    buf->data = new uint8_t[1]{42};
    buf->size = 1;
    buf->media_type = MediaType::VIDEO_RAW;

    q.pushBlocking(QueueItem{BufferRef{buf}});
    assert(q.size() == 1);

    auto item = q.tryPop();
    assert(item.has_value());
    assert(std::holds_alternative<BufferRef>(*item));
    assert(q.empty());

    printf(" OK\n");
}

static void test_bounded_queue_try_push_full() {
    printf("  test_bounded_queue_try_push_full...");
    fflush(stdout);

    BoundedQueue q(2);

    for (int i = 0; i < 2; i++) {
        auto* buf = new Buffer();
        buf->data = new uint8_t[1]{static_cast<uint8_t>(i)};
        buf->size = 1;
        buf->media_type = MediaType::VIDEO_RAW;
        assert(q.tryPush(QueueItem{BufferRef{buf}}));
    }

    auto* buf3 = new Buffer();
    buf3->data = new uint8_t[1]{99};
    buf3->size = 1;
    buf3->media_type = MediaType::VIDEO_RAW;
    assert(!q.tryPush(QueueItem{BufferRef{buf3}}));
    buf3->unref();

    printf(" OK\n");
}

static void test_bounded_queue_flush() {
    printf("  test_bounded_queue_flush...");
    fflush(stdout);

    BoundedQueue q(4);
    q.pushBlocking(QueueItem{EOSEvent{}});
    q.flush();
    // flush 后队列不可再用，不崩溃就算成功

    printf(" OK\n");
}

static void test_bounded_queue_resize() {
    printf("  test_bounded_queue_resize...");
    fflush(stdout);

    BoundedQueue q(2);

    for (int i = 0; i < 2; i++) {
        auto* buf = new Buffer();
        buf->data = new uint8_t[1]{static_cast<uint8_t>(i)};
        buf->size = 1;
        buf->media_type = MediaType::VIDEO_RAW;
        assert(q.tryPush(QueueItem{BufferRef{buf}}));
    }
    assert(q.full());

    q.resize(4);
    assert(!q.full());

    auto* buf3 = new Buffer();
    buf3->data = new uint8_t[1]{3};
    buf3->size = 1;
    buf3->media_type = MediaType::VIDEO_RAW;
    assert(q.tryPush(QueueItem{BufferRef{buf3}}));

    printf(" OK\n");
}

static void test_select_queue_capacity() {
    printf("  test_select_queue_capacity...");
    fflush(stdout);

    assert(selectQueueCapacity(MediaType::VIDEO_RAW) == DEFAULT_QUEUE_CAPACITY_VIDEO_RAW);
    assert(selectQueueCapacity(MediaType::AUDIO_RAW) == DEFAULT_QUEUE_CAPACITY_AUDIO_RAW);
    assert(selectQueueCapacity(MediaType::VIDEO_ENCODED) == DEFAULT_QUEUE_CAPACITY_ENCODED);
    assert(selectQueueCapacity(MediaType::AUDIO_ENCODED) == DEFAULT_QUEUE_CAPACITY_ENCODED);
    assert(selectQueueCapacity(MediaType::CONTAINER) == DEFAULT_QUEUE_CAPACITY_CONTAINER);

    printf(" OK\n");
}

// ===================================================================
// 集成级测试
// ===================================================================

static void test_pipeline_source_to_sink() {
    printf("  test_pipeline_source_to_sink...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src  = pipeline.addNode<MockSource>("src", 10, MediaType::VIDEO_RAW);
    auto* sink = pipeline.addNode<MockSink>("sink");

    assert(pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(sink->received() == 10);

    printf(" OK\n");
}

static void test_pipeline_source_transform_sink() {
    printf("  test_pipeline_source_transform_sink...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src   = pipeline.addNode<MockSource>("src", 5, MediaType::VIDEO_RAW);
    auto* xform = pipeline.addNode<MockTransform>("xform");
    auto* sink  = pipeline.addNode<MockSink>("sink");

    assert(pipeline.link(src, "out", xform, "in", MediaType::VIDEO_RAW));
    assert(pipeline.link(xform, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(xform->processed() == 5);
    assert(sink->received() == 5);

    printf(" OK\n");
}

static void test_pipeline_stop_idempotent() {
    printf("  test_pipeline_stop_idempotent...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src  = pipeline.addNode<MockSource>("src", 100, MediaType::VIDEO_RAW);
    auto* sink = pipeline.addNode<MockSink>("sink");

    pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW);
    pipeline.build();
    pipeline.play();

    pipeline.stop();
    pipeline.stop();
    pipeline.stop();

    printf(" OK\n");
}

static void test_pipeline_stop_before_play() {
    printf("  test_pipeline_stop_before_play...");
    fflush(stdout);

    Pipeline pipeline;
    pipeline.stop();  // CAS 失败直接返回

    printf(" OK\n");
}

static void test_pipeline_play_without_build() {
    printf("  test_pipeline_play_without_build...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src = pipeline.addNode<MockSource>("src", 1, MediaType::VIDEO_RAW);
    auto* sink = pipeline.addNode<MockSink>("sink");
    pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW);

    assert(!pipeline.play());

    printf(" OK\n");
}

static void test_pipeline_build_twice() {
    printf("  test_pipeline_build_twice...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src = pipeline.addNode<MockSource>("src", 1, MediaType::VIDEO_RAW);
    auto* sink = pipeline.addNode<MockSink>("sink");
    pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW);

    assert(pipeline.build());
    assert(!pipeline.build());

    printf(" OK\n");
}

static void test_pipeline_link_incompatible() {
    printf("  test_pipeline_link_incompatible...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src = pipeline.addNode<MockSource>("src", 1, MediaType::VIDEO_RAW);
    auto* sink = pipeline.addNode<MockSink>("sink");

    assert(!pipeline.link(src, "out", sink, "in", MediaType::AUDIO_ENCODED));

    printf(" OK\n");
}

static void test_pipeline_caps_propagation() {
    printf("  test_pipeline_caps_propagation...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src   = pipeline.addNode<MockSourceWithCaps>("src", 3);
    auto* xform = pipeline.addNode<MockTransformWithCaps>("xform");
    auto* sink  = pipeline.addNode<MockSink>("sink");

    assert(pipeline.link(src, "out", xform, "in", MediaType::VIDEO_ENCODED));
    assert(pipeline.link(xform, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(xform->got_caps());
    assert(xform->processed() == 3);
    assert(sink->received() == 3);

    printf(" OK\n");
}

static void test_pipeline_concurrent_stop() {
    printf("  test_pipeline_concurrent_stop...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src  = pipeline.addNode<MockSource>("src", 1000, MediaType::VIDEO_RAW);
    auto* sink = pipeline.addNode<MockSink>("sink");

    pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW);
    pipeline.build();
    pipeline.play();

    std::thread t1([&]() { pipeline.stop(); });
    std::thread t2([&]() { pipeline.stop(); });
    t1.join();
    t2.join();

    printf(" OK\n");
}

static void test_pipeline_wait_eos_and_stop() {
    printf("  test_pipeline_wait_eos_and_stop...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src  = pipeline.addNode<MockSource>("src", 1000, MediaType::VIDEO_RAW);
    auto* sink = pipeline.addNode<MockSink>("sink");

    pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW);
    pipeline.build();
    pipeline.play();

    std::thread t1([&]() { pipeline.waitEOS(); });
    std::thread t2([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        pipeline.stop();
    });
    t1.join();
    t2.join();

    printf(" OK\n");
}

// ===================================================================

int main() {
    printf("=== Phase 2 Unit Tests ===\n\n");

    printf("[Component Tests]\n");
    test_template_caps();
    test_buffer_refcount();
    test_buffer_clone();
    test_buffer_ref();
    test_bounded_queue_basic();
    test_bounded_queue_try_push_full();
    test_bounded_queue_flush();
    test_bounded_queue_resize();
    test_select_queue_capacity();

    printf("\n[Integration Tests]\n");
    test_pipeline_source_to_sink();
    test_pipeline_source_transform_sink();
    test_pipeline_stop_idempotent();
    test_pipeline_stop_before_play();
    test_pipeline_play_without_build();
    test_pipeline_build_twice();
    test_pipeline_link_incompatible();
    test_pipeline_caps_propagation();
    test_pipeline_concurrent_stop();
    test_pipeline_wait_eos_and_stop();

    printf("\n=== All Tests Passed ===\n");
    return 0;
}
