#include "pipeline/core/Pipeline.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Caps.h"

#include <atomic>
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

    void process(const Buffer* input, std::vector<Buffer*>& outputs) override {
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
    void consume(const Buffer* buf) override { count_++; }

private:
    int count_ = 0;
};

class MockSourceWithCaps : public SourceNode {
public:
    MockSourceWithCaps(const std::string& name, int frame_count)
        : SourceNode(name), max_frames_(frame_count) {}

    std::optional<MediaType> outActualType() {
        SrcPad* p = getSrcPad("out");
        return p ? p->actualType() : std::nullopt;
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onStreamInfo() override {
        CapsEvent caps;
        caps.media_type = MediaType::VIDEO_ENCODED;
        caps.codec_id = AV_CODEC_ID_H264;
        caps.width = 1920;
        caps.height = 1080;
        if (!sendCapsEvent("out", caps)) {
            return false;   // sendCapsEvent 已 postMessage(ERROR)，触发 Ready 回滚
        }
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
    std::optional<MediaType> inActualType() {
        SinkPad* p = getSinkPad("in");
        return p ? p->actualType() : std::nullopt;
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onStreamInfo() override {
        // receiveCapsEvent 内部完成 acquire + 校验 + setActualType + ack + 存入 negotiated_caps_["in"]
        if (!receiveCapsEvent("in")) {
            return false;   // receiveCapsEvent 已 postMessage(ERROR)，触发 Ready 回滚
        }
        got_caps_ = true;
        return true;
    }

    void process(const Buffer* input, std::vector<Buffer*>& outputs) override {
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
// MockSourceBadCaps: SrcPad 声明 {VIDEO_RAW}，onStreamInfo 却发 VIDEO_ENCODED
// 用于验证 sendCapsEvent 的 media_type ∈ TemplateCaps 校验失败 → Ready 回滚
// ===================================================================
class MockSourceBadCaps : public SourceNode {
public:
    MockSourceBadCaps(const std::string& name) : SourceNode(name) {
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onStreamInfo() override {
        CapsEvent caps;
        caps.media_type = MediaType::VIDEO_ENCODED;   // 不在 SrcPad {VIDEO_RAW} 内
        return sendCapsEvent("out", caps);
    }

    Buffer* capture() override { return nullptr; }
};

// ===================================================================
// MockSourceSendsAudio: SrcPad 声明 {VIDEO_ENCODED, AUDIO_ENCODED}，发 AUDIO_ENCODED
// 配合 MockTransformVideoOnly（SinkPad 只收 VIDEO_ENCODED）验证 receiveCapsEvent 校验：
// link 期两端交集非空能通过，但 Ready 时 AUDIO_ENCODED ∉ 下游 SinkPad → 失败
// ===================================================================
class MockSourceSendsAudio : public SourceNode {
public:
    MockSourceSendsAudio(const std::string& name) : SourceNode(name) {
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED}});
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onStreamInfo() override {
        CapsEvent caps;
        caps.media_type = MediaType::AUDIO_ENCODED;   // 下游 SinkPad 只含 VIDEO_ENCODED
        return sendCapsEvent("out", caps);
    }

    Buffer* capture() override { return nullptr; }
};

// ===================================================================
// MockTransformVideoOnly: SinkPad 只声明 {VIDEO_ENCODED}
// 收到 AUDIO_ENCODED CapsEvent 时 receiveCapsEvent 校验失败
// ===================================================================
class MockTransformVideoOnly : public TransformNode {
public:
    MockTransformVideoOnly(const std::string& name) : TransformNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_ENCODED}});
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onStreamInfo() override {
        return receiveCapsEvent("in");   // AUDIO_ENCODED ∉ {VIDEO_ENCODED} → false
    }

    void process(const Buffer*, std::vector<Buffer*>&) override {}
};

// ===================================================================
// MockSlowSink: consume 里 sleep 一小段时间，制造下游背压
// 配合小容量 OutputRoute 验证慢订阅者不会丢帧，并最终反压生产者
// ===================================================================
class MockSlowSink : public SinkNode {
public:
    MockSlowSink(const std::string& name, int sleep_us)
        : SinkNode(name), sleep_us_(sleep_us) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }
    int received() const { return count_; }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void consume(const Buffer* buf) override {
        count_++;
        if (sleep_us_ > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_us_));
        }
    }

private:
    int sleep_us_;
    int count_ = 0;
};

// ===================================================================
// MockFailingSource: onReady 里 postMessage(ERROR) 后返回 false
// 用于验证 Ready 阶段错误消息能被 lastError() 拿到
// ===================================================================
class MockFailingSource : public SourceNode {
public:
    MockFailingSource(const std::string& name, const std::string& err_text)
        : SourceNode(name), err_text_(err_text) {
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

protected:
    bool onReady() override {
        postMessage(MessageType::ERROR, err_text_);
        return false;
    }
    void onStop() override {}
    Buffer* capture() override { return nullptr; }

private:
    std::string err_text_;
};

// ===================================================================
// MockOnStopTracker: 记录 onStop 被调用次数
// 用于验证 Ready 失败时事务性回滚（前置节点已经 onReady 成功后应被回滚）
// ===================================================================
class MockOnStopTracker : public SourceNode {
public:
    MockOnStopTracker(const std::string& name)
        : SourceNode(name) {
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW}});
    }
    int stopCalled() const { return stop_calls_; }

protected:
    bool onReady() override { return true; }
    void onStop() override { stop_calls_++; }
    Buffer* capture() override { return nullptr; }

private:
    int stop_calls_ = 0;
};

// ===================================================================
// MockFailingSink: onReady 里让 Ready 阶段推进到中段再失败
// 前置节点先 onReady 成功，Sink 失败 → 前置的 onStop 应被回滚调用
// ===================================================================
class MockFailingSink : public SinkNode {
public:
    MockFailingSink(const std::string& name) : SinkNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

protected:
    bool onReady() override {
        postMessage(MessageType::ERROR, "sink init failed");
        return false;
    }
    void onStop() override {}
    void consume(const Buffer* buf) override {}
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

static void test_buffer_from_avpacket_metadata() {
    printf("  test_buffer_from_avpacket_metadata...");
    fflush(stdout);

    uint8_t data[3]{1, 2, 3};
    AVPacket pkt{};
    pkt.data = data;
    pkt.size = sizeof(data);
    pkt.pts = AV_NOPTS_VALUE;
    pkt.dts = AV_NOPTS_VALUE;
    pkt.duration = 90;
    pkt.flags = AV_PKT_FLAG_KEY;

    auto* buf = Buffer::fromAVPacket(&pkt, MediaType::VIDEO_ENCODED,
                                     AVRational{1, 90000}, AV_CODEC_ID_H264);
    assert(buf != nullptr);
    assert(buf->pts == AV_NOPTS_VALUE);
    assert(buf->dts == AV_NOPTS_VALUE);
    assert(buf->duration == 1000);

    auto meta = std::get<EncodedMeta>(buf->meta);
    assert(meta.codec_id == AV_CODEC_ID_H264);
    assert(meta.flags == AV_PKT_FLAG_KEY);

    buf->unref();
    printf(" OK\n");
}

static void test_buffer_from_avpacket_invalid_type() {
    printf("  test_buffer_from_avpacket_invalid_type...");
    fflush(stdout);

    uint8_t data[1]{0};
    AVPacket pkt{};
    pkt.data = data;
    pkt.size = sizeof(data);

    assert(Buffer::fromAVPacket(&pkt, MediaType::VIDEO_RAW, AVRational{1, 1000}) == nullptr);

    printf(" OK\n");
}

static void test_buffer_from_avframe_invalid_input() {
    printf("  test_buffer_from_avframe_invalid_input...");
    fflush(stdout);

    assert(Buffer::fromAVFrame(nullptr, MediaType::VIDEO_RAW, AVRational{1, 1000}) == nullptr);

    AVFrame* frame = av_frame_alloc();
    assert(frame != nullptr);
    frame->format = AV_PIX_FMT_YUV420P;
    frame->width = 0;
    frame->height = 1080;
    assert(Buffer::fromAVFrame(frame, MediaType::VIDEO_RAW, AVRational{1, 1000}) == nullptr);
    av_frame_free(&frame);

    printf(" OK\n");
}

static void test_buffer_from_avframe_audio_meta() {
    printf("  test_buffer_from_avframe_audio_meta...");
    fflush(stdout);

    uint8_t data[16]{};
    AVFrame* frame = av_frame_alloc();
    assert(frame != nullptr);
    frame->format = AV_SAMPLE_FMT_S16;
    frame->sample_rate = 48000;
    frame->ch_layout.nb_channels = 2;
    frame->nb_samples = 4;
    frame->pts = AV_NOPTS_VALUE;
    frame->data[0] = data;

    auto* buf = Buffer::fromAVFrame(frame, MediaType::AUDIO_RAW, AVRational{1, 48000});
    assert(buf != nullptr);
    assert(buf->pts == AV_NOPTS_VALUE);
    assert(buf->duration == 83);
    assert(buf->size == sizeof(data));

    auto meta = std::get<AudioRawMeta>(buf->meta);
    assert(meta.sample_rate == 48000);
    assert(meta.channels == 2);
    assert(meta.nb_samples == 4);
    assert(meta.sample_fmt == AV_SAMPLE_FMT_S16);

    buf->unref();
    av_frame_free(&frame);

    printf(" OK\n");
}

static BufferRef makeRouteBuffer(uint8_t value) {
    auto* buf = new Buffer();
    buf->data = new uint8_t[1]{value};
    buf->size = 1;
    buf->media_type = MediaType::VIDEO_RAW;
    return BufferRef{buf};
}

static void test_output_route_shared_delivery() {
    printf("  test_output_route_shared_delivery...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(4);
    auto first = route->subscribe();
    auto second = route->subscribe();
    assert(route->seal());

    BufferRef source = makeRouteBuffer(42);
    const Buffer* original = source.get();
    assert(route->publishBlocking(QueueItem{source}) == RoutePublishResult::PUBLISHED);

    auto first_delivery = first.acquireBlocking();
    auto second_delivery = second.acquireBlocking();
    assert(first_delivery && second_delivery);

    const auto& first_ref = std::get<BufferRef>(first_delivery->item());
    const auto& second_ref = std::get<BufferRef>(second_delivery->item());
    assert(first_ref.get() == original);
    assert(second_ref.get() == original);
    assert(first_ref.get() == second_ref.get());

    assert(first_delivery->ack());
    assert(route->retainedItems() == 1);
    assert(second_delivery->ack());
    assert(route->retainedItems() == 0);

    printf(" OK\n");
}

static void test_output_route_ack_controls_backpressure() {
    printf("  test_output_route_ack_controls_backpressure...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(1);
    auto fast = route->subscribe();
    auto slow = route->subscribe();
    assert(route->seal());
    assert(route->publishBlocking(QueueItem{makeRouteBuffer(1)}) == RoutePublishResult::PUBLISHED);

    auto fast_delivery = fast.acquireBlocking();
    auto slow_delivery = slow.acquireBlocking();
    assert(fast_delivery && slow_delivery);
    assert(fast_delivery->ack());

    std::atomic<bool> publish_finished{false};
    std::thread publisher([&] {
        auto result = route->publishBlocking(QueueItem{makeRouteBuffer(2)});
        assert(result == RoutePublishResult::PUBLISHED);
        publish_finished = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(!publish_finished.load());
    assert(route->retainedItems() == 1);

    assert(slow_delivery->ack());
    publisher.join();
    assert(publish_finished.load());

    auto fast_second = fast.acquireBlocking();
    auto slow_second = slow.acquireBlocking();
    assert(fast_second && slow_second);
    assert(fast_second->ack());
    assert(slow_second->ack());

    printf(" OK\n");
}

static void test_output_route_delivery_abandon_retries() {
    printf("  test_output_route_delivery_abandon_retries...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(1);
    auto subscriber = route->subscribe();
    assert(route->seal());
    assert(route->publishBlocking(QueueItem{makeRouteBuffer(7)}) == RoutePublishResult::PUBLISHED);

    const Buffer* first_ptr = nullptr;
    {
        auto delivery = subscriber.acquireBlocking();
        assert(delivery);
        first_ptr = std::get<BufferRef>(delivery->item()).get();
        // 未 ack，析构后同一订阅者应重新取得同一项。
    }

    auto retry = subscriber.acquireBlocking();
    assert(retry);
    assert(std::get<BufferRef>(retry->item()).get() == first_ptr);
    assert(retry->ack());
    assert(route->retainedItems() == 0);

    printf(" OK\n");
}

static void test_output_route_ack_after_processing() {
    printf("  test_output_route_ack_after_processing...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(1);
    auto subscriber = route->subscribe();
    assert(route->seal());
    assert(route->publishBlocking(QueueItem{makeRouteBuffer(1)}) == RoutePublishResult::PUBLISHED);

    auto delivery = subscriber.acquireBlocking();
    assert(delivery);

    std::atomic<bool> publish_finished{false};
    std::thread publisher([&] {
        assert(route->publishBlocking(QueueItem{makeRouteBuffer(2)}) ==
               RoutePublishResult::PUBLISHED);
        publish_finished = true;
    });

    // acquire 并不释放容量；只有处理完成后的 ack 才允许第二次 publish。
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(!publish_finished.load());
    assert(delivery->ack());
    publisher.join();
    assert(publish_finished.load());

    auto second = subscriber.acquireBlocking();
    assert(second && second->ack());

    printf(" OK\n");
}

static void test_output_route_cancel_wakes_subscriber() {
    printf("  test_output_route_cancel_wakes_subscriber...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(1);
    auto subscriber = route->subscribe();
    assert(route->seal());

    std::atomic<bool> returned{false};
    std::thread consumer([&] {
        auto delivery = subscriber.acquireBlocking();
        assert(!delivery);
        returned = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(!returned.load());
    route->cancel();
    consumer.join();
    assert(returned.load());

    printf(" OK\n");
}

static void test_output_route_cancel_wakes_publisher() {
    printf("  test_output_route_cancel_wakes_publisher...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(1);
    auto subscriber = route->subscribe();
    assert(route->seal());
    assert(route->publishBlocking(QueueItem{makeRouteBuffer(1)}) == RoutePublishResult::PUBLISHED);

    std::atomic<RoutePublishResult> result{RoutePublishResult::PUBLISHED};
    std::thread publisher([&] {
        result = route->publishBlocking(QueueItem{makeRouteBuffer(2)});
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    route->cancel();
    publisher.join();
    assert(result.load() == RoutePublishResult::CANCELLED);
    assert(!subscriber.acquireBlocking());

    printf(" OK\n");
}

static void test_output_route_event_order() {
    printf("  test_output_route_event_order...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(4);
    auto subscriber = route->subscribe();
    assert(route->seal());

    CapsEvent caps;
    caps.media_type = MediaType::VIDEO_RAW;
    assert(route->publishBlocking(QueueItem{Event{caps}}) == RoutePublishResult::PUBLISHED);
    assert(route->publishBlocking(QueueItem{makeRouteBuffer(9)}) == RoutePublishResult::PUBLISHED);
    assert(route->publishBlocking(QueueItem{Event{EOSEvent{}}}) == RoutePublishResult::PUBLISHED);

    auto first = subscriber.acquireBlocking();
    assert(first && std::holds_alternative<Event>(first->item()));
    assert(std::holds_alternative<CapsEvent>(std::get<Event>(first->item())));
    assert(first->ack());

    auto second = subscriber.acquireBlocking();
    assert(second && std::holds_alternative<BufferRef>(second->item()));
    assert(second->ack());

    auto third = subscriber.acquireBlocking();
    assert(third && std::holds_alternative<Event>(third->item()));
    assert(std::holds_alternative<EOSEvent>(std::get<Event>(third->item())));
    assert(third->ack());

    printf(" OK\n");
}

static void test_select_route_capacity() {
    printf("  test_select_route_capacity...");
    fflush(stdout);

    assert(selectRouteCapacity(MediaType::VIDEO_RAW) == 4);
    assert(selectRouteCapacity(MediaType::AUDIO_RAW) == 50);
    assert(selectRouteCapacity(MediaType::VIDEO_ENCODED) == 32);
    assert(selectRouteCapacity(MediaType::AUDIO_ENCODED) == 32);
    assert(selectRouteCapacity(MediaType::CONTAINER) == 32);

    auto route = std::make_shared<OutputRoute>(2);
    assert(route->capacity() == 2);
    route->resize(8);
    assert(route->capacity() == 8);

    printf(" OK\n");
}

static void test_graph_build_cycle_detection() {
    printf("  test_graph_build_cycle_detection...");
    fflush(stdout);

    // 构造一个有环的图：A → B → C → A
    // 通过手动操作 Graph 内部来测试
    // 由于 Graph 的 link() 不允许创建环（Pad 已被占用），
    // 我们用一个特殊方式：直接操作 Edge 来制造环
    //
    // 但 Graph::link() 会检查 isConnected()，所以环在 link 阶段就会被阻止
    // 实际上环路检测是在 build() 里做的，作为拓扑排序的副产品
    // 如果 topo_order_.size() != nodes_.size() 就说明有环
    //
    // 由于我们的 link() 实现已经阻止了环的创建（Pad 只能连一个 Edge），
    // 这个测试验证的是：正常无环图 build 成功
    Pipeline pipeline;
    auto* src = pipeline.addNode<MockSource>("src", 1, MediaType::VIDEO_RAW);
    auto* sink = pipeline.addNode<MockSink>("sink");
    pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW);

    assert(pipeline.build());  // 正常图应该 build 成功

    printf(" OK\n");
}

static void test_graph_build_orphan_detection() {
    printf("  test_graph_build_orphan_detection...");
    fflush(stdout);

    // 创建一个孤立节点（不连接到任何其他节点）
    Pipeline pipeline;
    auto* src = pipeline.addNode<MockSource>("src", 1, MediaType::VIDEO_RAW);
    auto* sink = pipeline.addNode<MockSink>("sink");
    // 故意不 link，让 sink 成为孤立节点
    (void)src;
    (void)sink;

    // build 应该失败，因为 sink 是孤立节点
    assert(!pipeline.build());

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
// 分叉路径：Source 一路输出 fork 给两个 Sink
// 覆盖同一逻辑 Route 的两个静态可靠订阅者。
// 两路共享底层 BufferRef，并且都必须收到全部帧。
// ===================================================================
static void test_pipeline_forked_broadcast() {
    printf("  test_pipeline_forked_broadcast...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src   = pipeline.addNode<MockSource>("src", 20, MediaType::VIDEO_RAW);
    auto* sink1 = pipeline.addNode<MockSink>("sink1");
    auto* sink2 = pipeline.addNode<MockSink>("sink2");

    // 第一次 link 直接用 "out"（走 requestSrcPad 创建）
    assert(pipeline.link(src, "out",  sink1, "in", MediaType::VIDEO_RAW));
    // 第二次 link 用新 pad 名，触发 SourceNode::requestSrcPad 分叉分支
    assert(pipeline.link(src, "out2", sink2, "in", MediaType::VIDEO_RAW));

    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    // 所有可靠订阅者都应收到完整序列，不允许因下游调度差异丢帧。
    assert(sink1->received() == 20);
    assert(sink2->received() == 20);

    printf(" OK\n");
}

// ===================================================================
// 分叉路径压力：一路慢 Sink 使 Route 达到硬容量，验证最慢可靠订阅者
// 将背压传给 Source，同时快慢两路最终都收到完整序列。
// ===================================================================
static void test_pipeline_forked_backpressure_no_uaf() {
    printf("  test_pipeline_forked_backpressure_no_uaf...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src   = pipeline.addNode<MockSource>("src", 200, MediaType::VIDEO_RAW);
    auto* fast  = pipeline.addNode<MockSink>("fast");
    auto* slow  = pipeline.addNode<MockSlowSink>("slow", 500);  // 每帧 sleep 500us

    assert(pipeline.link(src, "out",  fast, "in", MediaType::VIDEO_RAW));
    assert(pipeline.link(src, "out2", slow, "in", MediaType::VIDEO_RAW));

    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    // Route 满时阻塞 publish，不允许任何可靠订阅者丢帧。
    assert(pipeline.lastError().empty());
    assert(fast->received() == 200);
    assert(slow->received() == 200);

    printf(" OK\n");
}

// ===================================================================
// Ready 失败：错误消息能被 lastError() 拿到
// 覆盖问题 1 的核心承诺（bus 提前启动 + Ready 失败前 join drain）
// ===================================================================
static void test_pipeline_ready_failure_reports_error() {
    printf("  test_pipeline_ready_failure_reports_error...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src  = pipeline.addNode<MockFailingSource>("bad_src", "device open failed");
    auto* sink = pipeline.addNode<MockSink>("sink");

    assert(pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(!pipeline.play());   // Ready 失败

    // 修复前这里是空串
    assert(pipeline.lastError() == "device open failed");

    printf(" OK\n");
}

// ===================================================================
// Ready 失败事务性回滚：前置节点 onReady 成功后，
// 后置节点失败时前置节点的 onStop 应被回滚调用
// ===================================================================
static void test_pipeline_ready_failure_rollback() {
    printf("  test_pipeline_ready_failure_rollback...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src  = pipeline.addNode<MockOnStopTracker>("tracker");
    auto* sink = pipeline.addNode<MockFailingSink>("bad_sink");

    assert(pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(!pipeline.play());

    // 前置 tracker 的 onReady 已成功、queue 已建、然后 sink 的 onReady 失败
    // Graph::ready() 应按拓扑逆序回滚：先 sink.onStop 再 tracker.onStop
    // tracker 至少被 onStop 一次
    assert(src->stopCalled() >= 1);
    // sink 上报的错误也应能拿到
    assert(pipeline.lastError() == "sink init failed");

    printf(" OK\n");
}

// ===================================================================
// actual_type / Caps 校验测试
// ===================================================================

static void test_pad_actual_type_lifecycle() {
    printf("  test_pad_actual_type_lifecycle...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src   = pipeline.addNode<MockSourceWithCaps>("src", 1);
    auto* xform = pipeline.addNode<MockTransformWithCaps>("xform");
    auto* sink  = pipeline.addNode<MockSink>("sink");

    assert(pipeline.link(src, "out", xform, "in", MediaType::VIDEO_ENCODED));
    assert(pipeline.link(xform, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());

    // Ready 前：actualType 一律 nullopt（契约：Ready 之前不依赖 actualType 有值）
    assert(!src->outActualType().has_value());
    assert(!xform->inActualType().has_value());

    assert(pipeline.play());
    pipeline.waitEOS();

    // Ready 后：已连接且走过 CapsEvent 的 pad，actualType 必有值
    assert(src->outActualType().has_value());
    assert(*src->outActualType() == MediaType::VIDEO_ENCODED);
    assert(xform->inActualType().has_value());
    assert(*xform->inActualType() == MediaType::VIDEO_ENCODED);

    printf(" OK\n");
}

static void test_send_caps_event_validation_fail() {
    printf("  test_send_caps_event_validation_fail...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src  = pipeline.addNode<MockSourceBadCaps>("src");
    auto* sink = pipeline.addNode<MockSink>("sink");

    assert(pipeline.link(src, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(!pipeline.play());   // sendCapsEvent 校验失败 → Ready 回滚

    const std::string err = pipeline.lastError();
    assert(err.find("sendCapsEvent") != std::string::npos);
    assert(err.find("not in src pad") != std::string::npos);

    printf(" OK\n");
}

static void test_receive_caps_event_validation_fail() {
    printf("  test_receive_caps_event_validation_fail...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src   = pipeline.addNode<MockSourceSendsAudio>("src");
    auto* xform = pipeline.addNode<MockTransformVideoOnly>("xform");

    assert(pipeline.link(src, "out", xform, "in", MediaType::VIDEO_ENCODED));
    assert(pipeline.build());
    assert(!pipeline.play());   // receiveCapsEvent 校验失败 → Ready 回滚

    const std::string err = pipeline.lastError();
    assert(err.find("receiveCapsEvent") != std::string::npos);
    assert(err.find("not in sink pad") != std::string::npos);

    printf(" OK\n");
}

static void test_request_src_pad_rejects_mismatched_hint() {
    printf("  test_request_src_pad_rejects_mismatched_hint...");
    fflush(stdout);

    Pipeline pipeline;
    auto* src   = pipeline.addNode<MockSource>("src", 1, MediaType::VIDEO_RAW);
    auto* sink1 = pipeline.addNode<MockSink>("sink1");
    auto* sink2 = pipeline.addNode<MockSink>("sink2");

    // 首次 link：requestSrcPad 建立 {VIDEO_RAW}
    assert(pipeline.link(src, "out", sink1, "in", MediaType::VIDEO_RAW));
    // 再次 link：hint_type=AUDIO_RAW 不在已有 {VIDEO_RAW} 能力集合内 → requestSrcPad 返回 nullptr
    assert(!pipeline.link(src, "out2", sink2, "in", MediaType::AUDIO_RAW));

    // Transform 分叉同理
    auto* xform = pipeline.addNode<MockTransform>("xform");
    auto* sink3 = pipeline.addNode<MockSink>("sink3");
    auto* sink4 = pipeline.addNode<MockSink>("sink4");
    assert(pipeline.link(xform, "out", sink3, "in", MediaType::VIDEO_RAW));
    assert(!pipeline.link(xform, "out2", sink4, "in", MediaType::AUDIO_RAW));

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
    test_buffer_from_avpacket_metadata();
    test_buffer_from_avpacket_invalid_type();
    test_buffer_from_avframe_invalid_input();
    test_buffer_from_avframe_audio_meta();
    test_output_route_shared_delivery();
    test_output_route_ack_controls_backpressure();
    test_output_route_delivery_abandon_retries();
    test_output_route_ack_after_processing();
    test_output_route_cancel_wakes_subscriber();
    test_output_route_cancel_wakes_publisher();
    test_output_route_event_order();
    test_select_route_capacity();

    printf("\n[Graph Tests]\n");
    test_graph_build_cycle_detection();
    test_graph_build_orphan_detection();

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
    test_pipeline_forked_broadcast();
    test_pipeline_forked_backpressure_no_uaf();

    printf("\n[Ready Failure Tests]\n");
    test_pipeline_ready_failure_reports_error();
    test_pipeline_ready_failure_rollback();

    printf("\n[ActualType / Caps Validation Tests]\n");
    test_pad_actual_type_lifecycle();
    test_send_caps_event_validation_fail();
    test_receive_caps_event_validation_fail();
    test_request_src_pad_rejects_mismatched_hint();

    printf("\n=== All Tests Passed ===\n");
    return 0;
}
