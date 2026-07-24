#include "pipeline/core/Pipeline.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Caps.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace pipeline;

namespace {

CapsEvent makeVideoCaps(int width = 16, int height = 8) {
    CapsEvent caps;
    caps.media_type = MediaType::VIDEO_RAW;
    caps.width = width;
    caps.height = height;
    caps.pix_fmt = AV_PIX_FMT_YUV420P;
    caps.framerate = AVRational{30, 1};
    return caps;
}

CapsEvent makeEncodedVideoCaps() {
    CapsEvent caps;
    caps.media_type = MediaType::VIDEO_ENCODED;
    caps.codec_id = AV_CODEC_ID_H264;
    caps.width = 16;
    caps.height = 8;
    caps.framerate = AVRational{30, 1};
    return caps;
}

CapsEvent makeAudioCaps() {
    CapsEvent caps;
    caps.media_type = MediaType::AUDIO_RAW;
    caps.sample_rate = 48000;
    caps.sample_fmt = AV_SAMPLE_FMT_S16;
    caps.channel_layout = ChannelLayout::stereo();
    return caps;
}

CapsEvent makeEncodedAudioCaps() {
    CapsEvent caps;
    caps.media_type = MediaType::AUDIO_ENCODED;
    caps.codec_id = AV_CODEC_ID_AAC;
    caps.sample_rate = 48000;
    caps.channel_layout = ChannelLayout::stereo();
    return caps;
}

BufferRef makeBuffer(MediaType type, uint8_t value = 1, size_t size = 1) {
    auto* buffer = new Buffer();
    buffer->data = new uint8_t[size];
    memset(buffer->data, value, size);
    buffer->size = size;
    buffer->media_type = type;
    if (type == MediaType::VIDEO_RAW) {
        buffer->meta = VideoRawMeta{};
    } else if (type == MediaType::AUDIO_RAW) {
        buffer->meta = AudioRawMeta{static_cast<int>(size / 4)};
    } else {
        buffer->meta = EncodedMeta{};
    }
    return BufferRef(buffer);
}

// ===================================================================
// CapsScriptSource emits an explicit Running sequence. It deliberately does not rely on Ready Caps;
// each script item passes through the same Route publish machinery as production nodes.
// ===================================================================
class CapsScriptSource final : public SourceNode {
public:
    CapsScriptSource(const std::string& name, std::vector<QueueItem> script)
        : SourceNode(name), script_(std::move(script)) {}

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    Buffer* capture() override { return nullptr; }

    void runLoop() override {
        for (auto& item : script_) {
            if (stop_requested_.load() || !publishOutputItem(std::move(item))) {
                return;
            }
        }
        sendEOSDownstream();
    }

private:
    std::vector<QueueItem> script_;
};

class CapsTrackingSink final : public SinkNode {
public:
    explicit CapsTrackingSink(const std::string& name) : SinkNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW, MediaType::AUDIO_RAW}});
    }

    int received() const { return received_.load(); }
    std::vector<int> appliedWidths() const {
        std::lock_guard lock(mutex_);
        return applied_widths_;
    }
    std::vector<int> consumedWidths() const {
        std::lock_guard lock(mutex_);
        return consumed_widths_;
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onCaps(const std::string&, const CapsEvent& caps,
                std::vector<QueueItem>*) override {
        std::lock_guard lock(mutex_);
        applied_widths_.push_back(caps.width);
        return true;
    }

    void consume(const Buffer*) override {
        std::lock_guard lock(mutex_);
        consumed_widths_.push_back(active_caps_.at("in").width);
        ++received_;
    }

private:
    std::atomic<int> received_{0};
    mutable std::mutex mutex_;
    std::vector<int> applied_widths_;
    std::vector<int> consumed_widths_;
};

class ForwardTransform final : public TransformNode {
public:
    explicit ForwardTransform(const std::string& name) : TransformNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

    int processed() const { return processed_.load(); }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onCaps(const std::string&, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override {
        assert(outputs != nullptr);
        outputs->emplace_back(Event{caps});
        return true;
    }

    void process(const Buffer* input, std::vector<QueueItem>& outputs) override {
        auto* copy = new Buffer();
        copy->data = new uint8_t[input->size];
        memcpy(copy->data, input->data, input->size);
        copy->size = input->size;
        copy->media_type = input->media_type;
        copy->pts = input->pts;
        copy->meta = input->meta;
        outputs.emplace_back(BufferRef(copy));
        ++processed_;
    }

private:
    std::atomic<int> processed_{0};
};

class StopAfterProduceTransform final : public TransformNode {
public:
    explicit StopAfterProduceTransform(const std::string& name) : TransformNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

    bool waitUntilOutputsReady() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(2), [this] { return outputs_ready_; });
    }

    bool observersAreSoleOwners() {
        std::lock_guard lock(mutex_);
        if (observers_.size() != 3) {
            return false;
        }
        for (const auto& observer : observers_) {
            if (observer->ref_count.load() != 1) {
                return false;
            }
        }
        return true;
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    bool onCaps(const std::string&, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override {
        outputs->emplace_back(Event{caps});
        return true;
    }

    void process(const Buffer*, std::vector<QueueItem>& outputs) override {
        for (uint8_t value = 1; value <= 3; ++value) {
            auto output = makeBuffer(MediaType::VIDEO_RAW, value);
            {
                std::lock_guard lock(mutex_);
                observers_.push_back(output);
            }
            outputs.emplace_back(std::move(output));
        }
        {
            std::lock_guard lock(mutex_);
            outputs_ready_ = true;
        }
        cv_.notify_one();

        while (!stop_requested_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<BufferRef> observers_;
    bool outputs_ready_ = false;
};

class EosAfterFlushTransform final : public TransformNode {
public:
    explicit EosAfterFlushTransform(const std::string& name) : TransformNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void process(const Buffer*, std::vector<QueueItem>&) override {}
    void onEOS(std::vector<QueueItem>& outputs) override {
        // A subclass only contributes delayed output. TransformNode::runLoop is responsible for the EOS item.
        outputs.emplace_back(makeBuffer(MediaType::VIDEO_RAW, 99));
    }
};

class OrderedVideoSink final : public SinkNode {
public:
    explicit OrderedVideoSink(const std::string& name) : SinkNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

    std::vector<uint8_t> values() const {
        std::lock_guard lock(mutex_);
        return values_;
    }
    bool sawEOS() const { return eos_.load(); }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void consume(const Buffer* buffer) override {
        std::lock_guard lock(mutex_);
        values_.push_back(buffer->data[0]);
    }
    void onDrain() override { eos_ = true; }

private:
    mutable std::mutex mutex_;
    std::vector<uint8_t> values_;
    std::atomic<bool> eos_{false};
};

class BlockingSink final : public SinkNode {
public:
    explicit BlockingSink(const std::string& name) : SinkNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

    bool waitUntilConsuming() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(2), [this] { return consuming_; });
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void consume(const Buffer*) override {
        {
            std::lock_guard lock(mutex_);
            consuming_ = true;
        }
        cv_.notify_one();
        while (!stop_requested_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool consuming_ = false;
};

class BurstTransform final : public TransformNode {
public:
    explicit BurstTransform(const std::string& name) : TransformNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

    bool waitUntilOutputRouteFull() {
        SrcPad* output = getSrcPad("out");
        if (!output || !output->route()) {
            return false;
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            if (output->route()->retainedItems() == output->route()->capacity()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return false;
    }

    bool observersAreSoleOwners() {
        std::lock_guard lock(mutex_);
        if (observers_.size() != 12) {
            return false;
        }
        for (const auto& observer : observers_) {
            if (observer->ref_count.load() != 1) {
                return false;
            }
        }
        return true;
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    bool onCaps(const std::string&, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override {
        outputs->emplace_back(Event{caps});
        return true;
    }
    void process(const Buffer*, std::vector<QueueItem>& outputs) override {
        for (uint8_t value = 1; value <= 12; ++value) {
            auto output = makeBuffer(MediaType::VIDEO_RAW, value);
            {
                std::lock_guard lock(mutex_);
                observers_.push_back(output);
            }
            outputs.emplace_back(std::move(output));
        }
    }

private:
    std::mutex mutex_;
    std::vector<BufferRef> observers_;
};

class MockSink final : public SinkNode {
public:
    explicit MockSink(const std::string& name) : SinkNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void consume(const Buffer*) override {}
};

class SlowVideoSink final : public SinkNode {
public:
    SlowVideoSink(const std::string& name, int sleep_us)
        : SinkNode(name), sleep_us_(sleep_us) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

    int received() const { return received_.load(); }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void consume(const Buffer*) override {
        ++received_;
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_us_));
    }

private:
    int sleep_us_;
    std::atomic<int> received_{0};
};

class ContainerSink final : public SinkNode {
public:
    explicit ContainerSink(const std::string& name) : SinkNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::CONTAINER}});
    }

    int received() const { return received_.load(); }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void consume(const Buffer*) override { ++received_; }

private:
    std::atomic<int> received_{0};
};

class FakeMux final : public MuxNode {
public:
    explicit FakeMux(const std::string& name) : MuxNode(name, MuxFormat::MPEGTS) {}

    int streamCount() const { return stream_count_; }

private:
    bool allocateContext(MuxFormat) override { return true; }
    bool addStream(const CapsEvent&, int* stream_index) override {
        *stream_index = stream_count_++;
        return true;
    }
    bool writeHeader() override {
        const uint8_t header = 0x48;
        return appendContainerBytes(&header, 1);
    }
    bool writePacket(const Buffer*, int) override {
        const uint8_t packet = 0x50;
        return appendContainerBytes(&packet, 1);
    }
    bool writeTrailer() override {
        const uint8_t trailer = 0x54;
        return appendContainerBytes(&trailer, 1);
    }
    void closeContext() override {}

    int stream_count_ = 0;
};

void test_template_caps_compatibility() {
    printf("  test_template_caps_compatibility...");
    fflush(stdout);

    TemplateCaps video_raw{{MediaType::VIDEO_RAW}};
    TemplateCaps audio_raw{{MediaType::AUDIO_RAW}};
    TemplateCaps both{{MediaType::VIDEO_RAW, MediaType::AUDIO_RAW}};
    assert(video_raw.isCompatibleWith(video_raw));
    assert(!video_raw.isCompatibleWith(audio_raw));
    assert(both.isCompatibleWith(video_raw));
    assert(both.isCompatibleWith(audio_raw));
    printf(" OK\n");
}

void test_buffer_ref_lifecycle() {
    printf("  test_buffer_ref_lifecycle...");
    fflush(stdout);

    BufferRef original = makeBuffer(MediaType::VIDEO_RAW, 17, 4);
    const Buffer* raw = original.get();
    assert(raw->ref_count.load() == 1);
    {
        BufferRef shared = original;
        assert(raw->ref_count.load() == 2);
        BufferRef cloned = original.clone();
        assert(cloned.get() != raw);
        assert(cloned->size == original->size);
        assert(memcmp(cloned->data, original->data, original->size) == 0);
    }
    assert(raw->ref_count.load() == 1);
    printf(" OK\n");
}

void test_channel_layout_value_semantics() {
    printf("  test_channel_layout_value_semantics...");
    fflush(stdout);

    ChannelLayout stereo = ChannelLayout::stereo();
    assert(stereo.isValid());
    AVChannelLayout ffmpeg_layout{};
    assert(stereo.toAV(&ffmpeg_layout));
    AVChannelLayout expected = AV_CHANNEL_LAYOUT_STEREO;
    assert(av_channel_layout_compare(&ffmpeg_layout, &expected) == 0);
    av_channel_layout_uninit(&expected);

    ChannelLayout copied;
    assert(ChannelLayout::fromAV(ffmpeg_layout, &copied));
    assert(copied == stereo);
    av_channel_layout_uninit(&ffmpeg_layout);
    printf(" OK\n");
}

void test_caps_format_comparison_excludes_framerate() {
    printf("  test_caps_format_comparison_excludes_framerate...");
    fflush(stdout);

    CapsEvent raw = makeVideoCaps();
    CapsEvent raw_with_new_timing = raw;
    raw_with_new_timing.framerate = AVRational{25, 1};
    assert(raw.hasSameFormat(raw_with_new_timing));
    raw_with_new_timing.width = 32;
    assert(!raw.hasSameFormat(raw_with_new_timing));

    CapsEvent encoded = makeEncodedVideoCaps();
    CapsEvent encoded_with_new_timing = encoded;
    encoded_with_new_timing.framerate = AVRational{25, 1};
    assert(encoded.hasSameFormat(encoded_with_new_timing));
    encoded_with_new_timing.height = 16;
    assert(!encoded.hasSameFormat(encoded_with_new_timing));
    printf(" OK\n");
}

void test_buffer_metadata_is_frame_scoped() {
    printf("  test_buffer_metadata_is_frame_scoped...");
    fflush(stdout);

    uint8_t packet_data[2] = {1, 2};
    AVPacket packet{};
    packet.data = packet_data;
    packet.size = sizeof(packet_data);
    packet.flags = AV_PKT_FLAG_KEY;
    BufferRef encoded(Buffer::fromAVPacket(&packet, MediaType::VIDEO_ENCODED,
                                           AVRational{1, 1000}, AV_CODEC_ID_H264));
    assert(encoded);
    const auto& encoded_meta = std::get<EncodedMeta>(encoded->meta);
    assert(encoded_meta.flags == AV_PKT_FLAG_KEY);

    AVFrame* audio = av_frame_alloc();
    assert(audio);
    uint8_t audio_data[16]{};
    audio->format = AV_SAMPLE_FMT_S16;
    audio->sample_rate = 48000;
    audio->ch_layout = AV_CHANNEL_LAYOUT_STEREO;
    audio->nb_samples = 4;
    audio->data[0] = audio_data;
    BufferRef raw(Buffer::fromAVFrame(audio, MediaType::AUDIO_RAW, AVRational{1, 48000}));
    assert(raw);
    assert(std::get<AudioRawMeta>(raw->meta).nb_samples == 4);
    av_frame_free(&audio);
    printf(" OK\n");
}

void test_output_route_shared_delivery() {
    printf("  test_output_route_shared_delivery...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(2);
    auto first = route->subscribe();
    auto second = route->subscribe();
    assert(route->seal());

    BufferRef source = makeBuffer(MediaType::VIDEO_RAW, 42);
    const Buffer* original = source.get();
    assert(route->publishBlocking(QueueItem{source}) == RoutePublishResult::PUBLISHED);

    auto first_delivery = first.acquireBlocking();
    auto second_delivery = second.acquireBlocking();
    assert(first_delivery && second_delivery);
    assert(std::get<BufferRef>(first_delivery->item()).get() == original);
    assert(std::get<BufferRef>(second_delivery->item()).get() == original);
    assert(first_delivery->ack());
    assert(second_delivery->ack());
    assert(route->retainedItems() == 0);
    printf(" OK\n");
}

void test_output_route_cancel_wakes_publisher() {
    printf("  test_output_route_cancel_wakes_publisher...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(1);
    auto subscription = route->subscribe();
    assert(route->seal());
    assert(route->publishBlocking(QueueItem{makeBuffer(MediaType::VIDEO_RAW)}) ==
           RoutePublishResult::PUBLISHED);

    std::atomic<RoutePublishResult> result{RoutePublishResult::PUBLISHED};
    std::thread publisher([&] {
        result = route->publishBlocking(QueueItem{makeBuffer(MediaType::VIDEO_RAW)});
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    route->cancel();
    publisher.join();
    assert(result == RoutePublishResult::CANCELLED);
    assert(!subscription.acquireBlocking());
    printf(" OK\n");
}

void test_output_route_ack_controls_backpressure() {
    printf("  test_output_route_ack_controls_backpressure...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(1);
    auto fast = route->subscribe();
    auto slow = route->subscribe();
    assert(route->seal());
    assert(route->publishBlocking(QueueItem{makeBuffer(MediaType::VIDEO_RAW, 1)}) ==
           RoutePublishResult::PUBLISHED);

    auto fast_delivery = fast.acquireBlocking();
    auto slow_delivery = slow.acquireBlocking();
    assert(fast_delivery && slow_delivery);
    assert(fast_delivery->ack());

    std::atomic<bool> published{false};
    std::thread publisher([&] {
        assert(route->publishBlocking(QueueItem{makeBuffer(MediaType::VIDEO_RAW, 2)}) ==
               RoutePublishResult::PUBLISHED);
        published = true;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(!published.load());
    assert(slow_delivery->ack());
    publisher.join();
    assert(published.load());

    auto fast_second = fast.acquireBlocking();
    auto slow_second = slow.acquireBlocking();
    assert(fast_second && slow_second);
    assert(fast_second->ack());
    assert(slow_second->ack());
    printf(" OK\n");
}

void test_output_route_delivery_abandon_retries() {
    printf("  test_output_route_delivery_abandon_retries...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(1);
    auto subscription = route->subscribe();
    assert(route->seal());
    assert(route->publishBlocking(QueueItem{makeBuffer(MediaType::VIDEO_RAW, 7)}) ==
           RoutePublishResult::PUBLISHED);

    const Buffer* first = nullptr;
    {
        auto delivery = subscription.acquireBlocking();
        assert(delivery);
        first = std::get<BufferRef>(delivery->item()).get();
    }

    auto retry = subscription.acquireBlocking();
    assert(retry);
    assert(std::get<BufferRef>(retry->item()).get() == first);
    assert(retry->ack());
    printf(" OK\n");
}

void test_output_route_event_order() {
    printf("  test_output_route_event_order...");
    fflush(stdout);

    auto route = std::make_shared<OutputRoute>(4);
    auto subscription = route->subscribe();
    assert(route->seal());
    const CapsEvent caps = makeVideoCaps();
    assert(route->publishBlocking(QueueItem{Event{caps}}) == RoutePublishResult::PUBLISHED);
    assert(route->publishBlocking(QueueItem{makeBuffer(MediaType::VIDEO_RAW, 9)}) ==
           RoutePublishResult::PUBLISHED);
    assert(route->publishBlocking(QueueItem{Event{EOSEvent{}}}) == RoutePublishResult::PUBLISHED);

    auto first = subscription.acquireBlocking();
    assert(first && std::holds_alternative<CapsEvent>(std::get<Event>(first->item())));
    assert(first->ack());
    auto second = subscription.acquireBlocking();
    assert(second && std::holds_alternative<BufferRef>(second->item()));
    assert(second->ack());
    auto third = subscription.acquireBlocking();
    assert(third && std::holds_alternative<EOSEvent>(std::get<Event>(third->item())));
    assert(third->ack());
    printf(" OK\n");
}

void test_pipeline_running_caps_and_dynamic_boundary() {
    printf("  test_pipeline_running_caps_and_dynamic_boundary...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(Event{makeVideoCaps(16, 8)});
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW, 1));
    script.emplace_back(Event{makeVideoCaps(32, 8)});
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW, 2));
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW, 3));

    Pipeline pipeline;
    auto* source = pipeline.addNode<CapsScriptSource>("source", std::move(script));
    auto* sink = pipeline.addNode<CapsTrackingSink>("sink");
    assert(pipeline.link(source, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(sink->received() == 3);
    assert((sink->appliedWidths() == std::vector<int>{16, 32}));
    assert((sink->consumedWidths() == std::vector<int>{16, 32, 32}));
    assert(pipeline.lastError().empty());
    printf(" OK\n");
}

void test_transform_preserves_caps_before_buffer() {
    printf("  test_transform_preserves_caps_before_buffer...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(Event{makeVideoCaps()});
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW));

    Pipeline pipeline;
    auto* source = pipeline.addNode<CapsScriptSource>("source", std::move(script));
    auto* transform = pipeline.addNode<ForwardTransform>("transform");
    auto* sink = pipeline.addNode<CapsTrackingSink>("sink");
    assert(pipeline.link(source, "out", transform, "in", MediaType::VIDEO_RAW));
    assert(pipeline.link(transform, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(transform->processed() == 1);
    assert(sink->received() == 1);
    assert((sink->appliedWidths() == std::vector<int>{16}));
    assert(pipeline.lastError().empty());
    printf(" OK\n");
}

void test_buffer_before_caps_is_protocol_error() {
    printf("  test_buffer_before_caps_is_protocol_error...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW));

    Pipeline pipeline;
    auto* source = pipeline.addNode<CapsScriptSource>("source", std::move(script));
    auto* sink = pipeline.addNode<MockSink>("sink");
    assert(pipeline.link(source, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();
    assert(pipeline.lastError().find("before initial CapsEvent") != std::string::npos);
    printf(" OK\n");
}

void test_transform_eos_follows_flush_sequence() {
    printf("  test_transform_eos_follows_flush_sequence...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(Event{makeVideoCaps()});
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW, 1));

    Pipeline pipeline;
    auto* source = pipeline.addNode<CapsScriptSource>("source", std::move(script));
    auto* transform = pipeline.addNode<EosAfterFlushTransform>("transform");
    auto* sink = pipeline.addNode<OrderedVideoSink>("sink");
    assert(pipeline.link(source, "out", transform, "in", MediaType::VIDEO_RAW));
    assert(pipeline.link(transform, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(pipeline.lastError().empty());
    assert((sink->values() == std::vector<uint8_t>{99}));
    assert(sink->sawEOS());
    printf(" OK\n");
}

void test_transform_stop_releases_unpublished_outputs() {
    printf("  test_transform_stop_releases_unpublished_outputs...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(Event{makeVideoCaps()});
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW));

    Pipeline pipeline;
    auto* source = pipeline.addNode<CapsScriptSource>("source", std::move(script));
    auto* transform = pipeline.addNode<StopAfterProduceTransform>("transform");
    auto* sink = pipeline.addNode<MockSink>("sink");
    assert(pipeline.link(source, "out", transform, "in", MediaType::VIDEO_RAW));
    assert(pipeline.link(transform, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    assert(transform->waitUntilOutputsReady());

    pipeline.stop();
    assert(transform->observersAreSoleOwners());
    printf(" OK\n");
}

void test_transform_cancel_releases_partial_outputs() {
    printf("  test_transform_cancel_releases_partial_outputs...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(Event{makeVideoCaps()});
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW));

    Pipeline pipeline;
    auto* source = pipeline.addNode<CapsScriptSource>("source", std::move(script));
    auto* transform = pipeline.addNode<BurstTransform>("transform");
    auto* sink = pipeline.addNode<BlockingSink>("sink");
    assert(pipeline.link(source, "out", transform, "in", MediaType::VIDEO_RAW));
    assert(pipeline.link(transform, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    assert(sink->waitUntilConsuming());
    assert(transform->waitUntilOutputRouteFull());

    pipeline.stop();
    // Route cancel releases published entries; publish failure and unvisited QueueItems remain RAII-owned.
    assert(transform->observersAreSoleOwners());
    printf(" OK\n");
}

void test_pipeline_concurrent_stop() {
    printf("  test_pipeline_concurrent_stop...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(Event{makeVideoCaps()});
    for (int i = 0; i < 100; ++i) {
        script.emplace_back(makeBuffer(MediaType::VIDEO_RAW, static_cast<uint8_t>(i)));
    }

    Pipeline pipeline;
    auto* source = pipeline.addNode<CapsScriptSource>("source", std::move(script));
    auto* sink = pipeline.addNode<CapsTrackingSink>("sink");
    assert(pipeline.link(source, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());

    std::thread first([&] { pipeline.stop(); });
    std::thread second([&] { pipeline.stop(); });
    first.join();
    second.join();
    printf(" OK\n");
}

void test_pipeline_forked_backpressure() {
    printf("  test_pipeline_forked_backpressure...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(Event{makeVideoCaps()});
    for (int index = 0; index < 100; ++index) {
        script.emplace_back(makeBuffer(MediaType::VIDEO_RAW, static_cast<uint8_t>(index)));
    }

    Pipeline pipeline;
    auto* source = pipeline.addNode<CapsScriptSource>("source", std::move(script));
    auto* fast = pipeline.addNode<CapsTrackingSink>("fast");
    auto* slow = pipeline.addNode<SlowVideoSink>("slow", 500);
    assert(pipeline.link(source, "out", fast, "in", MediaType::VIDEO_RAW));
    assert(pipeline.link(source, "out2", slow, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(pipeline.lastError().empty());
    assert(fast->received() == 100);
    assert(slow->received() == 100);
    printf(" OK\n");
}

void test_mux_waits_for_all_initial_caps() {
    printf("  test_mux_waits_for_all_initial_caps...");
    fflush(stdout);

    std::vector<QueueItem> video_script;
    video_script.emplace_back(Event{makeEncodedVideoCaps()});
    video_script.emplace_back(makeBuffer(MediaType::VIDEO_ENCODED, 1));

    std::vector<QueueItem> audio_script;
    audio_script.emplace_back(Event{makeEncodedAudioCaps()});
    audio_script.emplace_back(makeBuffer(MediaType::AUDIO_ENCODED, 2));

    Pipeline pipeline;
    auto* video = pipeline.addNode<CapsScriptSource>("video", std::move(video_script));
    auto* audio = pipeline.addNode<CapsScriptSource>("audio", std::move(audio_script));
    auto* mux = pipeline.addNode<FakeMux>("mux");
    auto* sink = pipeline.addNode<ContainerSink>("sink");
    assert(pipeline.link(video, "out", mux, "video", MediaType::VIDEO_ENCODED));
    assert(pipeline.link(audio, "out", mux, "audio", MediaType::AUDIO_ENCODED));
    assert(pipeline.link(mux, "out_0", sink, "in", MediaType::CONTAINER));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(pipeline.lastError().empty());
    assert(mux->streamCount() == 2);
    assert(sink->received() == 4);  // header + video packet + audio packet + trailer
    printf(" OK\n");
}

void test_pipeline_ready_failure_rolls_back() {
    printf("  test_pipeline_ready_failure_rolls_back...");
    fflush(stdout);

    class FailingSource final : public SourceNode {
    public:
        explicit FailingSource(const std::string& name) : SourceNode(name) {
            addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW}});
        }
        int stopped() const { return stopped_; }
    protected:
        bool onReady() override {
            postMessage(MessageType::ERROR, "expected Ready failure");
            return false;
        }
        void onStop() override { ++stopped_; }
        Buffer* capture() override { return nullptr; }
    private:
        int stopped_ = 0;
    };

    Pipeline pipeline;
    auto* source = pipeline.addNode<FailingSource>("source");
    auto* sink = pipeline.addNode<MockSink>("sink");
    assert(pipeline.link(source, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(!pipeline.play());
    assert(source->stopped() >= 1);
    assert(pipeline.lastError() == "expected Ready failure");
    printf(" OK\n");
}

} // namespace

int main() {
    printf("=== Dynamic Caps Unit Tests ===\n\n");

    printf("[Value and Route Tests]\n");
    test_template_caps_compatibility();
    test_buffer_ref_lifecycle();
    test_channel_layout_value_semantics();
    test_caps_format_comparison_excludes_framerate();
    test_buffer_metadata_is_frame_scoped();
    test_output_route_shared_delivery();
    test_output_route_ack_controls_backpressure();
    test_output_route_delivery_abandon_retries();
    test_output_route_cancel_wakes_publisher();
    test_output_route_event_order();

    printf("\n[Pipeline Caps Tests]\n");
    test_pipeline_running_caps_and_dynamic_boundary();
    test_transform_preserves_caps_before_buffer();
    test_buffer_before_caps_is_protocol_error();
    test_transform_eos_follows_flush_sequence();
    test_transform_stop_releases_unpublished_outputs();
    test_transform_cancel_releases_partial_outputs();
    test_pipeline_concurrent_stop();
    test_pipeline_forked_backpressure();
    test_mux_waits_for_all_initial_caps();
    test_pipeline_ready_failure_rolls_back();

    printf("\n=== All Tests Passed ===\n");
    return 0;
}
