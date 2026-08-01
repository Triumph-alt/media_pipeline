#include "pipeline/core/Pipeline.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Caps.h"
#include "pipeline/nodes/DecodeNode.h"
#include "pipeline/nodes/VideoRenderNode.h"

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

namespace pipeline {

struct DecodeNodeTestAccess {
    static void setContext(DecodeNode& node, AVCodecContext* context) {
        node.ctx_ = context;
        node.is_video_ = true;
    }

    static bool sendPacketAndDrain(DecodeNode& node, AVPacket* packet,
                                   std::vector<QueueItem>& outputs) {
        return node.sendPacketAndDrain(packet, outputs);
    }

    static void releaseContext(DecodeNode& node) {
        avcodec_free_context(&node.ctx_);
    }
};

struct VideoRenderNodeTestAccess {
    static bool configureConversion(VideoRenderNode& node, const CapsEvent& caps) {
        return node.configureConversion(caps);
    }

    static bool convertFrameToYuv420p(VideoRenderNode& node, const Buffer* buffer,
                                      const CapsEvent& caps, uint8_t* output_data[4],
                                      int output_linesize[4]) {
        return node.convertFrameToYuv420p(buffer, caps, output_data, output_linesize);
    }

    static void releaseConversion(VideoRenderNode& node) {
        node.releaseConversion();
    }
};

} // namespace pipeline

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
// CapsScriptProducer 是有限输入的测试夹具，不继承 SourceNode。它刻意模拟文件等有自然
// EOF 的通用上游，以便为 Transform / Sink / Mux 的 EOS 合同提供输入；真实采集
// SourceNode 不具备此语义。
// ===================================================================
class CapsScriptProducer final : public BaseNode {
public:
    CapsScriptProducer(const std::string& name, std::vector<QueueItem> script)
        : BaseNode(name), script_(std::move(script)) {
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW, MediaType::AUDIO_RAW,
                                        MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED}});
    }

    NodeType nodeType() const override { return NodeType::DEMUX; }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    void runLoop() override final {
        for (auto& item : script_) {
            if (stop_requested_.load() || !publishOutputItem(std::move(item))) {
                return;
            }
        }
        if (!stop_requested_.load()) {
            sendEOSDownstream();
        }
    }

    SrcPad* requestSrcPad(const std::string& name, MediaType hint_type) override final {
        const auto& existing = src_pads_[0]->templateCaps();
        if (!existing.contains(hint_type)) {
            return nullptr;
        }
        return addBranchedSrcPad(name, *src_pads_[0]);
    }

private:
    std::vector<QueueItem> script_;
};

// CaptureScriptSource 通过真实 SourceNode::produce() 合同模拟持续采集：脚本项耗尽后
// 等待外部 stop，不会发送自然 EOS。
class CaptureScriptSource final : public SourceNode {
public:
    CaptureScriptSource(const std::string& name, std::vector<QueueItem> script)
        : SourceNode(name), script_(std::move(script)) {
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW, MediaType::AUDIO_RAW,
                                        MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED}});
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    void produce(std::vector<QueueItem>& outputs) override {
        if (next_item_ < script_.size()) {
            outputs.emplace_back(std::move(script_[next_item_]));
            ++next_item_;
            return;
        }

        while (!stop_requested_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

private:
    std::vector<QueueItem> script_;
    size_t next_item_ = 0;
};

// 未声明首个输出 Pad 的具体 Source 不得由 link hint 反向创造能力。
class UndeclaredOutputSource final : public SourceNode {
public:
    explicit UndeclaredOutputSource(const std::string& name) : SourceNode(name) {}

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void produce(std::vector<QueueItem>&) override {}
};

// 普通 Transform 也必须在构造期声明首个输出能力，不能由 requestSrcPad() 临时补造。
class UndeclaredOutputTransform final : public TransformNode {
public:
    explicit UndeclaredOutputTransform(const std::string& name) : TransformNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

protected:
    bool onReady() override { return true; }
    void onStop() override {}
    void process(const Buffer*, std::vector<QueueItem>&) override {}
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
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW}});
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
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW}});
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
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW}});
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

class StartupBarrierSink final : public SinkNode {
public:
    StartupBarrierSink(const std::string& name, bool arrive)
        : SinkNode(name), arrive_(arrive) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
    }

    bool waitUntilBarrierReached() {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(2), [this] { return barrier_reached_; });
    }

    bool released() const { return released_.load(); }

protected:
    bool onReady() override {
        return pipeline_->clock()->registerStartupParticipant();
    }
    void onStop() override {}
    void consume(const Buffer*) override {}

    void runLoop() override {
        if (!arrive_) {
            // 保留已登记但尚未到达的第二席位，直到 Pipeline::stop() 取消整轮 rendezvous。
            while (!stop_requested_.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            barrier_reached_ = true;
        }
        cv_.notify_one();
        released_ = pipeline_->clock()->arriveAndWaitForStartup();
    }

private:
    const bool arrive_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool barrier_reached_ = false;
    std::atomic<bool> released_{true};
};

class BurstTransform final : public TransformNode {
public:
    explicit BurstTransform(const std::string& name) : TransformNode(name) {
        addSinkPad("in", TemplateCaps{{MediaType::VIDEO_RAW}});
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_RAW}});
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

    // 按 writePacket() 实际被调用的顺序记录每个 Buffer 的 dts；测试据此验证 MuxNode
    // 是否真的按全局最小 DTS 调度，而不是按各输入 Buffer 到达 Route 的先后顺序。
    // 只在 waitEOS() 之后读取，与 stream_count_ 一样借助 join 建立的 happens-before。
    const std::vector<int64_t>& writtenDts() const { return written_dts_; }

private:
    bool allocateContext(MuxFormat) override { return true; }
    bool addStream(const CapsEvent&, int* stream_index) override {
        *stream_index = stream_count_++;
        return true;
    }
    bool writeHeader(MuxFormat) override {
        const uint8_t header = 0x48;
        return appendContainerBytes(&header, 1);
    }
    bool writePacket(const Buffer* buf, int) override {
        written_dts_.push_back(buf->dts);
        const uint8_t packet = 0x50;
        return appendContainerBytes(&packet, 1);
    }
    bool writeTrailer() override {
        const uint8_t trailer = 0x54;
        return appendContainerBytes(&trailer, 1);
    }
    void closeContext() override {}

    int stream_count_ = 0;
    std::vector<int64_t> written_dts_;
};

// DelayedScriptProducer 与 CapsScriptProducer 同构，但在指定脚本下标之前额外阻塞一段
// 时间。用于人为制造"某一路输入的 Buffer 比另一路晚到 Route"的时序，验证 MuxNode 在
// Header 建立后是否真的等到每一路都有队首 Buffer 才按全局最小 DTS 选择，而不是让先到
// 的 Buffer 越过尚未出现的更小 DTS。
class DelayedScriptProducer final : public BaseNode {
public:
    DelayedScriptProducer(const std::string& name, std::vector<QueueItem> script,
                          size_t delay_before_index, std::chrono::milliseconds delay)
        : BaseNode(name), script_(std::move(script)),
          delay_before_index_(delay_before_index), delay_(delay) {
        addSrcPad("out", TemplateCaps{{MediaType::VIDEO_ENCODED, MediaType::AUDIO_ENCODED}});
    }

    NodeType nodeType() const override { return NodeType::DEMUX; }

protected:
    bool onReady() override { return true; }
    void onStop() override {}

    void runLoop() override final {
        for (size_t index = 0; index < script_.size(); ++index) {
            if (index == delay_before_index_) {
                std::this_thread::sleep_for(delay_);
            }
            if (stop_requested_.load() || !publishOutputItem(std::move(script_[index]))) {
                return;
            }
        }
        if (!stop_requested_.load()) {
            sendEOSDownstream();
        }
    }

private:
    std::vector<QueueItem> script_;
    size_t delay_before_index_;
    std::chrono::milliseconds delay_;
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

void test_buffer_prefers_best_effort_timestamp() {
    printf("  test_buffer_prefers_best_effort_timestamp...");
    fflush(stdout);

    AVFrame* video = av_frame_alloc();
    assert(video);
    video->format = AV_PIX_FMT_YUV420P;
    video->width = 2;
    video->height = 2;
    assert(av_frame_get_buffer(video, 1) >= 0);
    video->pts = 11;
    video->best_effort_timestamp = 17;
    BufferRef video_buffer(Buffer::fromAVFrame(video, MediaType::VIDEO_RAW, AVRational{1, 1000}));
    assert(video_buffer);
    // best_effort_timestamp 是 Decoder 已完成重排后的展示时间，必须优先于仍存在的原始 PTS。
    assert(video_buffer->pts == 17000);

    video->best_effort_timestamp = AV_NOPTS_VALUE;
    video->pts = 23;
    BufferRef pts_fallback(Buffer::fromAVFrame(video, MediaType::VIDEO_RAW, AVRational{1, 1000}));
    assert(pts_fallback);
    // 某些 codec 不提供 best-effort 时仍保留 frame->pts，而不是把可用时间戳降级为 NOPTS。
    assert(pts_fallback->pts == 23000);
    av_frame_free(&video);

    AVFrame* audio = av_frame_alloc();
    assert(audio);
    audio->format = AV_SAMPLE_FMT_S16;
    audio->sample_rate = 48000;
    audio->ch_layout = AV_CHANNEL_LAYOUT_STEREO;
    audio->nb_samples = 4;
    assert(av_frame_get_buffer(audio, 1) >= 0);
    audio->pts = 29;
    audio->best_effort_timestamp = 31;
    BufferRef audio_buffer(Buffer::fromAVFrame(audio, MediaType::AUDIO_RAW, AVRational{1, 1000}));
    assert(audio_buffer);
    assert(audio_buffer->pts == 31000);
    av_frame_free(&audio);
    printf(" OK\n");
}

void test_decode_send_packet_eagain_drains_and_retries() {
    printf("  test_decode_send_packet_eagain_drains_and_retries...");
    fflush(stdout);

    const AVCodec* decoder = avcodec_find_decoder(AV_CODEC_ID_RAWVIDEO);
    assert(decoder);
    AVCodecContext* context = avcodec_alloc_context3(decoder);
    assert(context);
    context->codec_type = AVMEDIA_TYPE_VIDEO;
    context->codec_id = AV_CODEC_ID_RAWVIDEO;
    context->pix_fmt = AV_PIX_FMT_YUV420P;
    context->width = 2;
    context->height = 2;
    context->coded_width = 2;
    context->coded_height = 2;
    context->pkt_timebase = AVRational{1, 1000};
    assert(avcodec_open2(context, decoder, nullptr) >= 0);

    DecodeNode decoder_node("decode-eagain-test");
    pipeline::DecodeNodeTestAccess::setContext(decoder_node, context);
    AVPacket* packet = av_packet_alloc();
    assert(packet);
    assert(av_new_packet(packet, 6) >= 0);
    // rawvideo 的 2×2 YUV420P payload 恰为 4 个 Y + 1 个 U + 1 个 V 字节。
    memset(packet->data, 128, static_cast<size_t>(packet->size));
    packet->pts = 7;

    std::vector<QueueItem> outputs;
    // 有意连续 send 三次且不在前两次 receive，让第三次进入 FFmpeg 规定的 EAGAIN 分支。
    assert(avcodec_send_packet(context, packet) >= 0);
    assert(avcodec_send_packet(context, packet) >= 0);
    assert(pipeline::DecodeNodeTestAccess::sendPacketAndDrain(decoder_node, packet, outputs));

    size_t output_frames = 0;
    for (const QueueItem& item : outputs) {
        if (std::holds_alternative<BufferRef>(item)) {
            ++output_frames;
        }
    }
    // helper 先 drain 前两帧、再成功重发第三帧并再次 drain，三帧不能因 EAGAIN 丢失。
    assert(output_frames == 3);

    av_packet_free(&packet);
    pipeline::DecodeNodeTestAccess::releaseContext(decoder_node);
    printf(" OK\n");
}

void test_video_render_converts_yuv422p_to_yuv420p() {
    printf("  test_video_render_converts_yuv422p_to_yuv420p...");
    fflush(stdout);

    AVFrame* source = av_frame_alloc();
    assert(source);
    source->format = AV_PIX_FMT_YUV422P;
    source->width = 4;
    source->height = 2;
    assert(av_frame_get_buffer(source, 1) >= 0);

    for (int row = 0; row < source->height; ++row) {
        for (int column = 0; column < source->width; ++column) {
            source->data[0][row * source->linesize[0] + column] =
                static_cast<uint8_t>(16 + row * source->width + column);
        }
        for (int column = 0; column < (source->width + 1) / 2; ++column) {
            // 常量 chroma 让 4:2:2 -> 4:2:0 的垂直抽样结果可精确验证。
            source->data[1][row * source->linesize[1] + column] = 90;
            source->data[2][row * source->linesize[2] + column] = 200;
        }
    }

    BufferRef packed(Buffer::fromAVFrame(source, MediaType::VIDEO_RAW, AVRational{1, 1000}));
    assert(packed);
    av_frame_free(&source);

    CapsEvent caps;
    caps.media_type = MediaType::VIDEO_RAW;
    caps.width = 4;
    caps.height = 2;
    caps.pix_fmt = AV_PIX_FMT_YUV422P;

    VideoRenderNode renderer("conversion-test");
    assert(pipeline::VideoRenderNodeTestAccess::configureConversion(renderer, caps));
    uint8_t* output_data[4]{};
    int output_linesize[4]{};
    assert(pipeline::VideoRenderNodeTestAccess::convertFrameToYuv420p(
        renderer, packed.get(), caps, output_data, output_linesize));

    assert(output_linesize[0] == 4);
    assert(output_linesize[1] == 2);
    assert(output_linesize[2] == 2);
    for (int row = 0; row < 2; ++row) {
        for (int column = 0; column < 4; ++column) {
            assert(output_data[0][row * output_linesize[0] + column] ==
                   static_cast<uint8_t>(16 + row * 4 + column));
        }
    }
    for (int column = 0; column < 2; ++column) {
        assert(output_data[1][column] == 90);
        assert(output_data[2][column] == 200);
    }
    pipeline::VideoRenderNodeTestAccess::releaseConversion(renderer);
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

void test_clock_startup_barrier_two_participants() {
    printf("  test_clock_startup_barrier_two_participants...");
    fflush(stdout);

    Clock clock;
    clock.reset();
    assert(clock.registerStartupParticipant());
    assert(clock.registerStartupParticipant());
    assert(clock.sealStartupParticipants());

    std::atomic<bool> first_started{false};
    std::atomic<bool> first_released{false};
    std::thread first([&] {
        first_started = true;
        first_released = clock.arriveAndWaitForStartup();
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!first_started.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    assert(first_started.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(!first_released.load());

    assert(clock.arriveAndWaitForStartup());
    first.join();
    assert(first_released.load());
    printf(" OK\n");
}

void test_clock_startup_barrier_single_participant() {
    printf("  test_clock_startup_barrier_single_participant...");
    fflush(stdout);

    Clock clock;
    clock.reset();
    assert(clock.registerStartupParticipant());
    assert(clock.sealStartupParticipants());
    assert(clock.arriveAndWaitForStartup());
    printf(" OK\n");
}

void test_clock_startup_barrier_cancel_wakes_waiter() {
    printf("  test_clock_startup_barrier_cancel_wakes_waiter...");
    fflush(stdout);

    Clock clock;
    clock.reset();
    assert(clock.registerStartupParticipant());
    assert(clock.registerStartupParticipant());
    assert(clock.sealStartupParticipants());

    std::atomic<bool> first_started{false};
    std::atomic<bool> first_released{true};
    std::thread first([&] {
        first_started = true;
        first_released = clock.arriveAndWaitForStartup();
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!first_started.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    assert(first_started.load());
    clock.cancelStartupBarrier();
    first.join();
    assert(!first_released.load());
    printf(" OK\n");
}

void test_clock_startup_barrier_withdraw_releases_arrival() {
    printf("  test_clock_startup_barrier_withdraw_releases_arrival...");
    fflush(stdout);

    Clock clock;
    clock.reset();
    assert(clock.registerStartupParticipant());
    assert(clock.registerStartupParticipant());
    assert(clock.sealStartupParticipants());

    bool arrived = false;
    assert(clock.arriveAndWaitForStartupFor(std::chrono::milliseconds(1), arrived) ==
           Clock::StartupBarrierWaitResult::TIMEOUT);
    assert(arrived);
    clock.withdrawStartupParticipant();
    assert(clock.arriveAndWaitForStartupFor(std::chrono::microseconds(0), arrived) ==
           Clock::StartupBarrierWaitResult::RELEASED);
    printf(" OK\n");
}

void test_pipeline_stop_cancels_startup_barrier() {
    printf("  test_pipeline_stop_cancels_startup_barrier...");
    fflush(stdout);

    Pipeline pipeline;
    auto* source = pipeline.addNode<CapsScriptProducer>("source", std::vector<QueueItem>{});
    auto* waiting = pipeline.addNode<StartupBarrierSink>("waiting", true);
    auto* absent = pipeline.addNode<StartupBarrierSink>("absent", false);
    assert(pipeline.link(source, "out", waiting, "in", MediaType::VIDEO_RAW));
    assert(pipeline.link(source, "out2", absent, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    assert(waiting->waitUntilBarrierReached());

    pipeline.stop();
    assert(!waiting->released());
    printf(" OK\n");
}

void test_source_and_transform_require_declared_output_caps() {
    printf("  test_source_and_transform_require_declared_output_caps...");
    fflush(stdout);

    {
        Pipeline pipeline;
        auto* source = pipeline.addNode<UndeclaredOutputSource>("source");
        auto* sink = pipeline.addNode<MockSink>("sink");
        assert(!pipeline.link(source, "out", sink, "in", MediaType::VIDEO_RAW));
    }

    {
        Pipeline pipeline;
        auto* source = pipeline.addNode<CapsScriptProducer>("source", std::vector<QueueItem>{});
        auto* transform = pipeline.addNode<UndeclaredOutputTransform>("transform");
        auto* sink = pipeline.addNode<MockSink>("sink");
        assert(pipeline.link(source, "out", transform, "in", MediaType::VIDEO_RAW));
        assert(!pipeline.link(transform, "out", sink, "in", MediaType::VIDEO_RAW));
    }

    printf(" OK\n");
}

void test_capture_source_stops_without_eos() {
    printf("  test_capture_source_stops_without_eos...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(Event{makeVideoCaps()});
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW, 42));

    Pipeline pipeline;
    auto* source = pipeline.addNode<CaptureScriptSource>("source", std::move(script));
    auto* sink = pipeline.addNode<OrderedVideoSink>("sink");
    assert(pipeline.link(source, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (sink->values().empty() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    assert((sink->values() == std::vector<uint8_t>{42}));

    // 采集脚本耗尽后 Source 仍阻塞等待外部停止；Pipeline cancel 不能被伪装成自然 EOS。
    pipeline.stop();
    assert(!sink->sawEOS());
    assert(pipeline.lastError().empty());
    printf(" OK\n");
}

void test_source_rejects_buffer_before_caps() {
    printf("  test_source_rejects_buffer_before_caps...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(makeBuffer(MediaType::VIDEO_RAW));

    Pipeline pipeline;
    auto* source = pipeline.addNode<CaptureScriptSource>("source", std::move(script));
    auto* sink = pipeline.addNode<MockSink>("sink");
    assert(pipeline.link(source, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(pipeline.lastError().find("SourceNode: Buffer produced before initial CapsEvent") !=
           std::string::npos);
    printf(" OK\n");
}

void test_source_rejects_eos_item() {
    printf("  test_source_rejects_eos_item...");
    fflush(stdout);

    std::vector<QueueItem> script;
    script.emplace_back(Event{EOSEvent{}});

    Pipeline pipeline;
    auto* source = pipeline.addNode<CaptureScriptSource>("source", std::move(script));
    auto* sink = pipeline.addNode<MockSink>("sink");
    assert(pipeline.link(source, "out", sink, "in", MediaType::VIDEO_RAW));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(pipeline.lastError().find("SourceNode: produce must not emit EOSEvent") !=
           std::string::npos);
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
    auto* source = pipeline.addNode<CapsScriptProducer>("source", std::move(script));
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
    auto* source = pipeline.addNode<CapsScriptProducer>("source", std::move(script));
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
    auto* source = pipeline.addNode<CapsScriptProducer>("source", std::move(script));
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
    auto* source = pipeline.addNode<CapsScriptProducer>("source", std::move(script));
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
    auto* source = pipeline.addNode<CapsScriptProducer>("source", std::move(script));
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
    auto* source = pipeline.addNode<CapsScriptProducer>("source", std::move(script));
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
    auto* source = pipeline.addNode<CapsScriptProducer>("source", std::move(script));
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
    auto* source = pipeline.addNode<CapsScriptProducer>("source", std::move(script));
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
    BufferRef video_packet = makeBuffer(MediaType::VIDEO_ENCODED, 1);
    video_packet.mutableGet()->dts = 0;
    video_script.emplace_back(std::move(video_packet));

    std::vector<QueueItem> audio_script;
    audio_script.emplace_back(Event{makeEncodedAudioCaps()});
    BufferRef audio_packet = makeBuffer(MediaType::AUDIO_ENCODED, 2);
    audio_packet.mutableGet()->dts = 1000;
    audio_script.emplace_back(std::move(audio_packet));

    Pipeline pipeline;
    auto* video = pipeline.addNode<CapsScriptProducer>("video", std::move(video_script));
    auto* audio = pipeline.addNode<CapsScriptProducer>("audio", std::move(audio_script));
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

// 验证 MuxNode 在 Header 建立后确实按全局最小 DTS 调度跨输入 Buffer，而不是按 Buffer
// 到达 Route 的先后顺序。video 输入的 Buffer（dts=2000）在 Caps 之后立即发布；audio
// 输入的 Buffer（dts=1000）故意延迟发布，使其在到达时间上晚于 video。若 MuxNode 在
// 某一路暂无候选 Buffer 时就先选择已到达的那一路（本测试要防止的历史 bug），会先写出
// dts=2000 再写出 dts=1000，产生非法的非单调容器时间戳；正确实现必须等两路都有候选
// Buffer 后再比较，从而先写出 dts=1000。
void test_mux_orders_by_global_dts_when_inputs_arrive_out_of_order() {
    printf("  test_mux_orders_by_global_dts_when_inputs_arrive_out_of_order...");
    fflush(stdout);

    std::vector<QueueItem> video_script;
    video_script.emplace_back(Event{makeEncodedVideoCaps()});
    BufferRef video_packet = makeBuffer(MediaType::VIDEO_ENCODED, 1);
    video_packet.mutableGet()->dts = 2000;
    video_script.emplace_back(std::move(video_packet));

    std::vector<QueueItem> audio_script;
    audio_script.emplace_back(Event{makeEncodedAudioCaps()});
    BufferRef audio_packet = makeBuffer(MediaType::AUDIO_ENCODED, 2);
    audio_packet.mutableGet()->dts = 1000;
    audio_script.emplace_back(std::move(audio_packet));

    Pipeline pipeline;
    // video 不延迟：Caps 和 dts=2000 的 Buffer 都立即发布。
    auto* video = pipeline.addNode<CapsScriptProducer>("video", std::move(video_script));
    // audio 的 Caps（脚本下标 0）立即发布，使两路初始 Caps 都能尽快到齐、Header 尽快建立；
    // 只延迟脚本下标 1（dts=1000 的 Buffer），确保它在 Route 上出现时，video 的
    // dts=2000 Buffer 早已在 Header 建立后等在那里。
    auto* audio = pipeline.addNode<DelayedScriptProducer>(
        "audio", std::move(audio_script), /*delay_before_index=*/1,
        std::chrono::milliseconds(50));
    auto* mux = pipeline.addNode<FakeMux>("mux");
    auto* sink = pipeline.addNode<ContainerSink>("sink");
    assert(pipeline.link(video, "out", mux, "video", MediaType::VIDEO_ENCODED));
    assert(pipeline.link(audio, "out", mux, "audio", MediaType::AUDIO_ENCODED));
    assert(pipeline.link(mux, "out_0", sink, "in", MediaType::CONTAINER));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(pipeline.lastError().empty());
    const std::vector<int64_t>& order = mux->writtenDts();
    assert(order.size() == 2);
    // 全局最小 DTS 优先：dts=1000 的 audio Buffer 必须先于 dts=2000 的 video Buffer 写出，
    // 即使 video 的 Buffer 更早到达 Route。
    assert(order[0] == 1000);
    assert(order[1] == 2000);
    printf(" OK\n");
}

// 视频永久不提供 initial Caps，音频持续向已配置路灌包，直到击穿 Mux 全节点
// staging 硬顶。验证“满且无法前进 → 明确 ERROR”，而不是无限等待或静默丢包。
void test_mux_errors_when_staging_full_waiting_for_initial_caps() {
    printf("  test_mux_errors_when_staging_full_waiting_for_initial_caps...");
    fflush(stdout);

    // 超过 MuxNode 全节点 staging 硬顶 512；再留一点余量覆盖 Route handoff
    constexpr int kAudioPackets = 600;

    std::vector<QueueItem> audio_script;
    audio_script.emplace_back(Event{makeEncodedAudioCaps()});
    for (int index = 0; index < kAudioPackets; ++index) {
        BufferRef packet = makeBuffer(MediaType::AUDIO_ENCODED, static_cast<uint8_t>(index & 0xff));
        packet.mutableGet()->dts = static_cast<int64_t>(index) * 1000;
        audio_script.emplace_back(std::move(packet));
    }

    Pipeline pipeline;
    // 视频侧永不发布 Caps/Buffer/EOS，只等外部 stop；保证 Header 永远等不齐
    auto* video = pipeline.addNode<CaptureScriptSource>("video", std::vector<QueueItem>{});
    auto* audio = pipeline.addNode<CapsScriptProducer>("audio", std::move(audio_script));
    auto* mux = pipeline.addNode<FakeMux>("mux");
    auto* sink = pipeline.addNode<ContainerSink>("sink");
    assert(pipeline.link(video, "out", mux, "video", MediaType::VIDEO_ENCODED));
    assert(pipeline.link(audio, "out", mux, "audio", MediaType::AUDIO_ENCODED));
    assert(pipeline.link(mux, "out_0", sink, "in", MediaType::CONTAINER));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    const std::string error = pipeline.lastError();
    assert(!error.empty());
    assert(error.find("input staging full while waiting for initial Caps") != std::string::npos);
    // Header 未建立，下游不应收到任何容器字节
    assert(sink->received() == 0);
    assert(mux->writtenDts().empty());
    printf(" OK\n");
}

// Header 建立后，音频仅 3 个包就 EOS，但视频仍有大量更小/交错 DTS 的 packet 留在
// staging。验证：音频 EOS 不会在其 staging 排空前越过尾包；全部 packet 按全局最小
// DTS 写出；Trailer 只在两路都真正排空后出现。
void test_mux_drains_staging_after_peer_eos_before_trailer() {
    printf("  test_mux_drains_staging_after_peer_eos_before_trailer...");
    fflush(stdout);

    constexpr int kVideoPackets = 20;
    constexpr int kAudioPackets = 3;
    constexpr int kTotalPackets = kVideoPackets + kAudioPackets;

    std::vector<QueueItem> video_script;
    video_script.emplace_back(Event{makeEncodedVideoCaps()});
    // 视频包 DTS：0,100,...,1900，覆盖并延伸到音频结束之后仍有待写项
    for (int index = 0; index < kVideoPackets; ++index) {
        BufferRef packet = makeBuffer(MediaType::VIDEO_ENCODED, static_cast<uint8_t>(index & 0xff));
        packet.mutableGet()->dts = static_cast<int64_t>(index) * 100;
        video_script.emplace_back(std::move(packet));
    }

    std::vector<QueueItem> audio_script;
    audio_script.emplace_back(Event{makeEncodedAudioCaps()});
    // 音频仅 3 包后自然 EOS；DTS 插在视频序列中间，迫使 EOS 时视频 staging 仍可能非空
    for (int index = 0; index < kAudioPackets; ++index) {
        BufferRef packet = makeBuffer(MediaType::AUDIO_ENCODED, static_cast<uint8_t>(0xa0 + index));
        packet.mutableGet()->dts = 50 + static_cast<int64_t>(index) * 100;
        audio_script.emplace_back(std::move(packet));
    }

    Pipeline pipeline;
    auto* video = pipeline.addNode<CapsScriptProducer>("video", std::move(video_script));
    auto* audio = pipeline.addNode<CapsScriptProducer>("audio", std::move(audio_script));
    auto* mux = pipeline.addNode<FakeMux>("mux");
    auto* sink = pipeline.addNode<ContainerSink>("sink");
    assert(pipeline.link(video, "out", mux, "video", MediaType::VIDEO_ENCODED));
    assert(pipeline.link(audio, "out", mux, "audio", MediaType::AUDIO_ENCODED));
    assert(pipeline.link(mux, "out_0", sink, "in", MediaType::CONTAINER));
    assert(pipeline.build());
    assert(pipeline.play());
    pipeline.waitEOS();

    assert(pipeline.lastError().empty());
    const std::vector<int64_t>& order = mux->writtenDts();
    assert(static_cast<int>(order.size()) == kTotalPackets);

    // 全局 DTS 必须严格单调非降：证明 peer EOS 后仍按完整候选集排序，没有提前踢路
    for (size_t index = 1; index < order.size(); ++index) {
        assert(order[index] >= order[index - 1]);
    }

    // 期望交织序列：0(v),50(a),100(v),150(a),200(v),250(a),300(v)...1900(v)
    assert(order[0] == 0);
    assert(order[1] == 50);
    assert(order[2] == 100);
    assert(order[3] == 150);
    assert(order[4] == 200);
    assert(order[5] == 250);
    assert(order.back() == static_cast<int64_t>(kVideoPackets - 1) * 100);

    // header + 全部 packet + trailer
    assert(sink->received() == kTotalPackets + 2);
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
        void produce(std::vector<QueueItem>&) override {}
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
    test_buffer_prefers_best_effort_timestamp();
    test_decode_send_packet_eagain_drains_and_retries();
    test_video_render_converts_yuv422p_to_yuv420p();
    test_output_route_shared_delivery();
    test_output_route_ack_controls_backpressure();
    test_output_route_delivery_abandon_retries();
    test_output_route_cancel_wakes_publisher();
    test_output_route_event_order();
    test_clock_startup_barrier_two_participants();
    test_clock_startup_barrier_single_participant();
    test_clock_startup_barrier_cancel_wakes_waiter();
    test_clock_startup_barrier_withdraw_releases_arrival();

    printf("\n[Pipeline Caps Tests]\n");
    test_source_and_transform_require_declared_output_caps();
    test_capture_source_stops_without_eos();
    test_source_rejects_buffer_before_caps();
    test_source_rejects_eos_item();
    test_pipeline_running_caps_and_dynamic_boundary();
    test_transform_preserves_caps_before_buffer();
    test_buffer_before_caps_is_protocol_error();
    test_transform_eos_follows_flush_sequence();
    test_transform_stop_releases_unpublished_outputs();
    test_transform_cancel_releases_partial_outputs();
    test_pipeline_concurrent_stop();
    test_pipeline_stop_cancels_startup_barrier();
    test_pipeline_forked_backpressure();
    test_mux_waits_for_all_initial_caps();
    test_mux_orders_by_global_dts_when_inputs_arrive_out_of_order();
    test_mux_errors_when_staging_full_waiting_for_initial_caps();
    test_mux_drains_staging_after_peer_eos_before_trailer();
    test_pipeline_ready_failure_rolls_back();

    printf("\n=== All Tests Passed ===\n");
    return 0;
}
