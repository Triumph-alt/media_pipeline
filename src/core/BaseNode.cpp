#include "pipeline/core/BaseNode.h"
#include "pipeline/core/Edge.h"
#include "pipeline/core/Pipeline.h"

#include <algorithm>

namespace pipeline {

// ===================================================================
// BaseNode: Pad 访问
// ===================================================================
SrcPad* BaseNode::getSrcPad(const std::string& name) {
    for (auto& pad : src_pads_) {
        if (pad->name() == name) {
            return pad.get();
        }
    }
    return nullptr;
}

SinkPad* BaseNode::getSinkPad(const std::string& name) {
    for (auto& pad : sink_pads_) {
        if (pad->name() == name) {
            return pad.get();
        }
    }
    return nullptr;
}

// ===================================================================
// BaseNode: Pad 管理
// ===================================================================
SrcPad* BaseNode::addSrcPad(const std::string& name, TemplateCaps caps) {
    auto pad = std::make_unique<SrcPad>(name, this, std::move(caps));
    auto* ptr = pad.get();
    src_pads_.push_back(std::move(pad));
    return ptr;
}

SinkPad* BaseNode::addSinkPad(const std::string& name, TemplateCaps caps) {
    auto pad = std::make_unique<SinkPad>(name, this, std::move(caps));
    auto* ptr = pad.get();
    sink_pads_.push_back(std::move(pad));
    return ptr;
}

bool BaseNode::releaseSrcPad(SrcPad* pad) {
    if (!pad || pad->isConnected()) {
        return false;
    }

    auto it = std::find_if(src_pads_.begin(), src_pads_.end(),
                           [pad](const auto& candidate) { return candidate.get() == pad; });
    if (it == src_pads_.end()) {
        return false;
    }

    src_pads_.erase(it);
    return true;
}

bool BaseNode::releaseSinkPad(SinkPad* pad) {
    if (!pad || pad->isConnected()) {
        return false;
    }

    auto it = std::find_if(sink_pads_.begin(), sink_pads_.end(),
                           [pad](const auto& candidate) { return candidate.get() == pad; });
    if (it == sink_pads_.end()) {
        return false;
    }

    sink_pads_.erase(it);
    return true;
}

// ===================================================================
// BaseNode: 数据分发
// ===================================================================
void BaseNode::pushToDownstream(Buffer* buf, const std::string& src_pad_name) {
    if (!buf) return;
    BufferRef primary(buf);   // RAII 接管，从这一行起 buf 只由 primary 拥有

    if (!src_pad_name.empty()) {
        // 推给指定 SrcPad（DemuxNode 按流类型分发）
        SrcPad* pad = getSrcPad(src_pad_name);
        if (!pad || !pad->isConnected()) return;   // primary 析构自动 unref
        // 阻塞策略取决于节点总 SrcPad 数量
        if (src_pads_.size() == 1)
            pad->pushBlocking(QueueItem{std::move(primary)});
        else
            pad->tryPush(QueueItem{std::move(primary)});
        return;
    }

    // 推给所有 SrcPad
    if (src_pads_.empty()) return;   // primary 析构自动 unref

    if (src_pads_.size() == 1) {
        // 单路：阻塞 push，背压传导
        if (src_pads_[0]->isConnected())
            src_pads_[0]->pushBlocking(QueueItem{std::move(primary)});
        return;   // 未连接或已 move：primary 析构自动 unref
    }

    // 多路（分叉）：每路独立 clone 深拷贝，非阻塞 tryPush，满则丢弃该路副本
    // primary 始终持有原 buf，各路只拿 clone 互不影响；出作用域时统一 unref 原 buf
    for (auto& pad : src_pads_) {
        if (!pad->isConnected()) continue;
        pad->tryPush(QueueItem{primary.clone()});
    }
}

void BaseNode::sendEOSDownstream() {
    for (auto& pad : src_pads_) {
        if (!pad->isConnected()) {
            continue;
        }

        pad->pushBlocking(QueueItem{Event{EOSEvent{}}});
    }
}

bool BaseNode::sendCapsEvent(const std::string& src_pad_name, const CapsEvent& caps) {
    SrcPad* pad = getSrcPad(src_pad_name);
    if (!pad || !pad->isConnected()) {
        postMessage(MessageType::ERROR,
                    "sendCapsEvent: src pad '" + src_pad_name + "' not found or not connected");
        return false;
    }

    // 校验 CapsEvent.media_type 落在 SrcPad 的能力集合内
    if (!pad->templateCaps().contains(caps.media_type)) {
        postMessage(MessageType::ERROR,
                    "sendCapsEvent: caps.media_type not in src pad '" + src_pad_name + "' template caps");
        return false;
    }

    // 校验通过就 setActualType（唯一合法调用点），阻塞 push（CapsEvent 不允许丢失）
    pad->setActualType(caps.media_type);
    pad->pushBlocking(QueueItem{Event{caps}});
    return true;
}

bool BaseNode::receiveCapsEvent(const std::string& sink_pad_name) {
    SinkPad* pad = getSinkPad(sink_pad_name);
    if (!pad || !pad->isConnected()) {
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: sink pad '" + sink_pad_name + "' not found or not connected");
        return false;
    }

    auto item = pad->popBlocking();
    if (!item) {
        // Ready 期间被外部 flush，属异常
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: queue flushed, no CapsEvent on pad '" + sink_pad_name + "'");
        return false;
    }

    if (!std::holds_alternative<Event>(*item)) {
        // 取到的是 Buffer，视为上游 CapsEvent 未先行发送
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: expected CapsEvent on pad '" + sink_pad_name + "', got Buffer");
        return false;
    }

    const Event& event = std::get<Event>(*item);
    if (!std::holds_alternative<CapsEvent>(event)) {
        // 取到 Event 但不是 CapsEvent（如 EOS），视为上游误发
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: expected CapsEvent on pad '" + sink_pad_name + "', got other Event");
        return false;
    }

    const CapsEvent& caps = std::get<CapsEvent>(event);

    // 校验 CapsEvent.media_type 落在 SinkPad 的能力集合内
    if (!pad->templateCaps().contains(caps.media_type)) {
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: caps.media_type not in sink pad '" + sink_pad_name + "' template caps");
        return false;
    }

    // 校验通过：setActualType（唯一合法调用点），存入 negotiated_caps_
    pad->setActualType(caps.media_type);
    negotiated_caps_[sink_pad_name] = caps;
    return true;
}

// ===================================================================
// BaseNode: 消息上报
// ===================================================================
void BaseNode::postMessage(MessageType type, const std::string& text, int code) {
    if (pipeline_) {
        pipeline_->bus()->post({type, this, text, code});
    }
    // ERROR 时设置自己退出
    if (type == MessageType::ERROR) {
        stop_requested_.store(true);
    }
}

// ===================================================================
// SourceNode: runLoop
// ===================================================================

void SourceNode::runLoop() {
    while (!stop_requested_.load()) {
        auto* buf = capture();
        if (!buf) {
            // EOF：发送 EOS 给下游，SinkNode 收到后才上报 Pipeline
            sendEOSDownstream();
            break;
        }
        pushToDownstream(buf);
    }
}

// ===================================================================
// SinkNode: runLoop
// ===================================================================

void SinkNode::runLoop() {
    auto* sink_pad = sink_pads_[0].get();
    while (!stop_requested_.load()) {
        auto item = sink_pad->popBlocking();
        if (!item) {
            // flush 唤醒，检查 stop_requested_
            break;
        }

        if (std::holds_alternative<BufferRef>(*item)) {
            consume(std::get<BufferRef>(*item).get());
        } else if (std::holds_alternative<Event>(*item)) {
            onEvent(std::get<Event>(*item));
        }
    }
}

void SinkNode::onEvent(const Event& event) {
    if (std::holds_alternative<EOSEvent>(event)) {
        postMessage(MessageType::EOS, "");
        return;
    }

    postMessage(MessageType::ERROR,
                "CapsEvent received in runLoop; sink nodes must consume CapsEvent in onStreamInfo");
}

// ===================================================================
// TransformNode: runLoop
// ===================================================================

void TransformNode::runLoop() {
    auto* sink_pad = sink_pads_[0].get();
    std::vector<Buffer*> outputs;
    while (!stop_requested_.load()) {
        auto item = sink_pad->popBlocking();
        if (!item) break;

        if (std::holds_alternative<BufferRef>(*item)) {
            outputs.clear();
            process(std::get<BufferRef>(*item).get(), outputs);
            for (auto* out : outputs) {
                pushToDownstream(out);
            }
        } else if (std::holds_alternative<Event>(*item)) {
            onEvent(std::get<Event>(*item));
        }
    }
}

void TransformNode::onEvent(const Event& event) {
    if (std::holds_alternative<EOSEvent>(event)) {
        sendEOSDownstream();
        return;
    }

    postMessage(MessageType::ERROR,
                "CapsEvent received in runLoop; transform nodes must consume CapsEvent in onStreamInfo");
}

// ===================================================================
// DemuxNode: 格式无关的共享骨架
// ===================================================================

SrcPad* DemuxNode::requestSrcPad(const std::string& name, MediaType hint_type) {
    if (hint_type != MediaType::VIDEO_ENCODED && hint_type != MediaType::AUDIO_ENCODED) {
        return nullptr;
    }
    auto* pad = addSrcPad(name, TemplateCaps{{hint_type}});
    pad_to_type_[name] = hint_type;
    return pad;
}

bool DemuxNode::releaseSrcPad(SrcPad* pad) {
    if (!pad || pad->isConnected()) {
        return false;
    }

    const std::string pad_name = pad->name();
    if (!BaseNode::releaseSrcPad(pad)) {
        return false;
    }

    pad_to_type_.erase(pad_name);
    return true;
}

bool DemuxNode::onReady() {
    if (!openInput(url_)) {
        return false;
    }

    DemuxProbeResult result;
    if (!probeStreams(&result)) {
        return false;
    }

    // 具体类只负责探测；基类验证返回值后才写入正式状态。
    if (result.video && result.video->media_type != MediaType::VIDEO_ENCODED) {
        postMessage(MessageType::ERROR,
                    "DemuxNode: probeStreams returned non-video caps as video result");
        return false;
    }
    if (result.audio && result.audio->media_type != MediaType::AUDIO_ENCODED) {
        postMessage(MessageType::ERROR,
                    "DemuxNode: probeStreams returned non-audio caps as audio result");
        return false;
    }

    // 校验用户请求：nullopt 表示探测成功，但输入确实没有该类型。
    for (const auto& [pad_name, type] : pad_to_type_) {
        const bool found =
            (type == MediaType::VIDEO_ENCODED && result.video.has_value()) ||
            (type == MediaType::AUDIO_ENCODED && result.audio.has_value());
        if (!found) {
            postMessage(MessageType::ERROR,
                        "DemuxNode: no stream matching pad '" + pad_name + "'");
            return false;
        }
    }

    probe_result_ = std::move(result);
    return true;
}

bool DemuxNode::onStreamInfo() {
    for (const auto& [pad_name, type] : pad_to_type_) {
        SrcPad* pad = getSrcPad(pad_name);
        if (!pad || !pad->isConnected()) {
            continue;
        }

        const CapsEvent* caps = nullptr;
        if (type == MediaType::VIDEO_ENCODED && probe_result_.video) {
            caps = &*probe_result_.video;
        } else if (type == MediaType::AUDIO_ENCODED && probe_result_.audio) {
            caps = &*probe_result_.audio;
        }
        if (!caps) {
            postMessage(MessageType::ERROR,
                        "DemuxNode: missing probed caps for pad '" + pad_name + "'");
            return false;
        }

        // 先 resize 到正确容量，再发 CapsEvent
        // sendCapsEvent 内部校验 caps.media_type ∈ SrcPad.templateCaps，通过后 setActualType
        pad->edge()->queue->resize(selectQueueCapacity(type));
        if (!sendCapsEvent(pad_name, *caps)) {
            return false;   // sendCapsEvent 已 postMessage(ERROR)，触发 Ready 回滚
        }
    }
    return true;
}

void DemuxNode::runLoop() {
    while (!stop_requested_.load()) {
        DemuxReadResult result = readFrame();

        if (result.status == DemuxReadStatus::END_OF_STREAM) {
            if (result.buffer) {
                postMessage(MessageType::ERROR,
                            "DemuxNode: readFrame returned END_OF_STREAM with a buffer");
                return;
            }
            sendEOSDownstream();
            return;
        }

        if (result.status == DemuxReadStatus::CANCELLED) {
            if (result.buffer) {
                postMessage(MessageType::ERROR,
                            "DemuxNode: readFrame returned CANCELLED with a buffer");
            }
            return;
        }

        if (result.status == DemuxReadStatus::ERROR) {
            // 具体类负责上报后端错误；异常携带的 BufferRef 会自动释放。
            return;
        }

        if (!result.buffer) {
            postMessage(MessageType::ERROR,
                        "DemuxNode: readFrame returned BUFFER without a buffer");
            return;
        }

        BufferRef primary = std::move(result.buffer);

        // 按 media_type 分发到所有匹配的已连接 SrcPad，判断 pad 是否要收此帧一律用 pad->actualType()
        // 单路匹配：直接 pushBlocking，背压传导，不丢帧。
        for (auto& pad : src_pads_) {
            if (!pad->isConnected()) {
                continue;
            }
            auto actual = pad->actualType();
            if (!actual || *actual != primary->media_type) {
                continue;
            }
            pad->pushBlocking(QueueItem{std::move(primary)});
            break;  // 单路匹配后 primary 已移走，退出循环
        }
    }
}

void DemuxNode::onStop() {
    closeInput();
}

// ===================================================================
// MuxNode: 格式无关的共享骨架
// ===================================================================

SinkPad* MuxNode::requestSinkPad(const std::string& name, MediaType hint_type) {
    if (hint_type != MediaType::VIDEO_ENCODED && hint_type != MediaType::AUDIO_ENCODED) {
        return nullptr;
    }
    return addSinkPad(name, TemplateCaps{{hint_type}});
}

bool MuxNode::appendContainerBytes(const uint8_t* data, size_t size) {
    if (size == 0) {
        return true;
    }
    if (!data) {
        postMessage(MessageType::ERROR, "MuxNode: null container data");
        return false;
    }

    pending_output_.insert(pending_output_.end(), data, data + size);
    return true;
}

void MuxNode::flushPendingOutput() {
    if (pending_output_.empty()) {
        return;
    }

    auto* output = new Buffer();
    output->data = new uint8_t[pending_output_.size()];
    output->size = pending_output_.size();
    std::copy(pending_output_.begin(), pending_output_.end(), output->data);
    output->media_type = MediaType::CONTAINER;
    pending_output_.clear();

    // MuxNode 当前固定单输出，pushToDownstream 使用阻塞 push，容器字节不丢失。
    pushToDownstream(output, "out_0");
}

bool MuxNode::onReady() {
    return true;
}

bool MuxNode::onStreamInfo() {
    pending_output_.clear();
    pad_to_stream_.clear();
    eos_pads_.clear();

    // Mux 进一步在 Ready 阶段拒绝零输入或任何未连接 SinkPad，保证 Running 后所有输入都可用
    if (sink_pads_.empty()) {
        postMessage(MessageType::ERROR, "MuxNode: no input pad");
        return false;
    }
    for (auto& pad : sink_pads_) {
        if (!pad->isConnected()) {
            postMessage(MessageType::ERROR,
                        "MuxNode: sink pad '" + pad->name() + "' is not connected");
            return false;
        }
    }

    if (!allocateContext(format_)) {
        return false;   // 具体类负责上报格式后端错误
    }

    // 从每个 SinkPad 收取 CapsEvent，并添加输出流。
    for (auto& pad : sink_pads_) {
        if (!receiveCapsEvent(pad->name())) {
            return false;   // receiveCapsEvent 已上报框架错误
        }

        const CapsEvent& caps = negotiated_caps_[pad->name()];
        int stream_index = -1;
        if (!addStream(caps, &stream_index)) {
            return false;   // 具体类负责上报格式后端错误
        }
        pad_to_stream_[pad->name()] = stream_index;

        // 任意一路有数据时唤醒 mux_cv_。
        pad->edge()->queue->setExternalNotify([this]() {
            std::lock_guard<std::mutex> lock(mux_mutex_);
            mux_cv_.notify_one();
        });
    }

    // 固定输出 Queue 已由 Graph 创建；先确定容量并发送 CONTAINER Caps。
    auto* output_pad = getSrcPad("out_0");
    if (!output_pad || !output_pad->isConnected() || !output_pad->edge()->queue) {
        postMessage(MessageType::ERROR, "MuxNode: output pad 'out_0' is not connected");
        return false;
    }
    output_pad->edge()->queue->resize(selectQueueCapacity(MediaType::CONTAINER));

    CapsEvent output_caps;
    output_caps.media_type = MediaType::CONTAINER;
    if (!sendCapsEvent("out_0", output_caps)) {
        return false;
    }

    // Header 在 Ready 阶段生成以保留初始化失败语义；
    // AVIO callback 只能把字节追加到 pending_output_，真正的阻塞 push 延迟到 runLoop 开始后
    if (!writeHeader()) {
        pending_output_.clear();
        return false;   // 具体类负责上报格式后端错误
    }

    return true;
}

void MuxNode::runLoop() {
    // 下游线程此时已经启动，可以安全阻塞发送 Ready 阶段暂存的 Header 字节。
    flushPendingOutput();

    while (!stop_requested_.load()) {
        SinkPad* ready_pad = waitAnyPadReady();
        if (!ready_pad) {
            break;
        }

        auto item = ready_pad->tryPop();
        if (!item) {
            continue;
        }

        if (std::holds_alternative<BufferRef>(*item)) {
            Buffer* buf = std::get<BufferRef>(*item).get();
            auto it = pad_to_stream_.find(ready_pad->name());
            if (it == pad_to_stream_.end()) {
                postMessage(MessageType::ERROR,
                            "MuxNode: no output stream for pad '" + ready_pad->name() + "'");
                break;
            }

            if (!writePacket(buf, it->second)) {
                pending_output_.clear();
                break;   // 具体类负责上报格式后端错误
            }
            flushPendingOutput();
            continue;
        }

        const Event& event = std::get<Event>(*item);
        if (!std::holds_alternative<EOSEvent>(event)) {
            postMessage(MessageType::ERROR,
                        "CapsEvent received in runLoop; mux nodes must consume CapsEvent in onStreamInfo");
            break;
        }

        eos_pads_.insert(ready_pad->name());
        if (eos_pads_.size() != sink_pads_.size()) {
            continue;
        }

        if (!writeTrailer()) {
            pending_output_.clear();
            break;   // 具体类负责上报格式后端错误
        }
        flushPendingOutput();
        sendEOSDownstream();
        break;
    }
}

SinkPad* MuxNode::waitAnyPadReady() {
    std::unique_lock<std::mutex> lock(mux_mutex_);
    mux_cv_.wait(lock, [this] {
        if (stop_requested_.load()) {
            return true;
        }
        for (auto& pad : sink_pads_) {
            if (pad->isConnected() && pad->edge()->queue && !pad->edge()->queue->empty()) {
                return true;
            }
        }
        return false;
    });

    if (stop_requested_.load()) {
        return nullptr;
    }

    return selectMinDtsPad();
}

SinkPad* MuxNode::selectMinDtsPad() {
    SinkPad* min_pad = nullptr;
    int64_t min_dts = std::numeric_limits<int64_t>::max();

    for (auto& pad : sink_pads_) {
        if (!pad->isConnected() || !pad->edge()->queue) {
            continue;
        }

        auto top = pad->edge()->queue->peek();
        if (!top) {
            continue;
        }

        if (std::holds_alternative<Event>(*top)) {
            // Event（EOS 等）优先处理，不参与 DTS 比较
            return pad.get();
        }

        int64_t dts = std::get<BufferRef>(*top)->dts;
        if (dts < min_dts) {
            min_dts = dts;
            min_pad = pad.get();
        }
    }

    return min_pad;
}

void MuxNode::onStop() {
    pending_output_.clear();
    closeContext();
}

} // namespace pipeline
