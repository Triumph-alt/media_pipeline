#include "pipeline/core/BaseNode.h"
#include "pipeline/core/Edge.h"
#include "pipeline/core/Pipeline.h"

#include <algorithm>
#include <utility>

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
    // 首个输出 Pad 创建新逻辑 Route；临时容量只用于 Ready 前的 Caps 协商。
    auto route = std::make_shared<OutputRoute>(8);
    auto pad = std::make_unique<SrcPad>(name, this, std::move(caps), std::move(route));
    auto* ptr = pad.get();
    src_pads_.push_back(std::move(pad));
    return ptr;
}

SrcPad* BaseNode::addBranchedSrcPad(const std::string& name, const SrcPad& source_pad) {
    // 分叉 Pad 不创建第二份缓冲；它通过独立 Edge Subscription 订阅 source_pad 的 Route。
    auto pad = std::make_unique<SrcPad>(name, this, source_pad.templateCaps(), source_pad.route());
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
// BaseNode 唯一“向某条 OutputRoute 提交一个有序项目”的总闸门
// ===================================================================
bool BaseNode::publishOutputItem(QueueItem&& item, const std::string& src_pad_name) {
    // 接管QueueItem所有权，保证控制流中的未发布 BufferRef 始终由 RAII 管理
    QueueItem primary(std::move(item));

    // 目标 Route
    std::shared_ptr<OutputRoute> route;
    if (!src_pad_name.empty()) {
        // 如果指定了src_pad_name，直接找到该 SrcPad 所属 Route
        SrcPad* pad = getSrcPad(src_pad_name);
        if (!pad || !pad->isConnected()) {
            // Pad 不存在或没连接就算失败
            return false;
        }
        route = pad->route();
    } else {
        // 未指定 name 则遍历所有已连接 SrcPad，这就要求它们共享同一条逻辑 Route(同源分叉)
        for (const auto& pad : src_pads_) {
            if (!pad->isConnected()) {
                continue;
            }
            if (!route) {
                route = pad->route();
            } else if (route.get() != pad->route().get()) {
                // 如果发现已连接 SrcPad 指向不同 Route，无法判断发给谁，直接报错
                postMessage(MessageType::ERROR,
                            "publishOutputItem: ambiguous logical output route");
                return false;
            }
        }
    }

    if (!route) {
        return false;
    }

    // 如果 QueueItem 是 Buffer，则发布 Buffer
    if (std::holds_alternative<BufferRef>(primary)) {
        // 先检查不是空引用
        if (!std::get<BufferRef>(primary)) {
            return false;
        }
        // 直接可靠发布
        return route->publishBlocking(std::move(primary)) == RoutePublishResult::PUBLISHED;
    }

    // 不是 Buffer，那就是 Event
    const Event& event = std::get<Event>(primary);

    // 如果是 EOS Event，同样直接发布
    if (std::holds_alternative<EOSEvent>(event)) {
        return route->publishBlocking(std::move(primary)) == RoutePublishResult::PUBLISHED;
    }

    // 剩下的事件必然就是 CapsEvent
    const CapsEvent& caps = std::get<CapsEvent>(event);

    // 首份 Caps 用 TemplateCaps 选择并固定该逻辑 Route 的 MediaType；同类格式变化只比较
    // 已固定的 actualType，不能把同一条 Route 在 Running 中改成另一种媒体类型。
    bool is_firstcaps = false;
    for (auto& sibling : src_pads_) {
        if (!sibling->isConnected() || sibling->route().get() != route.get()) {
            continue;
        }

        const auto actual_type = sibling->actualType();
        if (actual_type) {
            if (*actual_type != caps.media_type) {
                postMessage(MessageType::ERROR,
                            "publishOutputItem: CapsEvent changes fixed MediaType on shared output route");
                return false;
            }
            continue;
        }

        if (!sibling->templateCaps().contains(caps.media_type)) {
            postMessage(MessageType::ERROR,
                        "publishOutputItem: caps.media_type not in shared route pad template caps");
            return false;
        }
        is_firstcaps = true;
        sibling->setActualType(caps.media_type);
    }

    // 首份 Caps 在写入其配置边界前确定该 Route 的条目硬容量。
    // 后续同类格式变化不改变容量；跨 MediaType 变化已在上方拒绝。
    if (is_firstcaps) {
        route->resize(selectRouteCapacity(caps.media_type));
    }
    return route->publishBlocking(std::move(primary)) == RoutePublishResult::PUBLISHED;
}

bool BaseNode::pushToDownstream(BufferRef&& buf, const std::string& src_pad_name) {
    return publishOutputItem(QueueItem{std::move(buf)}, src_pad_name);
}

bool BaseNode::sendEOSDownstream() {
    // Each logical Route gets one EOS even when several SrcPads express a static same-stream branch.
    std::unordered_set<OutputRoute*> published;
    for (const auto& pad : src_pads_) {
        auto route = pad->route();
        if (!pad->isConnected() || !route || !published.insert(route.get()).second) {
            continue;
        }
        if (!publishOutputItem(QueueItem{Event{EOSEvent{}}}, pad->name())) {
            return false;
        }
    }
    return true;
}

bool BaseNode::sendCapsEvent(const std::string& src_pad_name, const CapsEvent& caps) {
    SrcPad* pad = getSrcPad(src_pad_name);
    if (!pad || !pad->isConnected() || !pad->route()) {
        postMessage(MessageType::ERROR,
                    "sendCapsEvent: src pad '" + src_pad_name + "' not found or not connected");
        return false;
    }
    return publishOutputItem(QueueItem{Event{caps}}, src_pad_name);
}

bool BaseNode::applyCapsEvent(const std::string& sink_pad_name, const CapsEvent& caps,
                                  std::vector<QueueItem>* outputs) {
    SinkPad* pad = getSinkPad(sink_pad_name);
    if (!pad || !pad->isConnected()) {
        postMessage(MessageType::ERROR,
                    "applyCapsEvent: sink pad '" + sink_pad_name + "' not found or not connected");
        return false;
    }
    // 首份 Caps 用 TemplateCaps 选择 SinkPad 的实际类型；后续 Caps 只能在已固定的
    // MediaType 内重配格式，不能把同一条输入 Route 改成另一种媒体类型。
    const auto actual_type = pad->actualType();
    if (actual_type) {
        if (*actual_type != caps.media_type) {
            postMessage(MessageType::ERROR,
                        "applyCapsEvent: CapsEvent changes fixed MediaType on sink pad '" +
                            sink_pad_name + "'");
            return false;
        }
    } else if (!pad->templateCaps().contains(caps.media_type)) {
        postMessage(MessageType::ERROR,
                    "applyCapsEvent: caps.media_type not in sink pad '" + sink_pad_name +
                        "' template caps");
        return false;
    }

    // 字段是否足够由具体消费者 onCaps 判断；成功前不更新 active_caps_，也不允许 ack。
    if (!onCaps(sink_pad_name, caps, outputs)) {
        return false;
    }

    if (!actual_type) {
        pad->setActualType(caps.media_type);
    }
    active_caps_[sink_pad_name] = caps;
    return true;
}

// ===================================================================
// BaseNode: 消息上报
// ===================================================================
void BaseNode::postMessage(MessageType type, const std::string& text, int code) {
    if (pipeline_) {
        pipeline_->bus()->post({type, this, text, code});
    }
    // ERROR 和 STOP_REQUESTED 都要求当前节点立即退出工作循环。
    if (type == MessageType::ERROR || type == MessageType::STOP_REQUESTED) {
        stop_requested_.store(true);
    }
}

// ===================================================================
// SourceNode: runLoop
// ===================================================================

void SourceNode::runLoop() {
    while (!stop_requested_.load()) {
        // capture() 返回的新建 Buffer 立即进入 RAII，不能跨控制流保存拥有型裸指针。
        BufferRef buf(capture());
        if (!buf) {
            // EOF：发送 EOS 给下游，SinkNode 收到后才上报 Pipeline
            sendEOSDownstream();
            break;
        }
        if (!pushToDownstream(std::move(buf))) {
            break;
        }
    }
}

// ===================================================================
// SinkNode: runLoop
// ===================================================================

void SinkNode::runLoop() {
    auto* sink_pad = sink_pads_[0].get();
    const std::string& sink_pad_name = sink_pad->name();

    while (!stop_requested_.load()) {
        auto delivery = sink_pad->acquireBlocking();
        if (!delivery) {
            break;
        }

        const QueueItem& item = delivery->item();
        if (std::holds_alternative<BufferRef>(item)) {
            // Buffer 没有 active Caps 就无法被完整解释；这是上游协议错误而非默认值回退。
            const auto active = active_caps_.find(sink_pad_name);
            if (active == active_caps_.end()) {
                postMessage(MessageType::ERROR,
                            "SinkNode: Buffer received before initial CapsEvent on pad '" +
                                sink_pad_name + "'");
                break;
            }
            if (std::get<BufferRef>(item)->media_type != active->second.media_type) {
                postMessage(MessageType::ERROR,
                            "SinkNode: Buffer media type does not match active CapsEvent on pad '" +
                                sink_pad_name + "'");
                break;
            }

            consume(std::get<BufferRef>(item).get());
            if (stop_requested_.load() || !delivery->ack()) {
                break;
            }
            continue;
        }

        const Event& event = std::get<Event>(item);
        if (std::holds_alternative<CapsEvent>(event)) {
            // 重配在当前 Route worker 内串行完成；只有 onCaps 成功后才能提交此格式边界。
            if (!applyCapsEvent(sink_pad_name, std::get<CapsEvent>(event)) || !delivery->ack()) {
                break;
            }
            continue;
        }

        // EOS 先释放输入 Route；输出侧 drain 期间不应占住可靠背压窗口。
        if (!delivery->ack()) {
            break;
        }
        onDrain();
        if (stop_requested_.load()) {
            break;
        }
        postMessage(MessageType::EOS, "");
    }
}

// ===================================================================
// TransformNode: runLoop
// ===================================================================

bool TransformNode::onCaps(const std::string&, const CapsEvent& caps,
                           std::vector<QueueItem>* outputs) {
    // The generic Transform is format-preserving. Specialized transforms such as DecodeNode override this
    // hook and emit their own output Caps at the exact Buffer boundary where their result format is known.
    if (outputs) {
        outputs->emplace_back(Event{caps});
    }
    return true;
}

void TransformNode::runLoop() {
    auto* sink_pad = sink_pads_[0].get();
    const std::string& sink_pad_name = sink_pad->name();
    std::vector<QueueItem> outputs;

    while (!stop_requested_.load()) {
        // 从唯一输入 SinkPad 取得一个 RouteDelivery
        auto delivery = sink_pad->acquireBlocking();
        if (!delivery) {
            break;
        }

        const QueueItem& item = delivery->item();
        if (std::holds_alternative<BufferRef>(item)) {
            // 必须先有 active Caps
            const auto active = active_caps_.find(sink_pad_name);
            if (active == active_caps_.end()) {
                postMessage(MessageType::ERROR,
                            "TransformNode: Buffer received before initial CapsEvent on pad '" +
                                sink_pad_name + "'");
                break;
            }

            // Buffer 类型必须和 active Caps 一致
            if (std::get<BufferRef>(item)->media_type != active->second.media_type) {
                postMessage(MessageType::ERROR,
                            "TransformNode: Buffer media type does not match active CapsEvent on pad '" +
                                sink_pad_name + "'");
                break;
            }

            outputs.clear();
            process(std::get<BufferRef>(item).get(), outputs);

            // 如果 process 内部遇到错误 postMessage() 会置 stop_requested_
            if (stop_requested_.load()) {
                break;
            }

            bool outputs_published = true;
            for (auto& output : outputs) {
                // 同一个移动发布边界承载 Caps 和 Buffer；失败项及未遍历尾项仍受 vector RAII 管理
                if (!publishOutputItem(std::move(output))) {
                    outputs_published = false;
                    break;
                }
            }

            // 确保输出完整发布后 ack 输入 Buffer
            if (!outputs_published || !delivery->ack()) {
                break;
            }
            continue;
        }

        const Event& event = std::get<Event>(item);
        if (std::holds_alternative<CapsEvent>(event)) {
            outputs.clear();
            if (!applyCapsEvent(sink_pad_name, std::get<CapsEvent>(event), &outputs)) {
                break;
            }

            bool outputs_published = true;
            for (auto& output : outputs) {
                if (!publishOutputItem(std::move(output))) {
                    outputs_published = false;
                    break;
                }
            }
            if (!outputs_published || !delivery->ack()) {
                break;
            }
            continue;
        }

        // onEOS may append delayed decoder Caps/Buffers but never EOS. The framework owns the terminator,
        // so every Transform forwards exactly one EOSEvent even if a subclass has no context or was flushed.
        outputs.clear();
        onEOS(outputs);
        if (stop_requested_.load()) {
            break;
        }
        outputs.emplace_back(Event{EOSEvent{}});

        bool outputs_published = true;
        for (auto& output : outputs) {
            if (!publishOutputItem(std::move(output))) {
                outputs_published = false;
                break;
            }
        }
        if (!outputs_published || !delivery->ack()) {
            break;
        }
        break;
    }
}

// ===================================================================
// DemuxNode: 格式无关的共享骨架
// ===================================================================

SrcPad* DemuxNode::requestSrcPad(const std::string& name, MediaType hint_type) {
    if (hint_type != MediaType::VIDEO_ENCODED && hint_type != MediaType::AUDIO_ENCODED) {
        return nullptr;
    }
    auto* source_pad = getSrcPad(name);
    if (!source_pad) {
        for (const auto& existing : src_pads_) {
            auto actual = existing->actualType();
            if ((actual && *actual == hint_type) ||
                (!actual && existing->templateCaps().contains(hint_type))) {
                source_pad = existing.get();
                break;
            }
        }
    }

    SrcPad* pad = source_pad
        ? addBranchedSrcPad(name, *source_pad)
        : addSrcPad(name, TemplateCaps{{hint_type}});
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

    // 具体类只负责探测；基类只校验它必须给全的东西：类型正确 + 能选出解码器的 codec_id。
    // 尺寸/采样率等字段是否需要由具体消费者(解码器/Mux)各自判断。
    if (result.video && (result.video->media_type != MediaType::VIDEO_ENCODED ||
                         result.video->codec_id == AV_CODEC_ID_NONE)) {
        postMessage(MessageType::ERROR,
                    "DemuxNode: probeStreams returned codec-less or non-video caps as video result");
        return false;
    }
    if (result.audio && (result.audio->media_type != MediaType::AUDIO_ENCODED ||
                         result.audio->codec_id == AV_CODEC_ID_NONE)) {
        postMessage(MessageType::ERROR,
                    "DemuxNode: probeStreams returned codec-less or non-audio caps as audio result");
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

void DemuxNode::runLoop() {
    std::unordered_set<OutputRoute*> initialized;
    // Ready 只缓存探测结果；worker 启动后才把完整 encoded Caps 作为每条 Route 的首项发布。
    for (const auto& [pad_name, type] : pad_to_type_) {
        SrcPad* pad = getSrcPad(pad_name);
        if (!pad || !pad->isConnected() || !pad->route() ||
            !initialized.insert(pad->route().get()).second) {
            continue;
        }

        const CapsEvent* caps = type == MediaType::VIDEO_ENCODED
            ? (probe_result_.video ? &*probe_result_.video : nullptr)
            : (probe_result_.audio ? &*probe_result_.audio : nullptr);
        if (!caps) {
            postMessage(MessageType::ERROR,
                        "DemuxNode: missing cached caps for pad '" + pad_name + "'");
            return;
        }

        if (!sendCapsEvent(pad_name, *caps)) {
            return;
        }
    }

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
            return;
        }

        if (!result.buffer) {
            postMessage(MessageType::ERROR,
                        "DemuxNode: readFrame returned BUFFER without a buffer");
            return;
        }

        const MediaType media_type = result.buffer->media_type;
        // 按实际媒体类型定位唯一逻辑 Route；同类型分叉 Pad 必须共享这条 Route
        std::shared_ptr<OutputRoute> route;
        for (const auto& pad : src_pads_) {
            auto actual = pad->actualType();
            if (!pad->isConnected() || !actual || *actual != media_type) {
                continue;
            }
            if (!route) {
                route = pad->route();
            } else if (route.get() != pad->route().get()) {
                postMessage(MessageType::ERROR,
                            "DemuxNode: one media stream is bound to multiple routes");
                return;
            }
        }

        if (!route) {
            postMessage(MessageType::ERROR, "DemuxNode: no route for decoded media type");
            return;
        }

        if (route->publishBlocking(QueueItem{std::move(result.buffer)}) !=
            RoutePublishResult::PUBLISHED) {
            return;
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

bool MuxNode::flushPendingOutput() {
    if (pending_output_.empty()) {
        return true;
    }

    auto* output = new Buffer();
    output->data = new uint8_t[pending_output_.size()];
    output->size = pending_output_.size();
    output->media_type = MediaType::CONTAINER;
    std::copy(pending_output_.begin(), pending_output_.end(), output->data);
    pending_output_.clear();

    // Header、packet、trailer 都经同一个 BufferRef 发布边界；失败时 output_ref 自动释放。
    BufferRef output_ref(output);
    return pushToDownstream(std::move(output_ref), "out_0");
}

bool MuxNode::onReady() {
    pending_output_.clear();
    pad_to_stream_.clear();
    initial_caps_pads_.clear();
    eos_pads_.clear();
    header_written_ = false;

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
        // 任意一路 Route 新数据或 cancel 都唤醒 Mux 选择循环。
        pad->setRouteNotify([this]() {
            std::lock_guard<std::mutex> lock(mux_mutex_);
            mux_cv_.notify_one();
        });
    }

    auto* output_pad = getSrcPad("out_0");
    if (!output_pad || !output_pad->isConnected() || !output_pad->route()) {
        postMessage(MessageType::ERROR, "MuxNode: output pad 'out_0' is not connected");
        return false;
    }

    // Context 不依赖输入流格式，可在 Ready 建立；stream/header 留到 Running 的完整 Caps 到达后。
    return allocateContext(format_);
}

bool MuxNode::configureInitialInput(const std::string& pad_name, const CapsEvent& caps) {
    // Container headers must name every input stream before any packet bytes are written. This function
    // consumes exactly one initial encoded Caps per linked Pad, creates the backend stream, and records the
    // Pad → backend stream index mapping. A later Caps on that Pad is explicitly rejected by the caller.
    // Mux 只要求能建流的 codec_id；容器特有字段(如尺寸)由具体后端 addStream 自行校验。
    SinkPad* pad = getSinkPad(pad_name);
    if (!pad || caps.codec_id == AV_CODEC_ID_NONE || !pad->templateCaps().contains(caps.media_type)) {
        postMessage(MessageType::ERROR, "MuxNode: invalid initial CapsEvent on pad '" + pad_name + "'");
        return false;
    }
    if (initial_caps_pads_.count(pad_name)) {
        postMessage(MessageType::ERROR,
                    "MuxNode: runtime encoded Caps changes are not supported after header setup");
        return false;
    }

    if (!applyCapsEvent(pad_name, caps)) {
        return false;
    }

    int stream_index = -1;
    if (!addStream(caps, &stream_index)) {
        return false;
    }
    pad_to_stream_[pad_name] = stream_index;
    initial_caps_pads_.insert(pad_name);
    return true;
}

bool MuxNode::writeHeaderAfterAllInputsConfigured() {
    if (header_written_ || initial_caps_pads_.size() != sink_pads_.size()) {
        return true;
    }

    auto* output_pad = getSrcPad("out_0");
    if (!output_pad || !output_pad->isConnected() || !output_pad->route()) {
        postMessage(MessageType::ERROR, "MuxNode: output pad 'out_0' is not connected");
        return false;
    }

    CapsEvent output_caps;
    output_caps.media_type = MediaType::CONTAINER;
    if (!sendCapsEvent("out_0", output_caps) || !writeHeader() || !flushPendingOutput()) {
        pending_output_.clear();
        return false;
    }

    header_written_ = true;
    return true;
}

void MuxNode::runLoop() {
    while (!stop_requested_.load()) {
        SinkPad* ready_pad = waitAnyPadReady();
        if (!ready_pad) {
            break;
        }

        auto delivery = ready_pad->tryAcquire();
        if (!delivery) {
            continue;
        }

        const QueueItem& item = delivery->item();
        const std::string& pad_name = ready_pad->name();
        if (std::holds_alternative<BufferRef>(item)) {
            const auto active = active_caps_.find(pad_name);
            if (!header_written_ || active == active_caps_.end()) {
                postMessage(MessageType::ERROR,
                            "MuxNode: Buffer received before all linked inputs supplied initial CapsEvent");
                break;
            }

            const BufferRef& buffer = std::get<BufferRef>(item);
            if (buffer->media_type != active->second.media_type) {
                postMessage(MessageType::ERROR,
                            "MuxNode: Buffer media type does not match active CapsEvent on pad '" +
                                pad_name + "'");
                break;
            }

            auto stream = pad_to_stream_.find(pad_name);
            if (stream == pad_to_stream_.end() ||
                !writePacket(buffer.get(), stream->second) ||
                !flushPendingOutput() || stop_requested_.load() || !delivery->ack()) {
                pending_output_.clear();
                break;
            }
            continue;
        }

        const Event& event = std::get<Event>(item);
        if (std::holds_alternative<CapsEvent>(event)) {
            if (!configureInitialInput(pad_name, std::get<CapsEvent>(event)) ||
                !writeHeaderAfterAllInputsConfigured() || !delivery->ack()) {
                break;
            }
            continue;
        }

        // 已 link 的 Mux 输入在初始 Caps 前 EOS 无法参与容器 header，是明确配置错误。
        if (!initial_caps_pads_.count(pad_name)) {
            postMessage(MessageType::ERROR,
                        "MuxNode: linked input reached EOS before initial CapsEvent");
            break;
        }

        eos_pads_.insert(pad_name);
        const bool final_input_eos = eos_pads_.size() == sink_pads_.size();
        if (!final_input_eos) {
            if (!delivery->ack()) {
                break;
            }
            continue;
        }

        if (!writeTrailer() || !flushPendingOutput() || !sendEOSDownstream() || !delivery->ack()) {
            pending_output_.clear();
        }
        break;
    }
}

SinkPad* MuxNode::waitAnyPadReady() {
    std::unique_lock<std::mutex> lock(mux_mutex_);
    mux_cv_.wait(lock, [this] {
        if (stop_requested_.load()) {
            return true;
        }

        for (const auto& pad : sink_pads_) {
            if (!pad->isConnected()) {
                continue;
            }
            const auto top = pad->peek();
            if (!top) {
                continue;
            }

            // Mux cannot write a header until every linked input supplied initial Caps. While that
            // condition is incomplete, only control Events may advance the setup state; an early
            // Buffer on another input must remain retained instead of being mistaken for an error.
            if (!header_written_) {
                if (std::holds_alternative<Event>(*top) || !initial_caps_pads_.count(pad->name())) {
                    return true;
                }
                // This input already supplied Caps, so its queued Buffer is valid but must wait for
                // the other linked inputs to establish their header streams.
                continue;
            }
            return true;
        }
        return false;
    });

    if (stop_requested_.load()) {
        return nullptr;
    }

    if (!header_written_) {
        // Prefer a missing input's first item (Caps or an invalid early Buffer) so the loop always
        // makes progress or reports the precise protocol error instead of waiting indefinitely.
        for (auto& pad : sink_pads_) {
            const auto top = pad->peek();
            if (top && !initial_caps_pads_.count(pad->name())) {
                return pad.get();
            }
        }
        for (auto& pad : sink_pads_) {
            const auto top = pad->peek();
            if (top && std::holds_alternative<Event>(*top)) {
                return pad.get();
            }
        }
        return nullptr;
    }

    return selectMinDtsPad();
}

SinkPad* MuxNode::selectMinDtsPad() {
    SinkPad* min_pad = nullptr;
    int64_t min_dts = std::numeric_limits<int64_t>::max();

    for (auto& pad : sink_pads_) {
        if (!pad->isConnected()) {
            continue;
        }

        auto top = pad->peek();
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
