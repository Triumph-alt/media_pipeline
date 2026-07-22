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
// BaseNode: 数据分发
// ===================================================================
bool BaseNode::pushToDownstream(BufferRef&& buf, const std::string& src_pad_name) {
    // 发布入口立即接管调用方显式 move 进来的引用；后续任一失败路径都由 RAII 释放。
    BufferRef primary(std::move(buf));
    if (!primary) {
        return false;
    }

    std::shared_ptr<OutputRoute> route;
    if (!src_pad_name.empty()) {
        SrcPad* pad = getSrcPad(src_pad_name);
        if (!pad || !pad->isConnected()) {
            return false;
        }
        route = pad->route();
    } else {
        // 未指定 Pad 时只能有一条逻辑 Route；多 SrcPad 分叉应共享同一个 Route
        for (const auto& pad : src_pads_) {
            if (!pad->isConnected()) {
                continue;
            }
            if (!route) {
                route = pad->route();
            } else if (route.get() != pad->route().get()) {
                postMessage(MessageType::ERROR,
                            "pushToDownstream: ambiguous logical output route");
                return false;
            }
        }
    }

    if (!route) {
        return false;
    }

    return route->publishBlocking(QueueItem{std::move(primary)}) ==
           RoutePublishResult::PUBLISHED;
}

bool BaseNode::sendEOSDownstream() {
    // 同一 Route 的多个 SrcPad 只发布一次 EOS；每条 Subscription 按自身游标接收它。
    std::unordered_set<OutputRoute*> published;
    for (const auto& pad : src_pads_) {
        auto route = pad->route();
        if (!pad->isConnected() || !route || !published.insert(route.get()).second) {
            continue;
        }
        if (route->publishBlocking(QueueItem{Event{EOSEvent{}}}) !=
            RoutePublishResult::PUBLISHED) {
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

    if (!pad->templateCaps().contains(caps.media_type)) {
        postMessage(MessageType::ERROR,
                    "sendCapsEvent: caps.media_type not in src pad '" + src_pad_name + "' template caps");
        return false;
    }

    auto route = pad->route();
    // Route 是同源 Pad 的共同格式边界：所有成员必须接受同一个实际 MediaType。
    for (auto& sibling : src_pads_) {
        if (sibling->isConnected() && sibling->route().get() == route.get()) {
            if (!sibling->templateCaps().contains(caps.media_type)) {
                postMessage(MessageType::ERROR,
                            "sendCapsEvent: caps.media_type not in shared route pad template caps");
                return false;
            }
            sibling->setActualType(caps.media_type);
        }
    }

    if (route->publishBlocking(QueueItem{Event{caps}}) != RoutePublishResult::PUBLISHED) {
        postMessage(MessageType::ERROR, "sendCapsEvent: route publish cancelled");
        return false;
    }
    return true;
}

bool BaseNode::receiveCapsEvent(const std::string& sink_pad_name) {
    SinkPad* pad = getSinkPad(sink_pad_name);
    if (!pad || !pad->isConnected()) {
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: sink pad '" + sink_pad_name + "' not found or not connected");
        return false;
    }

    auto delivery = pad->acquireBlocking();
    // Delivery 保持当前游标；只有成功校验并写入 negotiated_caps_ 后才 ack。
    if (!delivery) {
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: route cancelled, no CapsEvent on pad '" + sink_pad_name + "'");
        return false;
    }

    const QueueItem& item = delivery->item();
    if (!std::holds_alternative<Event>(item)) {
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: expected CapsEvent on pad '" + sink_pad_name + "', got Buffer");
        return false;
    }

    const Event& event = std::get<Event>(item);
    if (!std::holds_alternative<CapsEvent>(event)) {
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: expected CapsEvent on pad '" + sink_pad_name + "', got other Event");
        return false;
    }

    const CapsEvent& caps = std::get<CapsEvent>(event);
    if (!pad->templateCaps().contains(caps.media_type)) {
        postMessage(MessageType::ERROR,
                    "receiveCapsEvent: caps.media_type not in sink pad '" + sink_pad_name + "' template caps");
        return false;
    }

    pad->setActualType(caps.media_type);
    negotiated_caps_[sink_pad_name] = caps;
    if (!delivery->ack()) {
        postMessage(MessageType::ERROR, "receiveCapsEvent: failed to acknowledge CapsEvent");
        return false;
    }
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
    // 找到唯一的输入 Pad
    auto* sink_pad = sink_pads_[0].get();

    while (!stop_requested_.load()) {
        // 从输入 Route 中阻塞获取下一项待处理数据
        auto delivery = sink_pad->acquireBlocking();
        if (!delivery) {
            break;
        }

        // 提取媒体数据或者控制事件，此时该项仍然属于 Route 的保留区
        const QueueItem& item = delivery->item();

        // 如果是媒体数据
        if (std::holds_alternative<BufferRef>(item)) {
            // consume 真正处理这帧
            consume(std::get<BufferRef>(item).get());

            // ack 是本轮 consume/onEvent 成功完成的提交点，在这之前 Route 不会释放该订阅者的容量
            if (stop_requested_.load() || !delivery->ack()) {
                break;
            }
            continue;
        }

        // 如果不是 Buffer，那就是控制事件，理论上 Running 阶段接到的 Event 只有 EOS
        // ack 会释放 Delivery 内的 QueueItem；复制 Event，使事件生命周期跨过提交点
        Event event = std::get<Event>(item);
        if (std::holds_alternative<EOSEvent>(event)) {
            // 先 ack EOS（释放上游 Route，输出侧 drain 期间不占背压窗口）
            if (!delivery->ack()) {
                break;
            }

            // 输出侧 drain，默认空实现；主动 stop 时具体实现内部应立即返回
            onDrain();

            // drain 被 stop 打断、或 drain 内部出错（postMessage(ERROR) 会置 stop_requested_）时，
            // 不再上报"自然 EOS"，直接退出循环
            if (stop_requested_.load()) {
                break;
            }

            // 再报告 Sink 完成，Pipeline 以全部 Sink 的 EOS 汇总决定自然 stop
            onEvent(event);
            continue;
        }

        // 这里基本是防御性路径，onEvent 会 postMessage(ERROR) 设置 stop_requested_
        // 所以此处先处理确认没有错误，再 ack
        onEvent(event);
        if (stop_requested_.load() || !delivery->ack()) {
            break;
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
    std::vector<BufferRef> outputs;
    while (!stop_requested_.load()) {
        // 从 RouteSubscription 获取下一项
        auto delivery = sink_pad->acquireBlocking();
        if (!delivery) {
            break;
        }

        // 输入 Delivery 直到 process 和所有输出 Route publish 都成功后才 ack，形成逐级背压
        const QueueItem& item = delivery->item();
        if (std::holds_alternative<BufferRef>(item)) {
            // clear 会释放上轮任何尚未移交的输出；正常路径中这些元素已经 move 为空。
            outputs.clear();

            // 一个输入 Buffer 产生 0 到 N 个由 BufferRef 持有的新输出 Buffer。
            process(std::get<BufferRef>(item).get(), outputs);
            if (stop_requested_.load()) {
                // process 后发生 stop/error 时，outputs 析构会释放全部尚未发布的输出。
                break;
            }

            bool outputs_published = true;
            for (auto& out : outputs) {
                // 发布入口无条件接管当前引用；成功进入 Route，失败也在入口内释放。
                if (!pushToDownstream(std::move(out))) {
                    outputs_published = false;
                    break;
                }
            }
            if (!outputs_published) {
                // 已发布元素已经为空，未遍历元素仍由 outputs 持有并在退出时自动释放。
                break;
            }
        } else {
            onEvent(std::get<Event>(item));
            if (stop_requested_.load()) {
                break;
            }
        }

        if (!delivery->ack()) {
            break;
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

    // 具体类只负责探测；基类验证返回值后才写入正式状态
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

    // 校验用户请求：nullopt 表示探测成功，但输入确实没有该类型
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
    std::unordered_set<OutputRoute*> initialized;
    // 多个同类型 Demux Pad 是同一 Track 分叉：每个 Route 只 resize/publish 一次 Caps
    for (const auto& [pad_name, type] : pad_to_type_) {
        SrcPad* pad = getSrcPad(pad_name);
        if (!pad || !pad->isConnected() || !pad->route()) {
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

        if (!initialized.insert(pad->route().get()).second) {
            continue;
        }
        pad->route()->resize(selectRouteCapacity(type));
        if (!sendCapsEvent(pad_name, *caps)) {
            return false;
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

    // MuxNode 当前固定单输出；显式 move，把 CONTAINER Buffer 的发布引用交给下游入口。
    BufferRef output_ref(output);
    if (!pushToDownstream(std::move(output_ref), "out_0")) {
        postMessage(MessageType::ERROR, "MuxNode: failed to publish container output");
    }
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

        // 任意一路 Route 有新数据或被取消时唤醒 mux_cv_。
        pad->setRouteNotify([this]() {
            std::lock_guard<std::mutex> lock(mux_mutex_);
            mux_cv_.notify_one();
        });
    }

    // 固定输出 Route 已由 Graph 建立静态订阅；先确定容量并发送 CONTAINER Caps。
    auto* output_pad = getSrcPad("out_0");
    if (!output_pad || !output_pad->isConnected() || !output_pad->route()) {
        postMessage(MessageType::ERROR, "MuxNode: output pad 'out_0' is not connected");
        return false;
    }
    output_pad->route()->resize(selectRouteCapacity(MediaType::CONTAINER));

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

        auto delivery = ready_pad->tryAcquire();
        if (!delivery) {
            continue;
        }

        // 选中的输入在 writePacket/trailer 及其容器输出 publish 成功前保持 in-flight。
        const QueueItem& item = delivery->item();
        if (std::holds_alternative<BufferRef>(item)) {
            const Buffer* buf = std::get<BufferRef>(item).get();
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
            if (stop_requested_.load() || !delivery->ack()) {
                break;
            }
            continue;
        }

        const Event& event = std::get<Event>(item);
        if (!std::holds_alternative<EOSEvent>(event)) {
            postMessage(MessageType::ERROR,
                        "CapsEvent received in runLoop; mux nodes must consume CapsEvent in onStreamInfo");
            break;
        }

        eos_pads_.insert(ready_pad->name());
        // 最后一路 EOS 的 ack 要等 trailer 和下游 EOS 都已可靠发布。
        const bool final_input_eos = eos_pads_.size() == sink_pads_.size();
        if (!final_input_eos) {
            if (!delivery->ack()) {
                break;
            }
            continue;
        }

        if (!writeTrailer()) {
            pending_output_.clear();
            break;   // 具体类负责上报格式后端错误
        }
        flushPendingOutput();
        if (!sendEOSDownstream()) {
            break;
        }
        if (!delivery->ack()) {
            break;
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
        for (auto& pad : sink_pads_) {
            if (pad->isConnected() && pad->peek().has_value()) {
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
