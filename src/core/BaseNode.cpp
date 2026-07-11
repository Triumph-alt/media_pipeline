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

bool DemuxNode::onReady() {
    if (!openInput()) {
        return false;
    }

    if (!probeStreams()) {
        closeInput();
        return false;
    }

    // 校验：用户 link 的每个 pad，在输入源里是否真的有对应类型的流
    for (const auto& [pad_name, type] : pad_to_type_) {
        bool found = false;
        for (size_t i = 0; i < stream_caps_.size(); ++i) {
            if (stream_caps_[i].media_type == type) {
                pad_to_stream_index_[pad_name] = static_cast<int>(i);
                found = true;
                break;
            }
        }
        if (!found) {
            postMessage(MessageType::ERROR,
                        "DemuxNode: no stream matching pad '" + pad_name + "'");
            closeInput();
            return false;
        }
    }

    return true;
}

bool DemuxNode::onStreamInfo() {
    for (const auto& [pad_name, type] : pad_to_type_) {
        auto it = pad_to_stream_index_.find(pad_name);
        if (it == pad_to_stream_index_.end()) {
            continue;
        }

        SrcPad* pad = getSrcPad(pad_name);
        if (!pad || !pad->isConnected()) {
            continue;
        }

        // 先 resize 到正确容量，再发 CapsEvent
        // sendCapsEvent 内部校验 caps.media_type ∈ SrcPad.templateCaps，通过后 setActualType
        pad->edge()->queue->resize(selectQueueCapacity(type));
        if (!sendCapsEvent(pad_name, stream_caps_[it->second])) {
            return false;   // sendCapsEvent 已 postMessage(ERROR)，触发 Ready 回滚
        }
    }
    return true;
}

void DemuxNode::runLoop() {
    while (!stop_requested_.load()) {
        Buffer* buf = nullptr;
        if (!readFrame(&buf)) {
            // 子类已上报 ERROR，直接退出
            break;
        }
        if (!buf) {
            // EOF
            sendEOSDownstream();
            break;
        }

        BufferRef primary(buf);   // RAII 接管，从这一行起 buf 只由 primary 拥有

        // 按 media_type 分发到所有匹配的已连接 SrcPad，判断 pad 是否要收此帧一律用 pad->actualType()
        // 每路独立 clone 深拷贝，非阻塞 tryPush，满则丢弃该路副本。
        // primary 始终持有原 buf，循环结束析构统一 unref。
        for (auto& pad : src_pads_) {
            if (!pad->isConnected()) {
                continue;
            }
            auto actual = pad->actualType();
            if (!actual || *actual != buf->media_type) {
                continue;
            }
            pad->tryPush(QueueItem{primary.clone()});
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

bool MuxNode::onReady() {
    return true;
}

bool MuxNode::onStreamInfo() {
    if (!allocateContext(format_)) {
        return false;
    }

    // 从每个 SinkPad 收取 CapsEvent，并添加输出流
    for (auto& pad : sink_pads_) {
        if (!pad->isConnected()) {
            continue;
        }

        // receiveCapsEvent 内部完成 popBlocking + 校验 + setActualType + 存入 negotiated_caps_
        // 若失败已 postMessage(ERROR)，此处 closeContext 后触发 Ready 回滚
        if (!receiveCapsEvent(pad->name())) {
            closeContext();
            return false;
        }

        const CapsEvent& caps = negotiated_caps_[pad->name()];

        int stream_index = -1;
        if (!addStream(caps, &stream_index)) {
            closeContext();
            return false;
        }
        pad_to_stream_[pad->name()] = stream_index;

        // 注册外部 notify：任意一路有数据就唤醒 mux_cv_
        pad->edge()->queue->setExternalNotify([this]() {
            std::lock_guard<std::mutex> lock(mux_mutex_);
            mux_cv_.notify_one();
        });
    }

    if (!writeHeader()) {
        closeContext();
        return false;
    }

    return true;
}

void MuxNode::runLoop() {
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
            if (it != pad_to_stream_.end()) {
                if (!writePacket(buf, it->second)) {
                    // 子类已上报 ERROR
                    break;
                }
            }
        } else if (std::holds_alternative<Event>(*item)) {
            const Event& event = std::get<Event>(*item);
            if (std::holds_alternative<EOSEvent>(event)) {
                eos_pads_.insert(ready_pad->name());
                if (eos_pads_.size() == sink_pads_.size()) {
                    if (!writeTrailer()) {
                        postMessage(MessageType::ERROR, "MuxNode: writeTrailer failed");
                        break;
                    }
                    sendEOSDownstream();
                    break;
                }
            }
        }
    }
}

SinkPad* MuxNode::waitAnyPadReady() {
    std::unique_lock<std::mutex> lock(mux_mutex_);
    mux_cv_.wait(lock, [this] {
        if (stop_requested_.load()) {
            return true;
        }
        for (auto& pad : sink_pads_) {
            if (pad->edge() && pad->edge()->queue && !pad->edge()->queue->empty()) {
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
        if (!pad->edge() || !pad->edge()->queue) {
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
    closeContext();
}

} // namespace pipeline
