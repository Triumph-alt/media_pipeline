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

void BaseNode::sendCapsEvent(const std::string& src_pad_name, const CapsEvent& caps) {
    SrcPad* pad = getSrcPad(src_pad_name);
    if (!pad || !pad->isConnected()) {
        return;
    }

    // CapsEvent 不允许丢失，用阻塞 push
    // 只负责发送，不存储。接收方在 onStreamInfo() 里自行存入 negotiated_caps_
    pad->pushBlocking(QueueItem{Event{caps}});
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

} // namespace pipeline
