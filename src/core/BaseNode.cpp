#include "pipeline/core/BaseNode.h"
#include "pipeline/core/Edge.h"
#include "pipeline/core/Pipeline.h"

#include <algorithm>
#include <deque>
#include <limits>
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
    // 首份 Caps 用 TemplateCaps 选择 SinkPad 的实际类型
    // 后续 Caps 只能在已固定的 MediaType 内重配格式，不允许运行中媒体类型改变
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

    // 字段是否足够由具体消费者 onCaps 判断；成功前不更新 active_caps_，也不允许 ack
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
    // 局部 outputs 容器
    std::vector<QueueItem> outputs;

    while (!stop_requested_.load()) {
        // 清理上一轮的 outputs
        outputs.clear();

        // 调用具体 Source 子类的 produce，将本轮要输出的有序项填入 outputs。采集 Source
        // 没有自然 EOF；produce 在自身内部等待数据、stop 或 ERROR，不用返回值表达 EOS。
        produce(outputs);

        // produce() 内的 ERROR 或外部 stop 会同步置位标志
        // 尚未发布的 BufferRef 由 outputs 的 RAII 所有权自动释放
        if (stop_requested_.load()) {
            break;
        }

        // 按具体 Source 写进 vector 的顺序提交 Route
        bool outputs_published = true;
        for (auto& output : outputs) {
            // 对 Caps 项先复制一份，以便安全地从原对象读取完整字段作为本地 active Caps
            std::optional<CapsEvent> published_caps;
            if (std::holds_alternative<BufferRef>(output)) {
                const BufferRef& buffer = std::get<BufferRef>(output);
                // Source 没有输入 Route 可借用 active_caps_
                // 所以必须在输出边界自己维护 active_output_caps_ 避免非法序列进入共享 OutputRoute。
                if (!buffer || !active_output_caps_) {
                    postMessage(MessageType::ERROR,
                                "SourceNode: Buffer produced before initial CapsEvent");
                    outputs_published = false;
                    break;
                }
                if (buffer->media_type != active_output_caps_->media_type) {
                    // Buffer 类型必须与当前 Caps 一致
                    postMessage(MessageType::ERROR,
                                "SourceNode: Buffer media type does not match active CapsEvent");
                    outputs_published = false;
                    break;
                }
            } else {
                const Event& event = std::get<Event>(output);
                // Source 的终结事件由基类独占，具体 produce() 只能产生 Caps 或 Buffer
                if (std::holds_alternative<EOSEvent>(event)) {
                    postMessage(MessageType::ERROR,
                                "SourceNode: produce must not emit EOSEvent");
                    outputs_published = false;
                    break;
                }
                // 移动发布后 CapsEvent 的 vector 字段可能已是 moved-from 状态，必须在进入
                // publishOutputItem() 前保留副本；仅 publish 成功才允许切换生产侧 active Caps。
                published_caps = std::get<CapsEvent>(event);
            }

            if (!publishOutputItem(std::move(output))) {
                outputs_published = false;
                break;
            }

            if (published_caps) {
                active_output_caps_ = std::move(*published_caps);
            }
        }

        // 发布失败或 stop 时退出。采集 Source 不发送自然 EOS：实时设备没有文件 EOF，
        // 窗口关闭、SIGINT 和外部 stop 均通过 Pipeline cancel 结束整条链路。
        if (!outputs_published || stop_requested_.load()) {
            break;
        }
    }
}

// ===================================================================
// SinkNode: runLoop
// ===================================================================

void SinkNode::runLoop() {
    // 绑定唯一输入 Pad
    auto* sink_pad = sink_pads_[0].get();
    const std::string& sink_pad_name = sink_pad->name();

    while (!stop_requested_.load()) {
        // 阻塞等待上游 Route 的下一项
        auto delivery = sink_pad->acquireBlocking();
        if (!delivery) {
            break;
        }

        const QueueItem& item = delivery->item();
        if (std::holds_alternative<BufferRef>(item)) {
            // Buffer 没有 active Caps 就无法被完整解释；这是上游协议错误而非默认值回退
            const auto active = active_caps_.find(sink_pad_name);
            if (active == active_caps_.end()) {
                postMessage(MessageType::ERROR,
                            "SinkNode: Buffer received before initial CapsEvent on pad '" +
                                sink_pad_name + "'");
                break;
            }

            // Buffer 的 MediaType 必须匹配当前 Caps
            if (std::get<BufferRef>(item)->media_type != active->second.media_type) {
                postMessage(MessageType::ERROR,
                            "SinkNode: Buffer media type does not match active CapsEvent on pad '" +
                                sink_pad_name + "'");
                break;
            }

            // 调用具体 Sink 的 consume()
            consume(std::get<BufferRef>(item).get());

            // consume() 完成后才 ack
            if (stop_requested_.load() || !delivery->ack()) {
                break;
            }
            continue;
        }

        const Event& event = std::get<Event>(item);
        if (std::holds_alternative<CapsEvent>(event)) {
            // 重配在当前 Route worker 内串行完成；只有 onCaps 成功后才能提交此格式边界
            if (!applyCapsEvent(sink_pad_name, std::get<CapsEvent>(event)) || !delivery->ack()) {
                break;
            }
            continue;
        }

        // 先 ack 输入 EOS，输出侧 drain 期间不应占住可靠背压窗口
        if (!delivery->ack()) {
            break;
        }

        // drain 等待输出真正完成
        onDrain();
        if (stop_requested_.load()) {
            break;
        }

        // 向 Pipeline 上报 EOS，表示最终 Sink 已经真正完成
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
    // 先获取 Transform 唯一 sink_pad 输入端口
    auto* sink_pad = sink_pads_[0].get();
    const std::string& sink_pad_name = sink_pad->name();
    std::vector<QueueItem> outputs;

    while (!stop_requested_.load()) {
        // 等上游 Route 出下一个 RouteDelivery
        auto delivery = sink_pad->acquireBlocking();
        if (!delivery) {
            break;
        }

        const QueueItem& item = delivery->item();
        if (std::holds_alternative<BufferRef>(item)) {
            // 如果是 Buffer 分支，先检查已有 active Caps
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

            // 调用具体 Transform 的 process，对数据进行处理并把一批有序项目塞进 outputs
            outputs.clear();
            process(std::get<BufferRef>(item).get(), outputs);

            // 如果 process 内部遇到错误 postMessage() 会置 stop_requested_
            if (stop_requested_.load()) {
                break;
            }

            // 顺序发布 outputs
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
            // 如果是 CapsEvent 分支
            outputs.clear();

            // 先应用输入的 CapsEvent
            if (!applyCapsEvent(sink_pad_name, std::get<CapsEvent>(event), &outputs)) {
                break;
            }

            // 发布处理后的输出 Caps
            bool outputs_published = true;
            for (auto& output : outputs) {
                if (!publishOutputItem(std::move(output))) {
                    outputs_published = false;
                    break;
                }
            }

            // 输出全部成功后，才 ack 输入 Buffer
            if (!outputs_published || !delivery->ack()) {
                break;
            }
            continue;
        }

        // 子类 onEOS() 只能追加 Decoder flush 等延迟 Caps/Buffer，不能自行追加 EOS
        outputs.clear();
        onEOS(outputs);
        if (stop_requested_.load()) {
            break;
        }

        // 基类统一在末尾追加唯一 EOSEvent，因此即使子类没有上下文或没有待 flush 数据
        // 输入 Route 的 EOS 也恰好对应输出 Route 的一个 EOS
        outputs.emplace_back(Event{EOSEvent{}});

        bool outputs_published = true;
        for (auto& output : outputs) {
            if (!publishOutputItem(std::move(output))) {
                outputs_published = false;
                break;
            }
        }

        // 最后一个 break 是正常终结，表示 Transform 的工作任务完成
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
//
// 调度模型：
//   pullPhase: 用 tryAcquire 把各输入 Route 上的控制事件和已配置 Buffer
//              拉进本地有界 staging，并在 ack 前完成 Buffer 合同校验
//   emitPhase: 在合同允许时写 Header / 按全局最小 DTS 写 packet / 写 Trailer
//   wait:      仅当本轮 pull 与 emit 都无进展时等待 Route notify 或 stop
// ===================================================================

SinkPad* MuxNode::requestSinkPad(const std::string& name, MediaType hint_type) {
    if (!acceptsInputPad(format_, hint_type)) {
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

    // Header、packet、trailer 都经同一个 BufferRef 发布边界；失败时 output_ref 自动释放
    BufferRef output_ref(output);
    return pushToDownstream(std::move(output_ref), "out_0");
}

void MuxNode::clearStaging() {
    for (auto& [pad_name, staging] : staging_) {
        (void)pad_name;
        staging.packets.clear();
        staging.initial_caps_done = false;
        staging.eos_done = false;
    }
}

size_t MuxNode::stagingTotalSize() const {
    size_t total = 0;
    for (const auto& [pad_name, staging] : staging_) {
        (void)pad_name;
        total += staging.packets.size();
    }
    return total;
}

bool MuxNode::stagingIsFull(const InputStaging& staging) const {
    return staging.packets.size() >= kStagingPerPadLimit ||
           stagingTotalSize() >= kStagingTotalLimit;
}

bool MuxNode::canEmitPacket() const {
    if (!header_written_) {
        return false;
    }

    bool has_active = false;
    for (const auto& pad : sink_pads_) {
        if (!pad->isConnected()) {
            continue;
        }
        const auto it = staging_.find(pad->name());
        if (it == staging_.end()) {
            return false;
        }
        // 已 EOS 且本地队列已空的路不再参与候选
        if (it->second.eos_done && it->second.packets.empty()) {
            continue;
        }
        has_active = true;
        // 任一仍活跃的路缺少队首，就不能越过其未知未来 DTS
        if (it->second.packets.empty()) {
            return false;
        }
    }
    return has_active;
}

bool MuxNode::allInputsEosAndDrained() const {
    if (sink_pads_.empty()) {
        return false;
    }
    for (const auto& pad : sink_pads_) {
        if (!pad->isConnected()) {
            return false;
        }
        const auto it = staging_.find(pad->name());
        if (it == staging_.end() || !it->second.eos_done || !it->second.packets.empty()) {
            return false;
        }
    }
    return true;
}

bool MuxNode::stagingBlockedWithoutProgress() const {
    // 任一仍需数据的活跃路 staging 已满，且当前既不能写 Header 也不能写 packet
    if (header_written_ && canEmitPacket()) {
        return false;
    }
    if (!header_written_) {
        size_t configured = 0;
        for (const auto& [pad_name, staging] : staging_) {
            (void)pad_name;
            if (staging.initial_caps_done) {
                ++configured;
            }
        }
        if (configured == sink_pads_.size() && !sink_pads_.empty()) {
            // Caps 已齐，应由 tryEmitHeader 前进，不算 blocked
            return false;
        }
    }

    for (const auto& pad : sink_pads_) {
        if (!pad->isConnected()) {
            continue;
        }
        const auto it = staging_.find(pad->name());
        if (it == staging_.end()) {
            continue;
        }
        const InputStaging& staging = it->second;
        if (staging.eos_done && staging.packets.empty()) {
            continue;
        }
        if (stagingIsFull(staging)) {
            // 该路已满，且我们仍在等 Header 或其他活跃路的数据
            return true;
        }
    }
    return false;
}

bool MuxNode::validateBufferForStaging(const std::string& pad_name, const Buffer* buffer) {
    // 必须在 ack 之前调用：失败时不 ack，Delivery 析构放弃 in-flight，runLoop 随后 ERROR 退出
    if (!buffer) {
        postMessage(MessageType::ERROR,
                    "MuxNode: null Buffer on pad '" + pad_name + "'");
        return false;
    }

    const auto staging_it = staging_.find(pad_name);
    if (staging_it == staging_.end() || !staging_it->second.initial_caps_done) {
        postMessage(MessageType::ERROR,
                    "MuxNode: Buffer received before initial CapsEvent on pad '" + pad_name + "'");
        return false;
    }

    const auto active = active_caps_.find(pad_name);
    if (active == active_caps_.end()) {
        postMessage(MessageType::ERROR,
                    "MuxNode: Buffer received without active CapsEvent on pad '" + pad_name + "'");
        return false;
    }
    if (buffer->media_type != active->second.media_type) {
        postMessage(MessageType::ERROR,
                    "MuxNode: Buffer media type does not match active CapsEvent on pad '" +
                        pad_name + "'");
        return false;
    }
    if (buffer->dts == AV_NOPTS_VALUE) {
        // 跨输入排序要求可比较 DTS；在进 staging 前拒绝，避免 ack 后才发现非法
        postMessage(MessageType::ERROR,
                    "MuxNode: encoded Buffer requires a valid DTS for framework interleaving");
        return false;
    }
    if (pad_to_stream_.find(pad_name) == pad_to_stream_.end()) {
        postMessage(MessageType::ERROR,
                    "MuxNode: no backend stream mapping for pad '" + pad_name + "'");
        return false;
    }
    return true;
}

bool MuxNode::onReady() {
    pending_output_.clear();
    pad_to_stream_.clear();
    staging_.clear();
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
        // 为每路建立空 staging 槽位
        staging_.emplace(pad->name(), InputStaging{});
        // 任意一路 Route 新数据或 cancel 都唤醒 Mux pull/emit 循环
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

    // Context 不依赖输入流格式，可在 Ready 建立；stream/header 留到 Running 的完整 Caps 到达后
    return allocateContext(format_);
}

bool MuxNode::configureInitialInput(const std::string& pad_name, const CapsEvent& caps) {
    // 每个已连接输入只接受一次 initial Caps；后续 Caps 由 pull 路径拒绝
    // Mux 只要求能建流的 codec_id；容器特有字段(如尺寸)由具体后端 addStream 自行校验
    SinkPad* pad = getSinkPad(pad_name);
    if (!pad || caps.codec_id == AV_CODEC_ID_NONE || !pad->templateCaps().contains(caps.media_type)) {
        postMessage(MessageType::ERROR, "MuxNode: invalid initial CapsEvent on pad '" + pad_name + "'");
        return false;
    }

    auto staging_it = staging_.find(pad_name);
    if (staging_it == staging_.end()) {
        postMessage(MessageType::ERROR, "MuxNode: missing staging for pad '" + pad_name + "'");
        return false;
    }
    if (staging_it->second.initial_caps_done) {
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
    staging_it->second.initial_caps_done = true;
    return true;
}

bool MuxNode::tryEmitHeader() {
    if (header_written_) {
        return true;
    }

    // 全部已连接输入都完成 initial Caps 后才允许建立 Header
    for (const auto& pad : sink_pads_) {
        if (!pad->isConnected()) {
            continue;
        }
        const auto it = staging_.find(pad->name());
        if (it == staging_.end() || !it->second.initial_caps_done) {
            return true;
        }
    }

    auto* output_pad = getSrcPad("out_0");
    if (!output_pad || !output_pad->isConnected() || !output_pad->route()) {
        postMessage(MessageType::ERROR, "MuxNode: output pad 'out_0' is not connected");
        return false;
    }

    CapsEvent output_caps;
    output_caps.media_type = MediaType::CONTAINER;
    // 顺序固定：CONTAINER Caps → Header 字节 → flush
    if (!sendCapsEvent("out_0", output_caps) || !writeHeader(format_) || !flushPendingOutput()) {
        pending_output_.clear();
        return false;
    }

    header_written_ = true;
    return true;
}

bool MuxNode::tryEmitPacket() {
    if (!canEmitPacket()) {
        return true;
    }

    // 在完整候选集（每路 staging 队首）上选全局最小 DTS
    std::string min_pad_name;
    int64_t min_dts = std::numeric_limits<int64_t>::max();
    bool found = false;

    for (const auto& pad : sink_pads_) {
        if (!pad->isConnected()) {
            continue;
        }
        const auto it = staging_.find(pad->name());
        if (it == staging_.end()) {
            continue;
        }
        if (it->second.eos_done && it->second.packets.empty()) {
            continue;
        }
        // canEmitPacket 已保证活跃路非空；这里再防一次状态撕裂
        if (it->second.packets.empty()) {
            postMessage(MessageType::ERROR,
                        "MuxNode: active input lost its Buffer candidate during DTS selection");
            return false;
        }

        const BufferRef& buffer = it->second.packets.front();
        // pull 阶段已校验 DTS；这里只比较
        if (buffer->dts < min_dts) {
            min_dts = buffer->dts;
            min_pad_name = pad->name();
            found = true;
        }
    }

    if (!found) {
        postMessage(MessageType::ERROR, "MuxNode: no active Buffer candidate for DTS selection");
        return false;
    }

    auto staging_it = staging_.find(min_pad_name);
    auto stream_it = pad_to_stream_.find(min_pad_name);
    if (staging_it == staging_.end() || stream_it == pad_to_stream_.end()) {
        postMessage(MessageType::ERROR,
                    "MuxNode: missing staging or stream mapping for pad '" + min_pad_name + "'");
        return false;
    }

    BufferRef buffer = std::move(staging_it->second.packets.front());
    staging_it->second.packets.pop_front();

    if (!writePacket(buffer.get(), stream_it->second) || !flushPendingOutput() ||
        stop_requested_.load()) {
        pending_output_.clear();
        return false;
    }
    return true;
}

bool MuxNode::tryEmitTrailer() {
    if (!header_written_ || !allInputsEosAndDrained()) {
        return true;
    }

    if (!writeTrailer() || !flushPendingOutput() || !sendEOSDownstream()) {
        pending_output_.clear();
        return false;
    }
    // Trailer 成功后由 runLoop 退出
    return true;
}

bool MuxNode::pullInputsOnce(bool* progressed, bool* fatal) {
    *progressed = false;
    *fatal = false;

    // 每轮扫描全部输入一次；只使用 tryAcquire，避免单路阻塞拖死其它路的 pull
    for (auto& pad : sink_pads_) {
        if (!pad->isConnected() || stop_requested_.load()) {
            continue;
        }

        const std::string& pad_name = pad->name();
        auto staging_it = staging_.find(pad_name);
        if (staging_it == staging_.end()) {
            postMessage(MessageType::ERROR, "MuxNode: missing staging for pad '" + pad_name + "'");
            *fatal = true;
            return false;
        }
        InputStaging& staging = staging_it->second;

        // 该路已完整结束：EOS 已 ack 且本地队列已空
        if (staging.eos_done && staging.packets.empty()) {
            continue;
        }

        // 先 peek 区分控制事件与 Buffer，避免在 staging 满时误拉 Buffer
        const auto top = pad->peek();
        if (!top) {
            continue;
        }

        if (std::holds_alternative<Event>(*top)) {
            const Event& event = std::get<Event>(*top);

            if (std::holds_alternative<CapsEvent>(event)) {
                // Caps 始终优先 tryAcquire；校验/建流失败时不 ack
                auto delivery = pad->tryAcquire();
                if (!delivery) {
                    continue;
                }
                const QueueItem& item = delivery->item();
                if (!std::holds_alternative<Event>(item) ||
                    !std::holds_alternative<CapsEvent>(std::get<Event>(item))) {
                    postMessage(MessageType::ERROR,
                                "MuxNode: peeked CapsEvent disappeared before acquire on pad '" +
                                    pad_name + "'");
                    *fatal = true;
                    return false;
                }

                // 校验
                if (!configureInitialInput(pad_name, std::get<CapsEvent>(std::get<Event>(item))) ||
                    !delivery->ack()) {
                    *fatal = true;
                    return false;
                }
                *progressed = true;
                continue;
            }

            // EOSEvent
            // 放行条件只看该路是否已完成 initial Caps，不看 header_written_
            // - initial_caps_done == false：该路尚未建流就 EOS，Header 永远无法齐套 → ERROR
            // - initial_caps_done == true：即使 Header 尚未写出也允许处理 EOS
            //   eager pull 会先把已配置路的 Buffer 移入 staging 并 ack，Route 队首因而可能
            //   在 Header 前就露出 EOS；这是正常时序，不能再按旧模型“Header 前任何 EOS
            //   都 ERROR”处理
            if (!staging.initial_caps_done) {
                auto delivery = pad->tryAcquire();
                if (!delivery) {
                    continue;
                }
                postMessage(MessageType::ERROR,
                            "MuxNode: linked input reached EOS before all inputs supplied initial CapsEvent");
                *fatal = true;
                return false;
            }

            // 本地仍有未写 packet 时，EOS 必须留在 Route 队首，避免越过尾包
            // Header 前后同一规则：staging 排空后再 ack EOS
            if (!staging.packets.empty()) {
                continue;
            }

            auto delivery = pad->tryAcquire();
            if (!delivery) {
                continue;
            }
            const QueueItem& item = delivery->item();
            if (!std::holds_alternative<Event>(item) ||
                !std::holds_alternative<EOSEvent>(std::get<Event>(item))) {
                postMessage(MessageType::ERROR,
                            "MuxNode: peeked EOSEvent disappeared before acquire on pad '" +
                                pad_name + "'");
                *fatal = true;
                return false;
            }
            if (!delivery->ack()) {
                *fatal = true;
                return false;
            }
            // 已配置路可在 Header 前标记 eos_done；Header 是否可写仍只依赖各路 initial_caps_done
            staging.eos_done = true;
            *progressed = true;
            continue;
        }

        // 队首是 Buffer
        if (!staging.initial_caps_done) {
            // 未配置 Caps 的 Buffer 是协议错误；先 tryAcquire 再校验，失败不 ack
            auto delivery = pad->tryAcquire();
            if (!delivery) {
                continue;
            }
            postMessage(MessageType::ERROR,
                        "MuxNode: Buffer received before initial CapsEvent on pad '" + pad_name + "'");
            *fatal = true;
            return false;
        }

        // staging 满时不再 pull 该路 Buffer，把机会留给 emit 或其它路的控制事件
        if (stagingIsFull(staging)) {
            continue;
        }

        auto delivery = pad->tryAcquire();
        if (!delivery) {
            continue;
        }

        const QueueItem& item = delivery->item();
        if (!std::holds_alternative<BufferRef>(item)) {
            // peek 与 acquire 之间被并发改变时视为内部错误
            postMessage(MessageType::ERROR,
                        "MuxNode: peeked Buffer disappeared before acquire on pad '" + pad_name + "'");
            *fatal = true;
            return false;
        }

        // 合同校验必须在 ack 前完成：类型、active Caps、DTS、stream mapping
        const BufferRef& buffer = std::get<BufferRef>(item);
        if (!validateBufferForStaging(pad_name, buffer.get())) {
            *fatal = true;
            return false;
        }

        // 再次确认容量（与其它路 pull 交错后 total 可能已变）
        if (stagingIsFull(staging)) {
            // 不 ack：Delivery 析构放弃 in-flight，下次可重新 acquire 同一项
            continue;
        }

        staging.packets.push_back(buffer);
        if (!delivery->ack()) {
            staging.packets.pop_back();
            *fatal = true;
            return false;
        }
        *progressed = true;
    }

    return true;
}

void MuxNode::waitForProgress() {
    std::unique_lock<std::mutex> lock(mux_mutex_);
    mux_cv_.wait(lock, [this] {
        if (stop_requested_.load()) {
            return true;
        }
        // 任一路 Route 上出现新队首，或仅靠 notify 唤醒后由 pull/emit 再判断
        for (const auto& pad : sink_pads_) {
            if (!pad->isConnected()) {
                continue;
            }
            if (pad->peek()) {
                return true;
            }
        }
        return false;
    });
}

void MuxNode::runLoop() {
    while (!stop_requested_.load()) {
        bool pulled = false;
        bool fatal = false;

        // 尽量从各 Route 拉进 staging
        if (!pullInputsOnce(&pulled, &fatal) || fatal) {
            break;
        }

        // Caps 齐套后尝试建立 Header；staging 中已有 packet 保留到 Header 后写出
        if (!tryEmitHeader()) {
            break;
        }

        // 尽可能按全局最小 DTS 连续写出，直到候选集不再完整
        bool emitted_packet = false;
        while (!stop_requested_.load() && canEmitPacket()) {
            const size_t before = stagingTotalSize();
            if (!tryEmitPacket()) {
                return;
            }
            if (stagingTotalSize() >= before) {
                // 防御：emit 后总量未下降说明状态机异常
                postMessage(MessageType::ERROR, "MuxNode: emit packet did not consume staging");
                return;
            }
            emitted_packet = true;
        }

        if (header_written_ && allInputsEosAndDrained()) {
            if (!tryEmitTrailer()) {
                break;
            }
            // 自然完成：Trailer + 输出 EOS 已发送
            return;
        }

        // 满且无法写 Header/Packet：从死锁改为可诊断 ERROR
        if (stagingBlockedWithoutProgress()) {
            if (!header_written_) {
                postMessage(MessageType::ERROR,
                            "MuxNode: input staging full while waiting for initial Caps");
            } else {
                postMessage(MessageType::ERROR,
                            "MuxNode: input staging full while waiting for peer stream data");
            }
            break;
        }

        if (pulled || emitted_packet) {
            // 本轮有进展：立刻再 pull，让刚释放的 Route 空位尽快被上游填上
            continue;
        }

        // 无进展：等待任一路 Route notify 或 stop
        waitForProgress();
    }
}

void MuxNode::onStop() {
    pending_output_.clear();
    clearStaging();
    closeContext();
}

} // namespace pipeline
