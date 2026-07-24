#pragma once

#include "pipeline/core/Caps.h"
#include "pipeline/core/Event.h"
#include "pipeline/core/MessageBus.h"
#include "pipeline/core/Pad.h"
#include "pipeline/core/Types.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace pipeline {

// 前向声明
class Pipeline;
class Graph;

// ===================================================================
// BaseNode: 节点抽象基类
//
// 生命周期回调（由 Pipeline/Graph 调用）：
//   onReady()      — 初始化不依赖上游格式的资源
//   onStop()       — 释放不要求节点工作线程亲和性的资源（join 线程后调用）
//   runLoop()      — 工作线程主循环；Caps、Buffer、EOS 都在同一 Route 中有序处理
//
// 数据分发（子类调用，不感知下游数量）：
//   publishOutputItem()  — 向一个逻辑 OutputRoute 可靠发布有序 QueueItem（Caps/Buffer/EOS）
//   pushToDownstream()   — publishOutputItem 的 BufferRef 便捷入口
//   sendCapsEvent()      — 向指定逻辑 Route 可靠发布完整、准确的 CapsEvent
//   applyCapsEvent()     — 校验并将收到的 Caps 设为指定 SinkPad 的 active Caps
//   sendEOSDownstream()  — 向每条逻辑 Route 可靠发布一次 EOS
//   postMessage()        — 统一上报 MessageBus
//
// 节点生命周期由 stop_requested_ + MessageBus 表达，不维护独立的 NodeState：
//   - 正常退出：Pipeline::stop() 置 stop_requested_，runLoop 循环退出
//   - 出错退出：postMessage(ERROR) 同步置 stop_requested_，Pipeline 从 MessageBus 收集 last_error_
//   - 节点请求停止：postMessage(STOP_REQUESTED) 同步置 stop_requested_，Pipeline 记录请求并唤醒 waitEOS
// ===================================================================
class BaseNode {
public:
    virtual ~BaseNode() = default;

    const std::string& name()  const { return name_; }
    virtual NodeType   nodeType() const = 0;

    // Pad 访问
    SrcPad*  getSrcPad(const std::string& name);
    SinkPad* getSinkPad(const std::string& name);
    const std::vector<std::unique_ptr<SrcPad>>&  srcPads()  const { return src_pads_; }
    const std::vector<std::unique_ptr<SinkPad>>& sinkPads() const { return sink_pads_; }

protected:
    // 所有子类必须在初始化列表里调用此构造函数
    // 不提供默认构造，忘记调用会编译报错，避免 name_ 为空
    explicit BaseNode(const std::string& name) : name_(name) {}

    // 初始化不依赖上游格式的自身资源。运行期 Caps 在 runLoop 内有序处理。
    // 返回 true 成功，false 失败
    virtual bool onReady() = 0;

    // 释放不要求节点工作线程亲和性的资源（Pipeline join 完线程后按拓扑逆序调用）
    // 线程亲和性资源由具体节点在 runLoop() 退出前释放
    virtual void onStop() = 0;

    // 应用一份运行期完整输入 Caps。BaseNode 在此回调成功后才更新 active_caps_。
    // Transform 可在此填充重配前必须先发布的 delayed 输出；Sink 传 nullptr。
    virtual bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                        std::vector<QueueItem>* outputs) { return true; }

    // 工作线程主循环（由 Pipeline 创建的线程调用）
    virtual void runLoop() = 0;

    // 向一个逻辑 Route 按序发布本地 QueueItem。BufferRef 必须显式 move，入口无条件接管。
    bool publishOutputItem(QueueItem&& item, const std::string& src_pad_name = "");

    // Buffer 发布的便捷入口；调用方必须显式 move，失败路径同样负责释放。
    bool pushToDownstream(BufferRef&& buf, const std::string& src_pad_name = "");

    // 向指定 SrcPad 的下游发送完整、准确的 CapsEvent；它必须先于受其管辖的第一个 Buffer。
    bool sendCapsEvent(const std::string& src_pad_name, const CapsEvent& caps);

    // 向每条不同的逻辑 Route 可靠发布一次 EOS。
    bool sendEOSDownstream();

    // 校验并应用一个从指定 SinkPad Route 收到的完整 CapsEvent，成功后更新 active Caps。
    // Transform 可传 outputs 收集重配前须先发布的有序输出；调用方完成发布后才 ack Delivery。
    bool applyCapsEvent(const std::string& sink_pad_name, const CapsEvent& caps,
                        std::vector<QueueItem>* outputs = nullptr);

    // 消息上报：统一上报 MessageBus
    void postMessage(MessageType type, const std::string& text, int code = 0);

    // Pad 管理
    // 添加新的逻辑输出 Route，并创建其首个 SrcPad。
    SrcPad*  addSrcPad(const std::string& name, TemplateCaps caps);

    // 为已有逻辑输出创建一个新的分叉 SrcPad；新 Pad 与 source_pad 共享 Route。
    SrcPad*  addBranchedSrcPad(const std::string& name, const SrcPad& source_pad);
    SinkPad* addSinkPad(const std::string& name, TemplateCaps caps);

    // 动态请求 Pad，需要支持分叉的节点需要重写对应的方法
    virtual SrcPad*  requestSrcPad(const std::string& name, MediaType hint_type) { return nullptr; }
    virtual SinkPad* requestSinkPad(const std::string& name, MediaType hint_type) { return nullptr; }

    // 释放动态请求的 Pad，若后续步骤失败，只对本次新建的 Pad 调用 release
    // release 默认只删除尚未连接的 Pad，有附属状态的节点可重写并同步清理
    virtual bool releaseSrcPad(SrcPad* pad);
    virtual bool releaseSinkPad(SinkPad* pad);

    // 成员
    std::string name_;
    Pipeline* pipeline_ = nullptr;
    std::vector<std::unique_ptr<SrcPad>> src_pads_;
    std::vector<std::unique_ptr<SinkPad>> sink_pads_;
    // 每个输入 Pad 最近一次成功应用的完整 Caps。它是该 Pad 后续 Buffer 的唯一格式权威。
    std::unordered_map<std::string, CapsEvent> active_caps_;
    std::atomic<bool> stop_requested_{false};

    friend class Pipeline;
    friend class Graph;
};

// ===================================================================
// SourceNode: 采集源节点
//
// 无 SinkPad，只有 SrcPad。
// runLoop 循环调用 capture() 采集数据，push 到下游。
// capture() 返回 nullptr 表示 EOF。
// ===================================================================
class SourceNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::SOURCE; }

protected:
    explicit SourceNode(const std::string& name) : BaseNode(name) {}
    void runLoop() override;

    // 子类实现：阻塞采集一帧数据，返回 nullptr 表示 EOF
    virtual Buffer* capture() = 0;

    // 支持分叉，新 SrcPad 是已有 SrcPad 的同源多路拷贝，能力集合必须与已有 pad 一致
    // hint_type 只用于校验"这次 link 想承载的类型是否落在已有能力集合内"，不参与 TemplateCaps 构造。
    SrcPad* requestSrcPad(const std::string& name, MediaType hint_type) override {
        if (!src_pads_.empty()) {
            // 如果已有 SrcPad，复制完整能力集合
            const auto& existing = src_pads_[0]->templateCaps();
            if (!existing.contains(hint_type)) {
                return nullptr;
            }
            return addBranchedSrcPad(name, *src_pads_[0]);
        }
        // 如果是首个 pad，从 hint_type 建立最初的能力集合
        return addSrcPad(name, TemplateCaps{{hint_type}});
    }
};

// ===================================================================
// SinkNode: 消费终端节点
//
// 只有 SinkPad，无 SrcPad。
// runLoop 从 SinkPad pop 数据并调用 consume()。
// 收到 EOS 时通过 postMessage 通知 Pipeline。
// ===================================================================
class SinkNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::SINK; }

protected:
    explicit SinkNode(const std::string& name) : BaseNode(name) {}
    void runLoop() override;

    // 子类实现：消费一帧数据；调用前基类已保证该 SinkPad 存在 active Caps。
    virtual void consume(const Buffer* buf) = 0;

    // 输出侧 drain，默认空实现，子类（如 AudioPlayNode）可重写
    // 收到上游 EOS 之后，上报最终 EOS 前调用，用于等待输出真正完成（如 AudioPlayNode 要等 SDL 设备播完尾音）
    virtual void onDrain() {}
};

// ===================================================================
// TransformNode: 处理节点
//
// 有 SinkPad 和 SrcPad。
// runLoop 从 SinkPad pop 数据，调用 process() 处理，push 到下游。
// ===================================================================
class TransformNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::TRANSFORM; }

protected:
    explicit TransformNode(const std::string& name) : BaseNode(name) {}

protected:
    void runLoop() override;

    // 一个输入映射为有序 Route 项。Caps 必须排在其管辖 Buffer 的前面；EOS flush
    // 必须把尾帧及其 EOS 都放入同一序列。新建 Buffer 立即由 BufferRef 接管。
    virtual void process(const Buffer* input, std::vector<QueueItem>& outputs) = 0;

    // 应用一份运行期完整输入 Caps。Decoder 可将重配前必须先发布的 delayed 输出填入 outputs。
    bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override;

    // 收到输入 EOS 后仅收集仍须发布的有序输出（如 Decode 的 delayed frames）。
    // 框架 runLoop 无条件在成功返回后向序列末尾追加唯一 EOSEvent，再发布并 ack 输入 EOS。
    // 子类绝不负责转发或追加 EOSEvent，因而不能遗漏终结事件。
    virtual void onEOS(std::vector<QueueItem>& outputs) {}

    // 支持分叉，新 SrcPad 是已有 SrcPad 的同源多路拷贝，能力集合必须与已有 pad 一致
    // hint_type 只用于校验"这次 link 想承载的类型是否落在已有能力集合内"
    SrcPad* requestSrcPad(const std::string& name, MediaType hint_type) override {
        if (!src_pads_.empty()) {
            const auto& existing = src_pads_[0]->templateCaps();
            if (!existing.contains(hint_type)) {
                return nullptr;
            }
            return addBranchedSrcPad(name, *src_pads_[0]);   // 复制完整能力集合
        }
        return addSrcPad(name, TemplateCaps{{hint_type}});
    }
};


/**
 * @brief Demux 具体类的显式探测结果
 */
struct DemuxProbeResult {
    std::optional<CapsEvent> video;
    std::optional<CapsEvent> audio;
};


enum class DemuxReadStatus {
    BUFFER,
    END_OF_STREAM,
    CANCELLED,
    ERROR,
};

/**
 * @brief 表示 Demux 具体类单次读取的显式结果
 */
struct DemuxReadResult {
    DemuxReadStatus status;
    BufferRef buffer;
};

// ===================================================================
// DemuxNode: 解复用基类
//
// 子类只需实现 openInput / closeInput / probeStreams / readFrame 四个钩子，
// 基类统一处理：不可变输入 URL、动态 SrcPad 管理、流存在性校验、CapsEvent 发送、
// 按 media_type 分发的多路广播、EOS 传播。
//
// 无 FFmpeg 依赖：具体格式解析（AVFormatContext 等）留在 AVDemuxNode 实现。
// ===================================================================
class DemuxNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::DEMUX; }

protected:
    DemuxNode(const std::string& name, std::string url)
        : BaseNode(name), url_(std::move(url)) {}

    // 子类必须实现的格式相关钩子

    // 打开构造时确定的输入源。失败时应 postMessage(ERROR) 并返回 false。
    virtual bool openInput(const std::string& url) = 0;

    // 关闭输入源
    virtual void closeInput() = 0;

    // 探测一路最佳视频和一路最佳音频，并通过 result 显式返回
    // 返回 true 时，nullopt 表示输入中不存在对应媒体类型
    // 探测过程本身失败仍由 probeStreams() 返回 false 表达，具体类应先 postMessage(ERROR)
    virtual bool probeStreams(DemuxProbeResult* result) = 0;

    // 读取下一帧选中的编码流；无关流由具体类内部循环跳过
    // BUFFER：必须携带有效 BufferRef；END_OF_STREAM：基类传播 EOS；
    // CANCELLED：主动取消，静默退出；ERROR：具体类应先 postMessage(ERROR)。
    virtual DemuxReadResult readFrame() = 0;

    // 基类统一生命周期（子类不要重写）
    bool onReady() override final;
    void runLoop() override final;
    void onStop() override final;

    // 动态创建 SrcPad：只接受 VIDEO_ENCODED / AUDIO_ENCODED
    SrcPad* requestSrcPad(const std::string& name, MediaType hint_type) override;

    // 回滚失败 link 创建的 SrcPad，并同步清理 pad_to_type_。
    bool releaseSrcPad(SrcPad* pad) override;

    // 输入地址在构造后不可变；本地路径和网络 URL 均由具体类解释。
    const std::string url_;

    // 由基类验证并保存的正式探测结果；具体类只能通过 probeStreams() 返回。
    DemuxProbeResult probe_result_;

private:
    // link 阶段记录的 Pad 请求类型，仅供 Ready 阶段校验流和选择 Caps
    std::unordered_map<std::string, MediaType> pad_to_type_;
};

// ===================================================================
// MuxNode: 复用基类
//
// 子类只需实现 allocateContext / addStream / writeHeader / writePacket /
// writeTrailer / closeContext 六个格式相关钩子。基类统一处理：动态 SinkPad、
// 固定单路 CONTAINER 输出 out_0、CapsEvent 收发、容器字节暂存与发送、
// 多路输入监听、EOS 汇合及向下游传播。
//
// appendContainerBytes() 是具体 Mux 后端的输出入口：未来 AVMuxNode 的
// AVIO callback 将临时字节复制到 pending_output_；基类在 runLoop 中再将
// pending 字节作为 CONTAINER Buffer 阻塞发送，避免 Ready 阶段无消费者死锁。
//
// 具体容器封装（AVFormatContext / AVIOContext 等）留在 AVMuxNode 实现
// ===================================================================
class MuxNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::MUX; }

protected:
    MuxNode(const std::string& name, MuxFormat format)
        : BaseNode(name), format_(format) {
        addSrcPad("out_0", TemplateCaps{{MediaType::CONTAINER}});
    }

    // === 子类必须实现的格式相关钩子 ===

    // 按构造时确定的格式分配输出上下文。失败前由具体类上报 ERROR。
    virtual bool allocateContext(MuxFormat format) = 0;

    // 根据 CapsEvent 创建一路具体后端输出流。
    // stream_index 是抽象的输出流序号：基类只保存并原样回传，不解释其值；
    // AVMuxNode 可用 AVStream::index 作为该序号。失败前由具体类上报 ERROR。
    virtual bool addStream(const CapsEvent& caps, int* stream_index) = 0;

    // 写文件头；失败前由具体类上报 ERROR。
    virtual bool writeHeader() = 0;

    // 写一帧。stream_index 是 addStream() 返回的抽象输出流序号；
    // buf 只读且所有权仍归基类。失败前由具体类上报 ERROR。
    virtual bool writePacket(const Buffer* buf, int stream_index) = 0;

    // 写文件尾；失败前由具体类上报 ERROR。
    virtual bool writeTrailer() = 0;

    // 关闭并释放输出上下文，必须支持部分初始化状态。
    virtual void closeContext() = 0;

    // 具体 Mux 后端的字节输出入口。只复制到 pending，不直接 push Queue。
    bool appendContainerBytes(const uint8_t* data, size_t size);

    // === 基类统一生命周期（子类不要重写）===
    bool onReady() override final;
    void runLoop() override final;
    void onStop() override final;

    // 动态创建 SinkPad：只接受 VIDEO_ENCODED / AUDIO_ENCODED
    SinkPad* requestSinkPad(const std::string& name, MediaType hint_type) override;

private:
    // 将 pending 字节作为一个 CONTAINER Buffer 阻塞发送到固定 out_0。
    bool flushPendingOutput();
    bool configureInitialInput(const std::string& pad_name, const CapsEvent& caps);
    bool writeHeaderAfterAllInputsConfigured();

    // 多路复用辅助
    SinkPad* waitAnyPadReady();
    SinkPad* selectMinDtsPad();

    // pad_name -> 具体后端返回的抽象输出流序号，基类不解释该整数
    std::unordered_map<std::string, int> pad_to_stream_;
    std::unordered_set<std::string> initial_caps_pads_;

    // 已收到 EOS 的输入 Pad。Mux 在每路初始 Caps 后才允许处理其 EOS。
    std::unordered_set<std::string> eos_pads_;
    bool header_written_ = false;

    // 格式钩子产生、尚未发给下游的容器字节。
    std::vector<uint8_t> pending_output_;

    // 多路复用等待
    std::mutex mux_mutex_;
    std::condition_variable mux_cv_;

    // 输出格式构造后不可变；MP4 的项目语义固定为 fragmented MP4。
    const MuxFormat format_;
};

} // namespace pipeline
