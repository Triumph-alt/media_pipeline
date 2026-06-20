#pragma once

namespace pipeline {

// ===================================================================
// MediaType: 数据流类型（5 种，区分编码前后）
//
// V2 核心设计：区分 RAW 和 ENCODED，Build 阶段即可检出
// 错误拓扑（如 DemuxNode 直连 VideoRenderNode 会在构建期报错）
// ===================================================================
enum class MediaType {
    VIDEO_RAW,       // 解码后视频帧（YUV/RGB）
    AUDIO_RAW,       // 解码后音频帧（PCM）
    VIDEO_ENCODED,   // 编码后视频 Packet（H264/H265）
    AUDIO_ENCODED,   // 编码后音频 Packet（AAC/Opus）
    CONTAINER,       // 容器封装后数据（Mux 输出）
};

// ===================================================================
// NodeType: 节点类型（5 类）
//
// SOURCE    — 0 SinkPad，动态 SrcPad，独立采集驱动
// SINK      — 动态 SinkPad，0 SrcPad，独立消费驱动
// TRANSFORM — 1 SinkPad，动态 SrcPad，队列驱动
// DEMUX     — 0/1 SinkPad，动态 SrcPad，文件/URL 驱动（懒连接）
// MUX       — 动态 SinkPad，1 SrcPad，多路复用监听
// ===================================================================
enum class NodeType {
    SOURCE,
    SINK,
    TRANSFORM,
    DEMUX,
    MUX,
};

// ===================================================================
// NodeState: 节点生命周期状态
// ===================================================================
enum class NodeState {
    NULL_STATE,   // 资源未分配
    READY,        // 资源已分配（onReady 成功）
    RUNNING,      // 工作线程运行中
    ERROR,        // 出错
};

// ===================================================================
// PipelineState: Pipeline 整体生命周期
//
// NULL_STATE ──(build)──→ BUILT ──(play)──→ RUNNING ──(stop/EOS)──→ STOPPED
// ===================================================================
enum class PipelineState {
    NULL_STATE,
    BUILT,
    RUNNING,
    STOPPED,
    ERROR,
};

// ===================================================================
// PadDir: Pad 方向
// ===================================================================
enum class PadDir {
    SRC,
    SINK,
};

} // namespace pipeline
