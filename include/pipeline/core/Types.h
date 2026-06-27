#pragma once

namespace pipeline {

// ===================================================================
// MediaType: 数据流类型（5 种，区分编码前后）
//
// 全局类型，多个模块共用（Buffer、Caps、Pad、BaseNode、Graph、Edge、Pipeline）。
// V2 核心设计：区分 RAW 和 ENCODED，Build 阶段即可检出
// 错误拓扑（如 DemuxNode 直连 VideoRenderNode 会在构建期报错）。
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
// 全局类型，BaseNode、Graph、Pipeline 共用。
// ===================================================================
enum class NodeType {
    SOURCE,
    SINK,
    TRANSFORM,
    DEMUX,
    MUX,
};

} // namespace pipeline
