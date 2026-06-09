# Media Pipeline Framework 设计文档

## 1. 项目概述

### 1.1 目标

构建一个轻量级的跨 Linux 平台媒体管线框架（Media Pipeline Framework），核心设计参考 GStreamer 的 Pipeline 架构思想，但大幅简化，聚焦于实际常用的媒体处理场景。

### 1.2 核心理念

- **节点即对象**：每个功能单元（采集、解复用、编码、解码、渲染、推流等）是一个独立的 Node 对象
- **自由组合**：用户通过 link 将节点串联，不同组合实现不同功能（播放器、推流器、录制器、转码器等）
- **极简 API**：用户只需几行代码即可构建完整的媒体处理管线
- **跨架构**：同一份源码可编译为 x86_64 和 ARM64 的二进制

### 1.3 依赖

| 库 | 用途 |
|---|------|
| FFmpeg (libavformat, libavcodec, libavutil, libswscale, libswresample) | 解复用、复用、编解码、像素/音频格式转换 |
| SDL3 | 视频渲染、音频播放 |
| V4L2 (内核接口) | 视频采集 |
| ALSA (libasound) | 音频采集 |
| CMake 3.16+ | 构建系统 |

### 1.4 不做的事情（初版）

- 不支持 Windows / macOS
- 不支持动态插件加载（.so）
- 不支持硬件编解码加速（VAAPI/NVENC/V4L2 M2M）
- 不支持 DMA-BUF 零拷贝
- 不支持嵌入式 Linux（OpenWrt/Buildroot）
- 不支持 RTSP 推流（后期加入）
- 不支持运行时格式变化（STREAM_INFO_CHANGED 运行时处理）

---

## 2. 整体架构

### 2.1 三层抽象

```text
Pipeline（管线）
└── Node（节点）
└── Pad（端口）
```

| 层级 | 职责 |
|------|------|
| **Pipeline** | 持有所有节点，管理全局状态机、时钟、消息总线、线程生命周期 |
| **Node** | 功能执行单元，每个节点完成一种媒体处理操作 |
| **Pad** | 节点间的输入/输出接口，负责数据传递和格式协商 |

### 2.2 数据流模型

**默认 Push 模型**：上游节点处理完数据后主动向下游 Push Buffer，下游被动接收。

```text
[SourceNode] --push--> [TransformNode] --push--> [SinkNode]
```

背压通过 Pad 内置的有界队列自然传导：当下游队列满时，上游的 push 调用阻塞。

### 2.3 组合示例

```text
播放本地文件（音视频）：
Demux(url) ──video_0──→ Decode ──→ VideoRender
──audio_0──→ Decode ──→ AudioPlay

采集预览：
V4L2Capture ──→ VideoRender

采集编码录制：
V4L2Capture ──→ Encode ──→ Mux ──→ FileSink
AudioCapture──→ Encode ──→ Mux

文件推流（不转码）：
Demux(url) ──video_0──→ Mux ──→ RTSPPush
──audio_0──→ Mux

文件推流（转码）：
Demux(url) ──video_0──→ Decode ──→ Encode ──→ Mux ──→ RTSPPush
──audio_0──→ Decode ──→ Encode ──→ Mux

纯音频：
AudioCapture ──→ AudioPlay

文件录制（仅视频）：
V4L2Capture ──→ Encode ──→ Mux ──→ FileSink
```

## 3. 类层次结构

### 3.1 继承关系

```text
INode（抽象基类）
│
├── SourceNode（无 SinkPad，只有 SrcPad）
│ ├── V4L2CaptureNode V4L2 视频采集
│ └── AudioCaptureNode ALSA 音频采集
│
├── TransformNode（有 SinkPad + SrcPad）
│ ├── DemuxNode FFmpeg 解复用（SinkPad 可选）
│ ├── MuxNode FFmpeg 复用（N 个动态 SinkPad → 1 个 SrcPad）
│ ├── DecodeNode FFmpeg 解码
│ ├── EncodeNode FFmpeg 编码
│ └── QueueNode 缓冲队列节点
│
└── SinkNode（只有 SinkPad，无 SrcPad）
├── VideoRenderNode SDL3 视频渲染
├── AudioPlayNode SDL3 音频播放
├── FileSinkNode 写入文件
└── RTSPPushNode RTSP 推流（后期）
```

### 3.2 INode（抽象基类）

```cpp
class INode {
public:
    virtual ~INode() = default;

    // ===== 基本信息 =====
    const std::string& name() const;
    NodeState state() const;

    // ===== 参数 =====
    using ParamValue = std::variant<int, int64_t, float, bool, std::string, AVRational>;
    void setParam(const std::string& key, ParamValue value);
    template<typename T> T getParam(const std::string& key, T defaultValue = T{}) const;
    bool hasParam(const std::string& key) const;

    // ===== Pad =====
    std::vector<SrcPad*> srcPads() const;
    std::vector<SinkPad*> sinkPads() const;
    virtual SrcPad* getSrcPad(const std::string& name = "out");
    virtual SinkPad* getSinkPad(const std::string& name = "in");
    virtual SinkPad* requestSinkPad(const std::string& name);  // 默认返回 nullptr

    // ===== 连接 =====
    INode* link(INode* downstream,
                const std::string& srcPadName = "",
                const std::string& sinkPadName = "");

    // ===== 生命周期（由 Pipeline 调用）=====
    void probe();             // Phase 2: 轻量级探测，创建 Pad
    void ready();             // Phase 5: 重量级资源分配
    void createThread();      // Phase 6: 创建工作线程
    void setState(NodeState s); // Phase 7: 通知线程开始
    void waitThreadExit();    // 等待线程退出
    void null();              // 释放所有资源

protected:
    // ===== 子类必须/可重写的虚函数 =====
    virtual void onProbe() {}                     // 创建 Pad
    virtual void onReady() = 0;                   // 分配资源
    virtual void onNull() = 0;                    // 释放资源
    virtual void onPlaying() {}                   // 线程开始工作
    virtual void onPause() {}                     // 线程暂停
    virtual void onLink(SinkPad* pad, const StreamInfo& info) {}
    virtual void onEvent(std::shared_ptr<Event> event, Pad* from) {}

    // ===== 工作循环 =====
    virtual void workerLoop();  // 工作线程主循环，子类可重写

    // ===== 内部成员 =====
    std::string m_name;
    NodeState m_state = NodeState::NULL_STATE;
    Pipeline* m_pipeline = nullptr;
    MessageBus* m_bus = nullptr;
    std::unordered_map<std::string, ParamValue> m_params;
    std::vector<std::unique_ptr<SrcPad>> m_srcPads;
    std::vector<std::unique_ptr<SinkPad>> m_sinkPads;
    std::thread m_workerThread;
    std::mutex m_stateMutex;
    std::condition_variable m_stateCV;
    bool m_stopRequested = false;
};
```

### 3.3 SourceNode

```cpp
class SourceNode : public INode {
protected:
    void workerLoop() override;  // 生成数据 → push 到 SrcPad
    virtual std::shared_ptr<Buffer> generateData() = 0;  // 子类实现
    virtual bool isEOF() { return false; }
};
```

### 3.4 TransformNode

```cpp
class TransformNode : public INode {
protected:
    void workerLoop() override;  // pop → process → push
    virtual std::shared_ptr<Buffer> process(std::shared_ptr<Buffer> input) = 0;
    virtual void handleEvent(std::shared_ptr<Event> event, SinkPad* from) {}
};
```

### 3.5 SinkNode

```cpp
class SinkNode : public INode {
protected:
    void workerLoop() override;  // pop → consume
    virtual void consume(std::shared_ptr<Buffer> buffer) = 0;
    virtual void handleEOS() {}  // 收到 EOS 时的处理
};
```

### 3.6 DemuxNode 特殊说明

DemuxNode 继承 TransformNode，但 SinkPad 为可选：
无上游连接时：通过 setParam("url", ...) 设置 URL，内部自行调用 avformat_open_input + av_read_frame，行为等同于 SourceNode
有上游连接时：SinkPad 接收上游推来的容器数据进行解复用

```cpp
class DemuxNode : public TransformNode {
    enum Mode { FROM_URL, FROM_UPSTREAM };
    Mode m_mode;
    AVFormatContext* m_fmtCtx = nullptr;

public:
    // 参数: "url" : std::string
    // 输出 Pad: "video_0", "audio_0", ...（根据文件流动态创建）

    void onProbe() override {
        // 读取用户设置的 url 参数
        // avformat_open_input + avformat_find_stream_info
        // 遍历 streams，为每个流创建 SrcPad 并填充 StreamInfo
    }

    void onReady() override {
        // 检查是否有上游连接
        if (!m_sinkPads.empty() && m_sinkPads[0]->isConnected()) {
            m_mode = FROM_UPSTREAM;
        } else {
            m_mode = FROM_URL;
            // 资源已在 onProbe 分配
        }
    }

    // workerLoop 根据 m_mode 选择驱动方式
};
```

### 3.7 MuxNode 特殊说明

MuxNode 继承 TransformNode，SinkPad 动态创建：

```cpp
class MuxNode : public TransformNode {
    AVFormatContext* m_fmtCtx = nullptr;
    AVIOContext* m_avioCtx = nullptr;
    int m_sinkPadCount = 0;

public:
    // 参数: "format" : std::string — "mp4", "flv", "mpegts" 等

    SinkPad* requestSinkPad(const std::string& name) override {
        // 检查是否已存在
        for (auto& pad : m_sinkPads) {
            if (pad->name() == name) return pad.get();
        }
        // 动态创建
        auto pad = std::make_unique<SinkPad>(name, this);
        m_sinkPads.push_back(std::move(pad));
        return m_sinkPads.back().get();
    }

    void onLink(SinkPad* pad, const StreamInfo& info) override {
        // 收集每个 SinkPad 的 StreamInfo
        // 等 onReady 时统一初始化输出容器
    }

    void onReady() override {
        // avformat_alloc_output_context2
        // 为每个 SinkPad 的 StreamInfo 调用 avformat_new_stream
        // 创建自定义 AVIOContext (write callback → push to SrcPad)
        // avformat_write_header
    }

    // workerLoop: 轮询所有 SinkPad，选 DTS 最小的 buffer
    // → av_interleaved_write_frame → write callback → push to SrcPad
};
```

## 4. 状态机

框架有两套独立的状态机，分别描述 Pipeline 整体和单个 Node 的状态。

### 4.1 PipelineState —— Pipeline 整体生命周期

```cpp
enum class PipelineState : uint8_t {
    NULL_STATE,   // 未创建
    READY,        // 节点已创建、已连接、资源已分配
    PAUSED,       // 工作线程已创建，等待中
    PLAYING,      // 正常运行
    STOPPED,      // 已停止（终态，waitForStop 等待此状态）
    ERROR,        // 异常
};
```

```text
NULL_STATE ──(play)──→ READY ──(play)──→ PAUSED ──(play)──→ PLAYING
                                                               │
                                                       (stop / EOS)
                                                               ↓
NULL_STATE ←──(destroy)── STOPPED ←────────────────────────────┘
```

| 状态 | Pipeline 行为 |
|------|--------------|
| **NULL_STATE** | 未创建/已销毁 |
| **READY** | 节点已 probe、已连接 |
| **PAUSED** | 节点线程已创建，等待中 |
| **PLAYING** | 正常运行 |
| **STOPPED** | 已停止，waitForStop() 等待此状态 |
| **ERROR** | 异常 |

### 4.2 NodeState —— 单个节点的状态

```cpp
enum class NodeState : uint8_t {
    NULL_STATE,   // 资源未分配
    READY,        // 资源已分配，线程未创建
    PAUSED,       // 线程存在但等待中
    PLAYING,      // 正常运行
    ERROR,        // 异常
};
```

NodeState 没有 STOPPED。stop 执行完成后，节点直接回到 NULL_STATE（资源已释放）。

```text
NULL_STATE → READY:    onReady()          // 分配资源
READY → PAUSED:        createThread()     // 创建线程，线程立即 wait
PAUSED → PLAYING:      setState(PLAYING)  // notify 线程开始
PLAYING → PAUSED:      onPause()          // 通知线程暂停
PAUSED → READY:        waitThreadExit()   // 等待线程退出
READY → NULL_STATE:    onNull()           // 释放资源
```

### 4.3 两套状态机的对应关系

```text
PipelineState          NodeState
─────────────          ─────────
NULL_STATE             —（节点不存在）
READY                  NULL_STATE（节点还没 probe）
PAUSED                 PAUSED（节点线程创建，等待中）
PLAYING                PLAYING（节点线程运行）
STOPPED                NULL_STATE（节点资源已释放）
```

Pipeline 状态转换时同步驱动节点状态转换，但两者的终态不同——Pipeline 停在 STOPPED，节点回到 NULL_STATE。

### 4.4 转换顺序

**进入 PLAYING（关键：Sink 先就绪，Source 最后启动）：**

```
SinkNode → PAUSED → PLAYING
TransformNode → PAUSED → PLAYING
SourceNode → PAUSED → PLAYING
```

**退出 PLAYING（Source 先停止）：**

```
SourceNode → NULL_STATE
TransformNode → NULL_STATE
SinkNode → NULL_STATE
```

此顺序保证上游开始推数据时下游已准备就绪，不会丢数据。

## 5. 线程模型

### 5.1 方案：每节点独立线程

每个 Node 实例拥有一个独立的 `std::thread`：

- 线程在 `READY → PAUSED` 时创建
- 线程在 `PAUSED → READY` 时销毁
- 线程在 PAUSED 状态下等待（`condition_variable`），在 PLAYING 状态下运行

### 5.2 线程数量估算

| 场景               | 节点数                                              | 线程数 |
| ------------------ | --------------------------------------------------- | ------ |
| 文件播放（音视频） | Demux + Decode×2 + Render + AudioPlay = 5           | 5      |
| 采集预览           | Capture + Render = 2                                | 2      |
| 采集编码录制       | V4L2 + AudioCapture + Encode×2 + Mux + FileSink = 6 | 6      |
| 文件推流（不转码） | Demux + Mux + RTSPPush = 3                          | 3      |
| 文件推流（转码）   | Demux + Decode×2 + Encode×2 + Mux + RTSPPush = 7    | 7      |

最多 7 个线程，完全可控。

### 5.3 SourceNode 工作循环

```cpp
void SourceNode::workerLoop() {
    while (true) {
        std::unique_lock lock(m_stateMutex);
        m_stateCV.wait(lock, [this] {
            return m_state == NodeState::PLAYING || m_stopRequested;
        });
        if (m_stopRequested) break;
        lock.unlock();

        auto buffer = generateData();  // 子类实现

        if (buffer) {
            m_srcPads[buffer->streamIndex]->push(buffer);
        }

        if (isEOF()) {
            for (auto& pad : m_srcPads) {
                pad->pushEvent(Event::makeEOS());
            }
            break;
        }
    }
}
```

### 5.4 TransformNode 工作循环

```cpp
void TransformNode::workerLoop() {
    while (true) {
        std::unique_lock lock(m_stateMutex);
        m_stateCV.wait(lock, [this] {
            return m_state == NodeState::PLAYING || m_stopRequested;
        });
        if (m_stopRequested) break;
        lock.unlock();

        auto result = m_sinkPads[0]->pop(std::chrono::milliseconds(100));

        if (result.hasBuffer()) {
            auto output = process(result.buffer());
            if (output) {
                m_srcPads[0]->push(output);
            }
        } else if (result.hasEvent()) {
            handleEvent(result.event(), m_sinkPads[0].get());
        }
        // 超时 → 继续循环，重新检查状态
    }
}
```

### 5.5 SinkNode 工作循环

```cpp
void SinkNode::workerLoop() {
    while (true) {
        std::unique_lock lock(m_stateMutex);
        m_stateCV.wait(lock, [this] {
            return m_state == NodeState::PLAYING || m_stopRequested;
        });
        if (m_stopRequested) break;
        lock.unlock();

        auto result = m_sinkPads[0]->pop(std::chrono::milliseconds(100));

        if (result.hasBuffer()) {
            consume(result.buffer());  // 子类实现
        } else if (result.hasEvent()) {
            if (result.event()->type() == Event::Type::EOS) {
                handleEOS();
                m_pipeline->reportSinkEOS(this);
                // 不 break，等 Pipeline 统一 stop
            }
        }
    }
}
```

### 5.6 MuxNode 工作循环（多输入特殊处理）

```cpp
void MuxNode::workerLoop() {
    while (true) {
        // 检查状态...

        // 从所有 SinkPad 中选 DTS 最小的 buffer
        std::shared_ptr<Buffer> earliest = nullptr;
        int earliestPadIdx = -1;

        for (int i = 0; i < m_sinkPads.size(); i++) {
            auto peek = m_sinkPads[i]->peek();
            if (peek && (!earliest || peek->dts < earliest->dts)) {
                earliest = peek;
                earliestPadIdx = i;
            }
        }

        if (earliest) {
            auto buf = m_sinkPads[earliestPadIdx]->pop();
            // av_interleaved_write_frame 写入
            // 自定义 AVIOContext write callback → push to SrcPad
            muxAndWrite(buf);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}
```

## 6. Pad 架构

### 6.1 Pad 类层次

```text
Pad（基类）
 ├── direction: SRC / SINK
 ├── mediaType: VIDEO / AUDIO / ANY
 ├── peer: Pad*
 ├── streamInfo: StreamInfo
 ├── state: ACTIVE / FLUSHING
 │
 ├── SrcPad
 │    ├── queue: BoundedQueue<variant<Buffer, Event>>
 │    ├── push(shared_ptr<Buffer>)
 │    ├── pushEvent(shared_ptr<Event>)
 │    ├── canPush() → bool          // 非阻塞检查队列是否有空间
 │    └── overflowPolicy: BLOCK / DROP_OLDEST / DROP_NEWEST
 │
 └── SinkPad
      ├── queue: BoundedQueue<variant<Buffer, Event>>
      ├── pop(timeout) → PopResult
      ├── peek() → shared_ptr<Buffer>
      └── connected: bool
```

### 6.2 Pad 连接过程

`srcPad->connect(sinkPad)` 执行流程：

1. 1.**方向检查**：`srcPad.direction == SRC && sinkPad.direction == SINK`
2. 2.**类型检查**：`srcPad.mediaType` 兼容 `sinkPad.mediaType`（VIDEO↔VIDEO，AUDIO↔AUDIO，ANY 通配）
3. 3.**建立双向引用**：`srcPad.peer = sinkPad; sinkPad.peer = srcPad`
4. 4.**传递 StreamInfo**：如果 srcPad 已有 StreamInfo，复制给 sinkPad
5. 5.**触发回调**：`sinkPad.node->onLink(sinkPad, streamInfo)`

### 6.3 静态 Pad 与请求 Pad

| 类型            | 说明                   | 例子                             |
| --------------- | ---------------------- | -------------------------------- |
| **Static Pad**  | probe 时创建，名字固定 | DecodeNode 的 "in"/"out"         |
| **Request Pad** | 连接时按需动态创建     | MuxNode 的 "video_in"/"audio_in" |

`resolvePendingLinks()` 中获取 SinkPad 的逻辑：

```cpp
SinkPad* sinkPad = nullptr;
if (sinkNode->hasSinkPad(sinkPadName)) {
    sinkPad = sinkNode->getSinkPad(sinkPadName);     // 静态 Pad
} else {
    sinkPad = sinkNode->requestSinkPad(sinkPadName); // 请求 Pad
}
```

### 6.4 BoundedQueue

```cpp
template<typename T>
class BoundedQueue {
    std::queue<T> m_queue;
    std::mutex m_mutex;
    std::condition_variable m_notEmpty;
    std::condition_variable m_notFull;
    size_t m_maxSize;
    OverflowPolicy m_policy;  // BLOCK / DROP_OLDEST / DROP_NEWEST

public:
    // push: BLOCK 策略下队列满时阻塞；DROP_OLDEST 下丢弃最老的；DROP_NEWEST 下丢弃新的
    bool push(T item, std::chrono::milliseconds timeout = std::chrono::milliseconds::max());

    // pop: 带超时，超时返回空
    PopResult pop(std::chrono::milliseconds timeout);

    // peek: 查看队列头部但不取出
    std::shared_ptr<Buffer> peek();

    bool canPush() const;  // 非阻塞检查是否有空间
    size_t size() const;
};
```

### 6.5 默认队列容量

| Pad 位置                          | 默认 maxBuffers | 默认溢出策略 | 说明                        |
| --------------------------------- | --------------- | ------------ | --------------------------- |
| SrcPad（Source 输出, video）      | 5               | BLOCK        | encoded packet 小，5 帧足够 |
| SrcPad（Source 输出, audio）      | 20              | BLOCK        | 音频帧更小，多缓些          |
| SrcPad（Transform 输出, video）   | 5               | BLOCK        | raw frame 大，少缓          |
| SrcPad（Transform 输出, encoded） | 10              | BLOCK        | encoded packet 适中         |
| MuxNode SrcPad                    | 50              | BLOCK        | muxed 数据块小              |
| QueueNode                         | 500             | 可配置       | 用户显式大缓冲              |

用户可通过 `setParam()` 覆盖默认值。

## 7. Buffer 设计

### 7.1 设计挑战

Pipeline 各阶段的数据大小跨度极大：

| 阶段 | 数据类型 | 典型大小 |
|------|---------|---------|
| 音频采集 | ALSA PCM (10ms, 48kHz stereo S16) | ~1.9 KB |
| 音频编码后 | AAC Packet | 0.5~8 KB |
| 容器封装后 | Muxed chunk (FLV/TS) | 1~10 KB |
| 视频编码后 | H.264 P/B 帧 | 5~100 KB |
| 视频编码后 | H.264 I 帧 | 100~500 KB |
| 视频采集 | V4L2 MJPEG 1080p | 100~500 KB |
| 视频解码后 | 720p YUV420 | ~1.4 MB |
| 视频采集 | V4L2 YUYV 1080p | ~4 MB |
| 视频解码后 | 1080p YUV420 | ~3 MB |
| 视频解码后 | 4K YUV420 | ~12 MB |

最小不到 1 KB，最大超过 10 MB，差距超过一万倍。如果全部使用默认 malloc/free，会出现：

1. **小块频繁分配**：音频 packet 每秒数百次，产生内存碎片
2. **大块反复分配**：raw frame 每帧 3~12 MB，分配/释放开销大，内存抖动

因此框架必须在设计阶段就引入**分级内存池（MemoryPool）**，而非后期优化。

### 7.2 分级定义

```cpp
enum class MemoryTier : uint8_t {
    TINY    = 0,    // < 4 KB          音频 packet, muxed chunk
    SMALL   = 1,    // 4 KB ~ 64 KB    视频 encoded packet (一般帧)
    MEDIUM  = 2,    // 64 KB ~ 512 KB  视频 encoded I帧, MJPEG 帧
    LARGE   = 3,    // 512 KB ~ 4 MB   720p/1080p raw YUV frame
    HUGE    = 4,    // > 4 MB          4K raw frame, YUYV 1080p 采集
    COUNT   = 5     // 非池管理标记（fallback 或外部内存）
};
```

自动分级函数：

```cpp
MemoryTier MemoryPool::tierForSize(size_t size) {
    if (size <= 4 * 1024)          return MemoryTier::TINY;
    if (size <= 64 * 1024)         return MemoryTier::SMALL;
    if (size <= 512 * 1024)        return MemoryTier::MEDIUM;
    if (size <= 4 * 1024 * 1024)   return MemoryTier::LARGE;
    return MemoryTier::HUGE;
}
```

### 7.3 每级池配置

```cpp
struct TierConfig {
    size_t blockSize;       // 该级别的固定块大小（向上对齐到 4K）
    size_t poolCapacity;    // 池中预分配的最大块数量
};

static constexpr TierConfig TIER_CONFIGS[MemoryTier::COUNT] = {
    //  blockSize             poolCapacity
    {   4 * 1024,              128     },   // TINY:   4KB × 128 = 512 KB
    {   64 * 1024,             64      },   // SMALL:  64KB × 64 = 4 MB
    {   512 * 1024,            16      },   // MEDIUM: 512KB × 16 = 8 MB
    {   4 * 1024 * 1024,       8       },   // LARGE:  4MB × 8 = 32 MB
    {   16 * 1024 * 1024,      4       },   // HUGE:   16MB × 4 = 64 MB
};
// 总预分配: 512KB + 4MB + 8MB + 32MB + 64MB ≈ 108 MB
```

HUGE 级别的 blockSize 设为 16MB 而非刚好 12MB（4K 帧），是为了覆盖各种极端情况。若同时有超过 4 个 4K 帧在 Pipeline 中流转，超出池容量的部分会 fallback 到普通 malloc。

### 7.4 MemoryPool 类

```cpp
class MemoryPool {
public:
    struct Tier {
        std::vector<void*> freeList;    // 空闲块列表
        std::mutex mutex;
        size_t blockSize;               // 固定块大小
        size_t allocatedCount = 0;      // 当前已分配出去的块数
        size_t poolCapacity;            // 池容量上限
    };

    Tier m_tiers[static_cast<int>(MemoryTier::COUNT)];

    // ===== 初始化：预分配所有块 =====
    void init() {
        for (int i = 0; i < static_cast<int>(MemoryTier::COUNT); i++) {
            Tier& t = m_tiers[i];
            t.blockSize = TIER_CONFIGS[i].blockSize;
            t.poolCapacity = TIER_CONFIGS[i].poolCapacity;

            for (size_t j = 0; j < t.poolCapacity; j++) {
                void* ptr = nullptr;
                posix_memalign(&ptr, 4096, t.blockSize);
                t.freeList.push_back(ptr);
            }
        }
    }

    // ===== 分配：返回 shared_ptr，自定义 deleter 自动归还 =====
    std::shared_ptr<MemoryBlock> alloc(size_t size) {
        MemoryTier tier = tierForSize(size);
        Tier& t = m_tiers[static_cast<int>(tier)];

        void* ptr = nullptr;
        size_t capacity = 0;

        {
            std::lock_guard<std::mutex> lock(t.mutex);
            if (!t.freeList.empty()) {
                ptr = t.freeList.back();
                t.freeList.pop_back();
                capacity = t.blockSize;
                t.allocatedCount++;
            } else if (t.allocatedCount < t.poolCapacity) {
                // 兜底：理论上 init 已全部预分配，但防御性地再分配一个
                posix_memalign(&ptr, 4096, t.blockSize);
                capacity = t.blockSize;
                t.allocatedCount++;
            }
        }

        if (ptr) {
            // 池分配：自定义 deleter 归还到池
            auto* raw = new MemoryBlock(ptr, size, capacity, tier, this);
            return std::shared_ptr<MemoryBlock>(raw, [this](MemoryBlock* block) {
                this->release(block);
            });
        }

        // 池耗尽 → fallback 到普通 malloc
        ptr = malloc(size);
        capacity = size;
        auto* raw = new MemoryBlock(ptr, size, capacity, MemoryTier::COUNT, nullptr);
        return std::shared_ptr<MemoryBlock>(raw, [](MemoryBlock* block) {
            // fallback 块：直接 free
            ::free(block->m_ptr);
            delete block;
        });
    }

    // ===== 归还：池内块回到 freeList，外部块调回调 =====
    void release(MemoryBlock* block) {
        if (block->m_releaseCallback) {
            // 外部内存（V4L2 mmap / DMA-BUF）：调回调，然后销毁对象
            block->m_releaseCallback();
            delete block;
            return;
        }

        if (block->m_pool) {
            // 池内块：归还到 freeList
            Tier& t = m_tiers[static_cast<int>(block->m_tier)];
            std::lock_guard<std::mutex> lock(t.mutex);
            t.freeList.push_back(block->m_ptr);
            t.allocatedCount--;
            delete block;
            return;
        }

        // fallback 块（tier == COUNT，pool == nullptr）
        ::free(block->m_ptr);
        delete block;
    }

    // ===== 析构：释放所有预分配的内存 =====
    ~MemoryPool() {
        for (int i = 0; i < static_cast<int>(MemoryTier::COUNT); i++) {
            Tier& t = m_tiers[i];
            for (void* ptr : t.freeList) {
                ::free(ptr);
            }
            t.freeList.clear();
        }
    }
};
```

### 7.5 MemoryBlock 类

```cpp
class MemoryBlock {
    friend class MemoryPool;  // 允许 Pool 直接访问 m_ptr 等私有成员

    void* m_ptr = nullptr;
    size_t m_size = 0;              // 用户请求的有效数据大小
    size_t m_capacity = 0;          // 实际分配的块大小（可能 > m_size）
    MemoryTier m_tier;
    MemoryPool* m_pool = nullptr;   // 所属池（nullptr 表示非池管理）
    std::function<void()> m_releaseCallback;  // 外部释放回调（零拷贝用）

    // 私有构造：只能由 MemoryPool 或 fromExternal 创建
    MemoryBlock(void* ptr, size_t size, size_t capacity,
                MemoryTier tier, MemoryPool* pool)
        : m_ptr(ptr), m_size(size), m_capacity(capacity),
          m_tier(tier), m_pool(pool) {}

public:
    // ===== 工厂：从内存池分配 =====
    // 不直接暴露构造函数，统一通过 MemoryPool::alloc() 创建

    // ===== 工厂：零拷贝构造（不拥有内存，由外部提供释放回调）=====
    static std::shared_ptr<MemoryBlock> fromExternal(
            void* ptr, size_t size, std::function<void()> releaseCallback) {
        auto* raw = new MemoryBlock(ptr, size, size, MemoryTier::COUNT, nullptr);
        raw->m_releaseCallback = std::move(releaseCallback);
        return std::shared_ptr<MemoryBlock>(raw, [](MemoryBlock* block) {
            if (block->m_releaseCallback) {
                block->m_releaseCallback();
            }
            delete block;
        });
    }

    void* data() { return m_ptr; }
    const void* data() const { return m_ptr; }
    size_t size() const { return m_size; }
    size_t capacity() const { return m_capacity; }
    MemoryTier tier() const { return m_tier; }

    // 在已分配的块上调整有效大小（不超过 capacity）
    // 安全前提：resize 只在 Buffer 尚未被其他线程可见时调用
    //（即 Buffer 被 push 到队列之前），因此无需加锁
    void resize(size_t newSize) {
        assert(newSize <= m_capacity);
        m_size = newSize;
    }

    // 析构函数为 default：所有释放逻辑由 MemoryPool::release() 处理，
    // 通过 shared_ptr 的自定义 deleter 调用，不在此处释放资源。
    ~MemoryBlock() = default;

    // 禁止拷贝/移动
    MemoryBlock(const MemoryBlock&) = delete;
    MemoryBlock& operator=(const MemoryBlock&) = delete;
};
```

**生命周期设计要点**：

- MemoryBlock 的 `new`/`delete` 仅管理对象本身的生命周期（调用构造/析构函数）
- 内存块的分配/释放（`m_ptr` 指向的内存）由 `MemoryPool::release()` 或 `fromExternal` 的回调处理
- `shared_ptr` 的自定义 deleter 调用 `MemoryPool::release()`，避免了析构函数中再调 `delete this` 的双重释放问题
- 三种释放路径统一在 `release()` 中：
  - 外部内存 → 调回调（如 V4L2 QBUF）
  - 池内块 → 归还到 freeList
  - fallback 块 → 直接 free

### 7.6 Buffer 类

```cpp
class Buffer {
public:
    // ===== 数据区 =====
    std::shared_ptr<MemoryBlock> data;
    size_t size = 0;                     // 有效数据大小（可能 < data->capacity()）

    // ===== 时间信息 =====
    int64_t pts = AV_NOPTS_VALUE;        // 显示时间戳
    int64_t dts = AV_NOPTS_VALUE;        // 解码时间戳
    int64_t duration = 0;                // 持续时长
    AVRational time_base = {0, 1};       // 时间基

    // ===== 关联流 =====
    int streamIndex = -1;                // 区分同一源输出的音/视频流

    // ===== 标志位 =====
    enum Flag : uint32_t {
        NONE        = 0,
        KEYFRAME    = 1 << 0,
        EOS         = 1 << 1,
        DISCONT     = 1 << 2,            // 不连续（seek 后）
        HEADER      = 1 << 3,            // codec header (SPS/PPS)
    };
    uint32_t flags = NONE;

    // ===== 便捷方法 =====
    bool isKeyFrame() const;
    bool isEOS() const;

    // ===== 工厂方法（统一走 MemoryPool）=====
    static std::shared_ptr<Buffer> fromAVPacket(const AVPacket* pkt,
                                                 AVRational tb,
                                                 MemoryPool* pool);
    static std::shared_ptr<Buffer> fromAVFrame(const AVFrame* frame,
                                                AVRational tb,
                                                MemoryPool* pool);
    static std::shared_ptr<Buffer> fromRawData(const void* data,
                                                size_t size,
                                                MemoryPool* pool);
};
```

### 7.7 Buffer 工厂方法实现

```cpp
std::shared_ptr<Buffer> Buffer::fromAVPacket(const AVPacket* pkt,
                                              AVRational tb,
                                              MemoryPool* pool) {
    auto buf = std::make_shared<Buffer>();

    auto block = pool->alloc(pkt->size);
    memcpy(block->data(), pkt->data, pkt->size);

    buf->data = block;
    buf->size = pkt->size;
    buf->pts = pkt->pts;
    buf->dts = pkt->dts;
    buf->duration = pkt->duration;
    buf->time_base = tb;
    buf->streamIndex = pkt->stream_index;
    buf->flags = (pkt->flags & AV_PKT_FLAG_KEY) ? Buffer::KEYFRAME : 0;

    return buf;
}

std::shared_ptr<Buffer> Buffer::fromAVFrame(const AVFrame* frame,
                                             AVRational tb,
                                             MemoryPool* pool) {
    auto buf = std::make_shared<Buffer>();

    size_t frameSize = calculateFrameSize(frame);  // 根据像素格式/采样格式计算
    auto block = pool->alloc(frameSize);

    if (isPlanarFormat(frame->format)) {
        // YUV420P: 3 个 plane，逐 plane 拷贝
        copyPlanarFrame(frame, block->data());
    } else {
        // YUYV / NV12 / 交错音频: 连续拷贝
        memcpy(block->data(), frame->data[0], frameSize);
    }

    buf->data = block;
    buf->size = frameSize;
    buf->pts = frame->pts;
    buf->time_base = tb;

    return buf;
}

std::shared_ptr<Buffer> Buffer::fromRawData(const void* data,
                                             size_t size,
                                             MemoryPool* pool) {
    auto buf = std::make_shared<Buffer>();

    auto block = pool->alloc(size);
    memcpy(block->data(), data, size);

    buf->data = block;
    buf->size = size;

    return buf;
}
```

### 7.8 V4L2 零拷贝

V4L2 采集场景比较特殊——设备通过 mmap 已经把帧数据映射到用户空间了，没必要再拷贝一次。使用 `MemoryBlock::fromExternal` 直接引用 mmap 地址，在 Buffer 释放时通过回调归还 buffer 给 V4L2：

```cpp
std::shared_ptr<Buffer> V4L2CaptureNode::generateData() {
    struct v4l2_buffer v4l2Buf = {};
    v4l2Buf.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    ioctl(m_fd, VIDIOC_DQBUF, &v4l2Buf);

    void* mmapAddr = m_buffers[v4l2Buf.index].start;
    size_t mmapLen = v4l2Buf.bytesused;

    // 直接用 mmap 地址构造 MemoryBlock，不拷贝
    auto block = MemoryBlock::fromExternal(
        mmapAddr, mmapLen,
        [this, idx = v4l2Buf.index]() {
            // 释放回调：当 Buffer 引用计数归零时调用
            struct v4l2_buffer retBuf = {};
            retBuf.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
            retBuf.index = idx;
            ioctl(m_fd, VIDIOC_QBUF, &retBuf);
        }
    );

    auto buf = std::make_shared<Buffer>();
    buf->data = block;
    buf->size = mmapLen;
    buf->pts = v4l2Buf.timestamp.tv_sec * 1000000LL
             + v4l2Buf.timestamp.tv_usec;
    buf->streamIndex = 0;

    return buf;
    // 不能立即 QBUF，必须等下游消费完（Buffer 引用计数归零后自动 QBUF）
}
```

将来支持 FFmpeg 硬件解码（DMA-BUF fd）时，同样走 `fromExternal` 路径。

### 7.9 MemoryBlock 生命周期总结

```text
                    ┌─────────────────────────────────────────┐
                    │              MemoryPool                 │
                    │                                         │
  alloc(size) ────→│  TINY:  [■][■][■]... freeList (128块)   │
                    │  SMALL: [■][■][■]... freeList (64块)    │
  池中有空闲 ─────→│  MEDIUM:[■][■]...    freeList (16块)    │ ──→ shared_ptr<MemoryBlock>
                    │  LARGE: [■][■]...    freeList (8块)     │       (自定义 deleter)
                    │  HUGE:  [■][■]...    freeList (4块)     │
                    │                                         │
                    │  池耗尽 → fallback malloc               │
                    │    → tier = COUNT，析构时直接 free()     │
                    └─────────────────────────────────────────┘
                               ↑                    │
                               │                    │
                    ┌──────────┴────────────────────┴──────────┐
                    │              MemoryBlock                  │
                    │  ptr, size, capacity, tier, pool           │
                    │  shared_ptr 管理生命周期                    │
                    │  所有引用释放 → 自定义 deleter               │
                    │    → MemoryPool::release(block)            │
                    │       → pool 块: 归还到 freeList           │
                    │       → 外部块: 调 releaseCallback         │
                    │       → fallback: ::free(ptr)              │
                    │    → delete block (销毁对象本身)            │
                    └──────────────────────────────────────────┘

  特殊路径 1: fromExternal (V4L2 mmap / DMA-BUF)
    → tier = COUNT, pool = nullptr
    → deleter → m_releaseCallback() → V4L2 QBUF / 关闭 fd → delete block

  特殊路径 2: pool 耗尽 fallback
    → tier = COUNT, pool = nullptr
    → deleter → ::free(ptr) → delete block
```

### 7.10 Pipeline 持有 MemoryPool

MemoryPool 由 Pipeline 持有（非全局单例），不同 Pipeline 实例有独立的内存管理：

```cpp
class Pipeline {
    std::unique_ptr<MemoryPool> m_memoryPool;
    // ...
public:
    MemoryPool* memoryPool() { return m_memoryPool.get(); }
};
```

Node 通过 `m_pipeline->memoryPool()` 访问池：

```cpp
// 在节点内部使用
auto buf = Buffer::fromAVPacket(pkt, timeBase, m_pipeline->memoryPool());
```

### 7.11 池容量可配置

用户可通过 Pipeline 参数调整各级别池容量，适应不同场景：

```cpp
// 嵌入式 / 内存受限场景：减小池
pipeline->setParam("pool_tiny_count", 32);
pipeline->setParam("pool_large_count", 2);

// 4K 场景 / 内存充足：增大池
pipeline->setParam("pool_huge_count", 8);
```

Pipeline 构造时根据参数初始化 MemoryPool：

```cpp
Pipeline::Pipeline(const std::string& name) {
    MemoryPoolConfig config;
    config.tierConfigs[0] = {4*1024,       getParam<int>("pool_tiny_count", 128)};
    config.tierConfigs[1] = {64*1024,      getParam<int>("pool_small_count", 64)};
    config.tierConfigs[2] = {512*1024,     getParam<int>("pool_medium_count", 16)};
    config.tierConfigs[3] = {4*1024*1024,  getParam<int>("pool_large_count", 8)};
    config.tierConfigs[4] = {16*1024*1024, getParam<int>("pool_huge_count", 4)};

    m_memoryPool = std::make_unique<MemoryPool>(config);
    m_memoryPool->init();
}
```

## 8. StreamInfo 与参数传递

### 8.1 StreamInfo 结构

```cpp
struct StreamInfo {
    MediaType type = MediaType::UNKNOWN;  // VIDEO / AUDIO

    // ===== 视频 =====
    int width = 0;
    int height = 0;
    AVRational frameRate = {0, 1};
    AVPixelFormat pixelFmt = AV_PIX_FMT_NONE;

    // ===== 音频 =====
    int sampleRate = 0;
    int channels = 0;
    AVSampleFormat sampleFmt = AV_SAMPLE_FMT_NONE;

    // ===== 编码 =====
    AVCodecID codecId = AV_CODEC_ID_NONE;
    const AVCodecParameters* codecpar = nullptr;

    // ===== 时间 =====
    AVRational time_base = {0, 1};
    int64_t duration = 0;  // 流总时长（微秒），未知为 0
};
```

### 8.2 两阶段传递机制

**阶段一：连接时传递（Play Phase 3 — resolveLinks）**

当 Pad 建立物理连接时，SrcPad 将 StreamInfo 复制给 SinkPad，触发目标节点的 `onLink()` 回调。

```text
DemuxNode::SrcPad("video_0").streamInfo ──link()──→ DecodeNode::SinkPad("in")
    StreamInfo: {VIDEO, H264, 1920, 1080, codecpar, time_base}
    ↓
    DecodeNode::onLink() 保存 codecpar，ready 时用于初始化 AVCodecContext
```

**阶段二：Ready 阶段补充输出信息**

部分节点（如 DecodeNode、EncodeNode）在 probe 时不知道输出格式，需在 ready 打开 codec 后补充：

```cpp
void DecodeNode::onReady() {
    m_codecCtx = avcodec_alloc_context3(codec);
    avcodec_parameters_to_context(m_codecCtx, m_codecpar);
    avcodec_open2(m_codecCtx, codec, nullptr);

    // 更新输出 StreamInfo
    StreamInfo outInfo;
    outInfo.type = MediaType::VIDEO;
    outInfo.width = m_codecCtx->width;
    outInfo.height = m_codecCtx->height;
    outInfo.pixelFmt = m_codecCtx->pix_fmt;
    outInfo.time_base = m_codecCtx->time_base;
    m_srcPads[0]->setStreamInfo(outInfo);

    // 通知下游
    m_srcPads[0]->pushEvent(Event::makeStreamInfoChanged(outInfo));
}
```

下游节点（如 VideoRenderNode）在 ready 阶段或收到 STREAM_INFO_CHANGED Event 后，根据 StreamInfo 初始化资源（创建 SDL Texture 等）。

### 8.3 多流处理

DemuxNode 可能输出多个流（音频 + 视频），为每个流创建独立的 SrcPad，每个 SrcPad 携带各自的 StreamInfo：

```cpp
auto videoPad = demux->getSrcPad("video_0");  // StreamInfo{VIDEO, H264, ...}
auto audioPad = demux->getSrcPad("audio_0");  // StreamInfo{AUDIO, AAC, ...}
```

## 9. 事件系统

### 9.1 Event 类

```cpp
class Event {
public:
    enum class Type {
        EOS,                    // 流结束
        FLUSH_START,            // 开始 flush
        FLUSH_DONE,             // flush 完成
        STREAM_INFO_CHANGED,    // 流参数变化
        SEEK,                   // seek 请求
        CUSTOM,                 // 用户自定义
    };

    Type type;
    int streamIndex = -1;       // 针对哪个流（-1 表示全局）

    // 附加数据（union 或 variant）
    struct SeekData { int64_t position; };
    struct StreamChangedData { StreamInfo info; };

    // 工厂方法
    static std::shared_ptr<Event> makeEOS(int streamIdx = -1);
    static std::shared_ptr<Event> makeFlushStart();
    static std::shared_ptr<Event> makeFlushDone();
    static std::shared_ptr<Event> makeStreamInfoChanged(const StreamInfo& info);
};
```

### 9.2 Event 传递

Event 和 Buffer 共享同一个 BoundedQueue（队列存储 `std::variant<Buffer, Event>`），保证严格顺序性。

```text
SrcPad::push(Buffer)  → queue.push(variant<Buffer>)
SrcPad::pushEvent(Event) → queue.push(variant<Event>)

SinkPad::pop() → PopResult {
    hasBuffer() → 从 variant 中取出 Buffer
    hasEvent()  → 从 variant 中取出 Event
}
```

## 10. 背压机制

### 10.1 原理

背压通过 Pad 内置的有界队列自然传导。当下游处理慢于上游时：

```text
VideoRenderNode 每 16.6ms 消费一帧
  ↓
VideoRenderNode::SinkPad 队列逐渐填满 (max 5)
  ↓
DecodeNode::SrcPad::push() 阻塞 (BLOCK 策略)
  ↓
DecodeNode 线程暂停
  ↓
DecodeNode::SinkPad 队列填满 (max 5)
  ↓
DemuxNode::SrcPad::push() 阻塞
  ↓
整条管线暂停，直到 VideoRenderNode 消费一帧
  ↓
队列有空位 → 解除阻塞 → 继续处理
```

### 10.2 溢出策略

QueueNode 和 Pad 队列支持三种溢出策略：

| 策略            | 行为                 | 适用场景                 |
| --------------- | -------------------- | ------------------------ |
| `BLOCK`（默认） | 阻塞上游的 push 调用 | 保证不丢帧，适合文件播放 |
| `DROP_OLDEST`   | 丢弃队列中最老的帧   | 实时场景，保证低延迟     |
| `DROP_NEWEST`   | 丢弃刚到来的帧       | 特殊需求                 |

用户通过 QueueNode 或直接配置 Pad 设置：

```cpp
queue->setOverflowPolicy(QueueNode::Policy::DROP_OLDEST);
queue->setParam("max_size", 500);
```

### 10.3 音视频链路独立性

音频和视频走各自独立的链路，背压独立传导：

```text
Demux ──video_0──→ Decode(V) ──→ VideoRender   ← 背压独立
      ──audio_0──→ Decode(A) ──→ AudioPlay      ← 背压独立
```

DemuxNode 内部对每个 SrcPad 独立 push，一个流的队列满不会阻塞另一个流的处理：

```cpp
void DemuxNode::workerLoop() {
    while (m_state == PLAYING) {
        auto pkt = readPacket();
        auto srcPad = m_srcPads[pkt->stream_index];
        srcPad->push(makeBuffer(pkt), std::chrono::milliseconds(10));
        // 带超时的 push，避免一个流阻塞导致另一个流也停
    }
}
```

## 11. A/V 同步

### 11.1 核心原理

```text
AudioPlayNode = Master Clock（音频播放进度即为全局时钟）
VideoRenderNode = Slave（以音频时钟为基准呈现视频帧）
```

SDL3 音频设备以固定速率（由声卡硬件决定，如 48000 samples/sec）消费 PCM 数据，音频播放位置就是天然的时间基准。

### 11.2 全局时钟

```cpp
class Clock {
    bool m_hasAudioMaster = false;
    int64_t m_audioPositionUs = 0;        // 音频播放位置（微秒）
    std::chrono::steady_clock::time_point m_startWallTime;  // 墙钟起始时间

public:
    // AudioPlayNode 调用：推进时钟
    void advance(int64_t durationUs);

    // VideoRenderNode 调用：获取当前时间位置
    int64_t getPositionUs();

    // 设置是否有音频 Master
    void setAudioMaster(bool has);
};
```

**有音频时**：`getPositionUs()` 返回 `m_audioPositionUs`（由 AudioPlayNode 推进）

**无音频时**：`getPositionUs()` 返回 `steady_clock::now() - m_startWallTime`（墙钟）

### 11.3 VideoRenderNode 同步逻辑

```cpp
void VideoRenderNode::consume(std::shared_ptr<Buffer> buffer) {
    // 1. 将 buffer PTS 转换为微秒
    int64_t frameTimeUs = av_rescale_q(
        buffer->pts, buffer->time_base, {1, 1000000});

    // 2. 获取当前时钟位置
    int64_t clockUs = m_pipeline->getClock()->getPositionUs();

    // 3. 计算差值
    int64_t diffUs = frameTimeUs - clockUs;

    if (diffUs > 2000) {
        // 视频超前音频 > 2ms → 等待
        std::this_thread::sleep_for(std::chrono::microseconds(diffUs));
    } else if (diffUs < -50000) {
        // 视频落后音频 > 50ms → 丢帧
        return;
    }
    // 差值在 [-50ms, +2ms] → 立即渲染

    // 4. 渲染
    renderFrame(buffer);  // SDL_UpdateYUVTexture + SDL_RenderPresent
}
```

### 11.4 AudioPlayNode 时钟推进

```cpp
void AudioPlayNode::consume(std::shared_ptr<Buffer> buffer) {
    // 如需重采样
    // swr_convert(...)

    // 写入 SDL 音频流
    SDL_PutAudioStreamData(m_audioStream, buffer->data->data(), buffer->size);

    // 根据写入的样本数推进时钟
    int64_t durationUs = (int64_t)sampleCount * 1000000 / m_sampleRate;
    m_pipeline->getClock()->advance(durationUs);
}
```

## 12. 消息总线

### 12.1 Message 类

```cpp
class Message {
public:
    enum class Type {
        ERROR,              // 错误
        WARNING,            // 警告
        STATE_CHANGED,      // 节点状态变化
        EOS,                // 某个 Sink 收到 EOS
        STREAM_INFO,        // 流信息通知
        BUFFERING,          // 缓冲状态（网络流用）
        CUSTOM,             // 用户自定义
    };

    Type type;
    INode* source = nullptr;        // 来源节点
    std::string text;               // 描述文本
    int code = 0;                   // 错误码
    int streamIndex = -1;
};
```

### 12.2 MessageBus 类

```cpp
class MessageBus {
    std::queue<Message> m_queue;
    std::mutex m_mutex;
    std::function<void(const Message&)> m_callback;

public:
    // 节点调用：投递消息
    void post(Message msg);

    // 用户设置回调（推荐方式）
    void setCallback(std::function<void(const Message&)> cb);

    // 用户轮询（可选方式）
    std::optional<Message> poll(std::chrono::milliseconds timeout);
};
```

### 12.3 用户侧使用

```cpp
// 方式一：回调
pipeline->setMessageCallback([](const Message& msg) {
    if (msg.type == Message::Type::ERROR)
        fprintf(stderr, "Error from %s: %s\n",
                msg.source->name().c_str(), msg.text.c_str());
});

// 方式二：轮询
while (auto msg = pipeline->pollMessage(std::chrono::milliseconds(100))) {
    // 处理消息
}
```

### 12.4 节点内部使用

```cpp
void DecodeNode::process(std::shared_ptr<Buffer> buf) {
    int ret = avcodec_send_packet(m_codecCtx, ...);
    if (ret < 0) {
        m_bus->post({Message::Type::ERROR, this,
                     "avcodec_send_packet failed", ret});
        setState(NodeState::ERROR);
    }
}
```

## 13. EOS 与退出机制

### 13.1 路径 A：自然 EOS（文件播放）

```text
DemuxNode 检测到 AVERROR_EOF
  ↓
对每个 SrcPad 发送 EOS Event
  ↓
DecodeNode 收到 EOS
  → flush decoder: avcodec_send_packet(NULL)
  → 循环 avcodec_receive_frame 取出 B 帧缓冲中的剩余帧
  → 推送剩余帧到下游
  → 发送 EOS Event 到 SrcPad
  ↓
VideoRenderNode / AudioPlayNode 收到 EOS
  → 渲染/播放完剩余数据
  → 调用 Pipeline::reportSinkEOS(this)
  ↓
Pipeline 检查：m_eosCount == m_sinkCount ?
  → 所有 Sink 都 EOS → 触发用户 EOS 回调
```

### 13.2 路径 B：用户主动停止

```cpp
pipeline->stop();
```

```text
Pipeline::stop() 内部：
1. 向所有 SourceNode 设置 m_stopRequested = true
2. SourceNode 线程检测到标志 → 退出循环
3. SourceNode 向 SrcPad 发送 FLUSH_START Event
4. FLUSH_START 沿链路传播，每个节点清空队列和内部缓冲
5. 每个节点线程退出
6. Pipeline 逐个调用各节点的 null() 释放资源
```

### 13.3 路径 C：错误导致退出

```text
节点 process() 出错
  ↓
post(Message::ERROR, ...) 到 MessageBus
  ↓
setState(NodeState::ERROR)
  ↓
Pipeline 错误处理策略:
  ├── ErrorAction::STOP   → 整条 Pipeline stop
  └── ErrorAction::IGNORE → 跳过错误继续（慎用）
```

### 13.4 EOS 回调设计

Pipeline 不自动 stop，由用户在 EOS 回调中决定后续行为：

```cpp
pipeline->setEosCallback([&]() {
    // 播放完成，用户决定退出
    pipeline->stop();
    mainLoop.quit();
});
```

### 13.5 用户命令机制

#### 13.5.1 设计目标

框架需要一个从用户侧到 Pipeline 再到各节点的命令通道。当前只需支持 stop，但架构必须便于未来扩展（pause、seek、音量控制、自定义命令等）。

采用 Command Pattern + typeId 分发：命令定义为独立类，通过虚函数返回类型标识，分发侧使用 switch 选择处理逻辑。不依赖 RTTI。

#### 13.5.2 命令类型 ID 与 Command 基类

```cpp
// ===== 命令类型 ID =====
namespace CmdId {
    constexpr uint32_t STOP = 0;
    // 未来内置命令在此扩展
    // constexpr uint32_t PAUSE = 1;
    // constexpr uint32_t SEEK  = 2;

    // 用户自定义命令从 1000 开始
    constexpr uint32_t USER_BASE = 1000;
}

// ===== 命令基类 =====
class Command {
public:
    virtual ~Command() = default;
    virtual uint32_t typeId() const = 0;
};

// ===== 内置命令：停止 =====
class StopCommand : public Command {
public:
    uint32_t typeId() const override { return CmdId::STOP; }
};
```

使用 `typeId()` 虚函数返回类型标识，分发侧用 switch 选择处理逻辑。不使用 `dynamic_cast`，不依赖 RTTI。switch 编译后生成跳转表，性能优于 `dynamic_cast` 的继承链回溯。

ID 分配规范：

| 范围 | 用途 |
|------|------|
| 0 ~ 999 | 框架内置命令保留 |
| 1000+ | 用户自定义命令 |

#### 13.5.3 Pipeline 命令接收与分发

```cpp
class Pipeline {
public:
    // 通用命令入口
    void sendCommand(std::shared_ptr<Command> cmd);

    // 内置命令的便捷方法
    void stop() { sendCommand(std::make_shared<StopCommand>()); }

    // 等待停止
    void waitForStop();

private:
    // Pipeline 自身对命令的响应
    void handleCommand(Command* cmd);
};
```

`sendCommand` 执行顺序：先分发给所有节点（让节点先响应），再由 Pipeline 自身处理后续逻辑（如等待线程退出、释放资源）。

```cpp
void Pipeline::sendCommand(std::shared_ptr<Command> cmd) {
    // 1. 分发给所有节点
    for (auto& node : m_nodes) {
        node->handleCommand(cmd.get());
    }
    // 2. Pipeline 自身处理
    handleCommand(cmd.get());
}
```

#### 13.5.4 INode 命令响应

```cpp
class INode {
public:
    // 默认实现：处理所有节点通用的命令
    virtual void handleCommand(Command* cmd) {
        switch (cmd->typeId()) {
        case CmdId::STOP:
            m_stopRequested = true;
            m_stateCV.notify_all();  // 唤醒可能正在等待的线程
            break;
        default:
            break;  // 未知命令，忽略
        }
    }
};
```

stop 是所有节点的通用行为（设置退出标志），因此放在基类默认实现中。子类如果需要额外响应某个命令，重写 `handleCommand`，先调用基类再添加自己的逻辑：

```cpp
void DemuxNode::handleCommand(Command* cmd) {
    INode::handleCommand(cmd);  // 通用处理（stop 等）

    switch (cmd->typeId()) {
    // 未来：处理 DemuxNode 特有的命令
    // case CmdId::SEEK: {
    //     auto* seek = static_cast<SeekCommand*>(cmd);
    //     doSeek(seek->positionUs);
    //     break;
    // }
    default:
        break;
    }
}
```

#### 13.5.5 核心原则

`stop()` 永远由**用户线程**（主线程或用户开的子线程）调用，**永远不在节点工作线程中调用**。

节点线程只负责：
- 报告状态（通过 `reportSinkEOS` / `reportNodeError`）
- 退出自己的工作循环

清理逻辑（等线程退出、释放资源）全部在 `stop()` 内部执行，由调用 `stop()` 的用户线程负责。

#### 13.5.6 stop 执行流程

```text
用户线程调用 pipeline->stop()
  ↓
sendCommand(StopCommand)
  ├── 广播给所有节点:
  │     每个节点 m_stopRequested = true
  │     唤醒可能在 pop 中等待的线程
  ↓
Pipeline::handleCommand(StopCommand)
  ├── 等待所有节点线程退出 (waitThreadExit)
  ├── 按逆拓扑顺序释放资源 (null)
  ├── m_state = STOPPED
  └── 唤醒 waitForStop()
```

#### 13.5.7 waitForStop

`waitForStop()` 同时监听三种退出条件：已被 stop、所有 Sink EOS、有节点报错。如果是 EOS 或错误唤醒的，由当前线程（用户线程）调 `stop()` 执行清理。

```cpp
void Pipeline::waitForStop() {
    std::unique_lock lock(m_waitMutex);
    m_waitCV.wait(lock, [this] {
        return m_state == PipelineState::STOPPED   // 已被 stop()
            || m_eosCount == m_sinkCount           // 所有 Sink 都 EOS
            || m_errorOccurred;                    // 有节点报错
    });

    // 如果是 EOS 或错误唤醒的，由当前线程执行 stop()
    if (m_state != PipelineState::STOPPED) {
        lock.unlock();
        stop();
    }
}
```

轮询场景下用户自行检查状态：

```cpp
while (running) {
    // ... 用户自己的事件处理 ...
    if (pipeline->isAllEosReached()) running = false;
    if (pipeline->isErrorOccurred())  running = false;
    std::this_thread::sleep_for(16ms);
}
pipeline->stop();  // 用户线程调，安全
```

#### 13.5.8 EOS 与错误退出

**reportSinkEOS**：节点线程调用，只做计数 + 通知，**不调 stop()**。

```cpp
void Pipeline::reportSinkEOS(INode* sink) {
    std::lock_guard lock(m_waitMutex);
    m_eosCount++;
    if (m_eosCount == m_sinkCount) {
        if (m_eosCallback) m_eosCallback();
    }
    m_waitCV.notify_all();  // 唤醒 waitForStop()
}
```

**reportNodeError**：节点线程调用，只报错 + 通知，**不调 stop()**。

```cpp
void Pipeline::reportNodeError(INode* node, int code, const std::string& msg) {
    m_bus->post({Message::Type::ERROR, node, msg, code});
    {
        std::lock_guard lock(m_waitMutex);
        m_errorOccurred = true;
    }
    m_waitCV.notify_all();  // 唤醒 waitForStop()
}
```

节点侧错误处理：报错后退出自己的循环，不管下游。

```cpp
void DecodeNode::workerLoop() {
    while (!m_stopRequested) {
        auto result = m_sinkPads[0]->pop(100ms);
        if (result.hasBuffer()) {
            auto output = process(result.buffer());
            if (!output && m_hasError) {
                m_pipeline->reportNodeError(this, m_errorCode, m_errorMsg);
                break;  // 只退出自己
            }
            if (output) m_srcPads[0]->push(output);
        }
    }
}
```

#### 13.5.9 用户触发 stop 的方式

框架只提供 `stop()` / `sendCommand()` / `waitForStop()` API，不内置输入捕获。`stop()` 必须在用户线程中调用。

```cpp
// 场景 1：播放文件，EOS 自动退出
pipeline->play();
pipeline->waitForStop();   // EOS → 唤醒 → 自动 stop → 返回

// 场景 2：Ctrl+C 退出
signal(SIGINT, [](int) { pipeline->stop(); });
pipeline->play();
pipeline->waitForStop();   // EOS 或 Ctrl+C → 返回

// 场景 3：SDL 窗口 + 按键退出
pipeline->play();
bool running = true;
while (running) {
    SDL_Event e;
    while (SDL_PollEvent(&e)) {
        if (e.type == SDL_EVENT_QUIT) running = false;
    }
    if (pipeline->isAllEosReached()) running = false;
    std::this_thread::sleep_for(16ms);
}
pipeline->stop();

// 场景 4：推流 60 秒自动停
pipeline->play();
std::this_thread::sleep_for(60s);
pipeline->stop();
```

#### 13.5.10 扩展性说明

新增命令只需三步，无需修改已有命令的处理逻辑：

1. 定义新的命令类型 ID 和命令类（继承 `Command`，实现 `typeId()`）
2. Pipeline 的 `handleCommand` 添加 case 分支
3. 相关节点重写 `handleCommand` 添加 case 分支

**内置扩展示例（pause）**：

```cpp
// 1. 新命令
namespace CmdId {
    constexpr uint32_t PAUSE = 1;
}
class PauseCommand : public Command {
public:
    uint32_t typeId() const override { return CmdId::PAUSE; }
};

// 2. Pipeline 侧
void Pipeline::pause() {
    sendCommand(std::make_shared<PauseCommand>());
}
void Pipeline::handleCommand(Command* cmd) {
    switch (cmd->typeId()) {
    case CmdId::STOP:  { /* 已有逻辑 */ break; }
    case CmdId::PAUSE: { m_state = PipelineState::PAUSED; break; }
    default: break;
    }
}

// 3. 节点侧
void INode::handleCommand(Command* cmd) {
    switch (cmd->typeId()) {
    case CmdId::STOP:  { /* 已有逻辑 */ break; }
    case CmdId::PAUSE: { m_pauseRequested = true; break; }
    default: break;
    }
}
```

**用户自定义命令示例**：

```cpp
// 用户定义命令 ID（从 USER_BASE 开始）
namespace MyCmd {
    constexpr uint32_t RESTART_DEVICE = CmdId::USER_BASE + 0;
}

class RestartDeviceCommand : public Command {
public:
    uint32_t typeId() const override { return MyCmd::RESTART_DEVICE; }
    std::string deviceName;  // 命令参数
};

// 用户在自己的节点中响应
void V4L2CaptureNode::handleCommand(Command* cmd) {
    INode::handleCommand(cmd);  // 先处理通用命令（stop 等）

    switch (cmd->typeId()) {
    case MyCmd::RESTART_DEVICE: {
        auto* restart = static_cast<RestartDeviceCommand*>(cmd);
        reopenDevice(restart->deviceName);
        break;
    }
    default:
        break;
    }
}
```

## 14. Pipeline 构建与执行流程

### 14.1 用户侧 API 流程

```cpp
// 1. 创建 Pipeline
auto pipeline = Pipeline::create("my-player");

// 2. 添加节点 + 设置参数
auto demux = pipeline->addNode<DemuxNode>("demux");
demux->setParam("url", "/home/user/video.mp4");
auto decV = pipeline->addNode<DecodeNode>("dec-v");
// ...

// 3. 连接节点
demux->link(decV, "video_0");
decV->link(render);

// 4. 设置回调
pipeline->setEosCallback([]() { ... });
pipeline->setMessageCallback([](const Message& msg) { ... });

// 5. 启动
pipeline->play();  // 内部执行完整的五阶段流程

// 6. 阻塞等待
pipeline->waitForEos();

// 7. 停止销毁
pipeline->stop();
pipeline.reset();
```

### 14.2 Link 机制：延迟绑定

用户调用 `node->link(downstream, srcPadName, sinkPadName)` 时，不立即建立 Pad 物理连接，只在 Pipeline 内部记录一条**连接意图**：

```cpp
struct PendingLink {
    INode* srcNode;
    std::string srcPadName;      // 可以为空（使用默认 SrcPad）
    INode* sinkNode;
    std::string sinkPadName;     // 可以为空（使用默认 SinkPad）
};
```

真正连接在 `play()` 的 Phase 3 执行。

### 14.3 play() 五阶段流程

```text
play()
 │
 ├── Phase 1: 拓扑排序
 │     根据 pendingLinks 构建有向图，做拓扑排序
 │     输出: nodeOrder (Source → Transform → Sink)
 │
 ├── Phase 2: Probe（按拓扑顺序 Source → Sink）
 │     调用每个节点的 onProbe()
 │     ├── DemuxNode: avformat_open_input + find_stream_info, 创建 SrcPad + StreamInfo
 │     ├── V4L2CaptureNode: open device + query caps, 创建 SrcPad + StreamInfo
 │     ├── AudioCaptureNode: snd_pcm_open + query, 创建 SrcPad + StreamInfo
 │     ├── DecodeNode: 创建 SinkPad "in" + SrcPad "out"（StreamInfo 暂空）
 │     ├── EncodeNode: 创建 SinkPad "in" + SrcPad "out"（StreamInfo 暂空）
 │     ├── MuxNode: 不创建 Pad（等 requestSinkPad）
 │     ├── VideoRenderNode: 创建 SinkPad "in"
 │     ├── AudioPlayNode: 创建 SinkPad "in"
 │     ├── FileSinkNode: 创建 SinkPad "in"
 │     └── QueueNode: 创建 SinkPad "in" + SrcPad "out"
 │
 ├── Phase 3: 解析连接
 │     遍历 pendingLinks，对每条:
 │     ├── 获取/创建 Pad（静态直接取，MuxNode 通过 requestSinkPad 动态创建）
 │     ├── 建立 Pad 物理连接（peer 指针）
 │     ├── 传递 StreamInfo（如果 srcPad 已有）
 │     └── 触发 sinkNode->onLink(sinkPad, streamInfo)
 │
 ├── Phase 4: 验证拓扑
 │     ├── 每个 SinkNode 的 SinkPad 必须已连接
 │     ├── 每个 TransformNode 的 SinkPad 和 SrcPad 必须已连接（MuxNode 至少一个 SinkPad）
 │     ├── 无环路（DFS 检测）
 │     └── 失败 → 抛异常
 │
 ├── Phase 5: Ready（按拓扑顺序 Source → Sink）
 │     调用每个节点的 onReady()
 │     ├── V4L2CaptureNode: S_FMT + REQBUFS + MMAP + STREAMON
 │     ├── AudioCaptureNode: hw_params + prepare
 │     ├── DemuxNode: 资源已在 probe 分配，无额外操作
 │     ├── DecodeNode: avcodec_open2, 更新 SrcPad StreamInfo, push STREAM_INFO_CHANGED
 │     ├── EncodeNode: avcodec_open2, 初始化 sws_scale/swr_convert, 更新 StreamInfo
 │     ├── MuxNode: avformat_new_stream × N + AVIOContext + write_header
 │     ├── QueueNode: 无操作
 │     ├── VideoRenderNode: SDL_CreateWindow + Texture（如有 StreamInfo）
 │     ├── AudioPlayNode: SDL_OpenAudioDevice
 │     └── FileSinkNode: fopen
 │
 ├── Phase 6: 创建线程（按逆拓扑顺序 Sink → Source）
 │     调用每个节点的 createThread()
 │     └── 工作线程创建后立即 wait (state == PAUSED)
 │
 └── Phase 7: 启动（按逆拓扑顺序 Sink → Source）
       调用每个节点的 setState(PLAYING)
       └── notify 线程开始运行
```

## 15. 用户侧 API 与示例

### 15.1 播放本地文件

```cpp
auto pipeline = Pipeline::create("player");

auto demux  = pipeline->addNode<DemuxNode>("demux");
demux->setParam("url", "/home/user/video.mp4");

auto decV   = pipeline->addNode<DecodeNode>("dec-v");
auto decA   = pipeline->addNode<DecodeNode>("dec-a");
auto render = pipeline->addNode<VideoRenderNode>("render");
auto audio  = pipeline->addNode<AudioPlayNode>("audio");

demux->link(decV, "video_0");
demux->link(decA, "audio_0");
decV->link(render);
decA->link(audio);

pipeline->setEosCallback([&]() { mainLoop.quit(); });
pipeline->setMessageCallback([](const Message& msg) {
    if (msg.type == Message::Type::ERROR)
        fprintf(stderr, "Error: %s\n", msg.text.c_str());
});

pipeline->play();
mainLoop.run();           // 阻塞
pipeline->stop();
```

### 15.2 采集预览

```cpp
auto pipeline = Pipeline::create("preview");

auto cap = pipeline->addNode<V4L2CaptureNode>("cam");
cap->setParam("device", "/dev/video0");
cap->setParam("width", 1280);
cap->setParam("height", 720);

auto render = pipeline->addNode<VideoRenderNode>("render");
render->setParam("title", "Preview");

cap->link(render);

pipeline->setEosCallback([&]() { mainLoop.quit(); });
pipeline->play();
mainLoop.run();
pipeline->stop();
```

### 15.3 采集编码录制

```cpp
auto pipeline = Pipeline::create("recorder");

auto vCap = pipeline->addNode<V4L2CaptureNode>("vcam");
vCap->setParam("device", "/dev/video0");
vCap->setParam("width", 1920);
vCap->setParam("height", 1080);

auto aCap = pipeline->addNode<AudioCaptureNode>("amic");
aCap->setParam("device", "default");
aCap->setParam("sample_rate", 48000);

auto vEnc = pipeline->addNode<EncodeNode>("venc");
vEnc->setParam("codec", "libx264");
vEnc->setParam("bitrate", 4000000);

auto aEnc = pipeline->addNode<EncodeNode>("aenc");
aEnc->setParam("codec", "aac");
aEnc->setParam("bitrate", 128000);

auto mux  = pipeline->addNode<MuxNode>("mux");
mux->setParam("format", "mp4");

auto sink = pipeline->addNode<FileSinkNode>("sink");
sink->setParam("path", "/home/user/output.mp4");

vCap->link(vEnc)->link(mux, "", "video_in");
aCap->link(aEnc)->link(mux, "", "audio_in");
mux->link(sink);

pipeline->setEosCallback([&]() { mainLoop.quit(); });
pipeline->play();
mainLoop.run();           // 用户按 Ctrl+C 触发 stop
pipeline->stop();
```

### 15.4 文件推流（不转码）

```cpp
auto pipeline = Pipeline::create("relay");

auto demux = pipeline->addNode<DemuxNode>("demux");
demux->setParam("url", "/home/user/video.mp4");

auto mux   = pipeline->addNode<MuxNode>("mux");
mux->setParam("format", "flv");

auto push  = pipeline->addNode<RTSPPushNode>("push");
push->setParam("url", "rtsp://server/live/stream");

demux->link(mux, "video_0", "video_in");
demux->link(mux, "audio_0", "audio_in");
mux->link(push);

pipeline->play();
mainLoop.run();
pipeline->stop();
```

### 15.5 Link API 规范

```cpp
// 签名
INode* INode::link(INode* downstream,
                   const std::string& srcPadName = "",
                   const std::string& sinkPadName = "");
// 返回 downstream，支持链式调用
```

| 场景           | 写法                                           |
| -------------- | ---------------------------------------------- |
| 双方单 Pad     | `a->link(b)`                                   |
| 源多输出 Pad   | `demux->link(decV, "video_0")`                 |
| 目标多输入 Pad | `vEnc->link(mux, "", "video_in")`              |
| 双方都需指定   | `demux->link(mux, "video_0", "video_in")`      |
| 链式调用       | `cap->link(encode)->link(mux, "", "video_in")` |

## 16. 各节点详细设计

### 16.1 DemuxNode

```cpp
class DemuxNode : public TransformNode {
public:
    // 参数:
    //   "url" : std::string — 文件路径或网络 URL

    // 输出 Pad:
    //   "video_0", "audio_0", ... (根据文件流动态创建)

    void onProbe() override;
    // avformat_open_input + avformat_find_stream_info
    // 遍历 streams → 创建 SrcPad + 填充 StreamInfo

    void onReady() override;
    // 检查 m_mode (FROM_URL / FROM_UPSTREAM)

    // workerLoop:
    //   FROM_URL: 循环 av_read_frame → Buffer::fromAVPacket → push
    //   FROM_UPSTREAM: 从 SinkPad pop → 写入 avformat 上下文 → av_read_frame
    //   EOF 时发送 EOS

    void onNull() override;
    // avformat_close_input
};
```

### 16.2 DecodeNode

```cpp
class DecodeNode : public TransformNode {
public:
    // 参数:
    //   "thread_count" : int — 解码线程数，默认 1

    void onLink(SinkPad* pad, const StreamInfo& info) override;
    // 保存 codecpar 和 time_base

    void onReady() override;
    // avcodec_alloc_context3 + avcodec_parameters_to_context + avcodec_open2
    // 更新输出 StreamInfo → push STREAM_INFO_CHANGED

    // process:
    //   avcodec_send_packet → 循环 avcodec_receive_frame → Buffer::fromAVFrame
    //   EAGAIN → return nullptr (需要更多 packet)

    // handleEOS:
    //   avcodec_send_packet(NULL)  // flush
    //   循环 avcodec_receive_frame → 推送剩余帧
    //   发送 EOS 到 SrcPad

    void onNull() override;
    // avcodec_free_context
};
```

### 16.3 EncodeNode

```cpp
class EncodeNode : public TransformNode {
public:
    // 参数:
    //   "codec"    : std::string — "libx264", "libx265", "aac" 等
    //   "bitrate"  : int
    //   "width"    : int (可选)
    //   "height"   : int (可选)
    //   "fps"      : AVRational
    //   "gop"      : int

    void onLink(SinkPad* pad, const StreamInfo& info) override;
    // 保存输入格式信息

    void onReady() override;
    // avcodec_find_encoder + 设置 ctx + avcodec_open2
    // 初始化 sws_scale (像素格式转换) 或 swr_convert (重采样)
    // 更新输出 StreamInfo

    // process:
    //   如需 sws_scale/swr_convert → 转换
    //   avcodec_send_frame → avcodec_receive_packet → Buffer::fromAVPacket
};
```

### 16.4 MuxNode

```cpp
class MuxNode : public TransformNode {
public:
    // 参数:
    //   "format" : std::string — "mp4", "flv", "mpegts" 等

    // SinkPad: 动态创建，通过 requestSinkPad()
    // SrcPad: "out" (muxed data chunks)

    SinkPad* requestSinkPad(const std::string& name) override;

    void onLink(SinkPad* pad, const StreamInfo& info) override;
    // 收集 StreamInfo

    void onReady() override;
    // avformat_alloc_output_context2
    // avformat_new_stream × N
    // 创建自定义 AVIOContext (write callback → push to SrcPad)
    // avformat_write_header

    // workerLoop:
    //   轮询所有 SinkPad，选 DTS 最小的
    //   av_interleaved_write_frame → write callback → push

    // handleAllEOS:
    //   av_write_trailer → push EOS 到 SrcPad

    void onNull() override;
    // avformat_free_context + avio_context_free
};
```

### 16.5 V4L2CaptureNode

```cpp
class V4L2CaptureNode : public SourceNode {
public:
    // 参数:
    //   "device" : std::string — "/dev/video0"
    //   "width"  : int
    //   "height" : int
    //   "fps"    : int

    void onProbe() override;
    // open(device) + VIDIOC_QUERYCAP + VIDIOC_ENUM_FMT
    // 创建 SrcPad + StreamInfo (RAWVIDEO, YUYV/MJPEG/...)

    void onReady() override;
    // VIDIOC_S_FMT + VIDIOC_REQBUFS + mmap + VIDIOC_STREAMON

    // generateData:
    //   VIDIOC_DQBUF → Buffer::fromRawData → return buffer

    // isEOF:
    //   return false (采集永无 EOF)

    void onNull() override;
    // VIDIOC_STREAMOFF + munmap + close
};
```

### 16.6 AudioCaptureNode

```cpp
class AudioCaptureNode : public SourceNode {
public:
    // 参数:
    //   "device"      : std::string — "default"
    //   "sample_rate" : int — 48000
    //   "channels"    : int — 2

    void onProbe() override;
    // snd_pcm_open + query → StreamInfo (PCM_S16LE)

    void onReady() override;
    // snd_pcm_hw_params + snd_pcm_prepare

    // generateData:
    //   snd_pcm_readi → Buffer::fromRawData

    void onNull() override;
    // snd_pcm_close
};
```

### 16.7 VideoRenderNode

```cpp
class VideoRenderNode : public SinkNode {
public:
    // 参数:
    //   "title"  : std::string — 窗口标题
    //   "width"  : int (可选，默认跟随视频)
    //   "height" : int (可选)
    //   "fullscreen" : bool

    void onReady() override;
    // SDL_Init(SDL_INIT_VIDEO)
    // SDL_CreateWindow + SDL_CreateRenderer
    // SDL_CreateTexture (根据 StreamInfo 的像素格式)
    // 如果 SDL 不支持输入像素格式，初始化 sws_scale 转换

    // consume:
    //   A/V Sync 等待
    //   如需格式转换: sws_scale
    //   SDL_UpdateYUVTexture / SDL_UpdateTexture
    //   SDL_RenderPresent

    void onNull() override;
    // SDL_DestroyTexture + SDL_DestroyWindow + SDL_Quit
};
```

### 16.8 AudioPlayNode

```cpp
class AudioPlayNode : public SinkNode {
public:
    // 参数:
    //   "sample_rate" : int — 48000
    //   "channels"    : int — 2
    //   "format"      : std::string — "s16", "flt"

    void onReady() override;
    // SDL_Init(SDL_INIT_AUDIO)
    // SDL_OpenAudioDevice
    // SDL_CreateAudioStream
    // SDL_BindAudioStream
    // SDL_ResumeAudioDevice
    // 如需重采样，初始化 swr_convert

    // consume:
    //   如需重采样: swr_convert
    //   SDL_PutAudioStreamData
    //   推进全局 Clock

    void onNull() override;
    // SDL_CloseAudioDevice + SDL_DestroyAudioStream
};
```

### 16.9 FileSinkNode

```cpp
class FileSinkNode : public SinkNode {
public:
    // 参数:
    //   "path" : std::string — 输出文件路径

    void onReady() override;
    // fopen(path, "wb")

    // consume:
    //   fwrite(data, size, 1, file)

    // handleEOS:
    //   flush + fclose
    //   reportSinkEOS

    void onNull() override;
    // fclose
};
```

### 16.10 QueueNode

```cpp
class QueueNode : public TransformNode {
public:
    // 参数:
    //   "max_size"        : int — 最大缓冲帧数，默认 500
    //   "overflow_policy" : Policy — BLOCK / DROP_OLDEST / DROP_NEWEST

    void onReady() override;
    // 无操作（队列在 Pad 上）

    // process:
    //   return input;  // 透传，缓冲在 SinkPad 的 BoundedQueue 中
};
```

## 17. 项目文件结构

头文件（公有 API）与实现分离，`include/pipeline/` 暴露接口，`src/` 放实现。

```text
media-pipeline/
├── CMakeLists.txt                          # 顶层构建配置
├── Media_Pipeline_Framework.md             # 本文档
│
├── cmake/
│   ├── toolchains/
│   │   ├── aarch64.cmake                   # ARM64 交叉编译 Toolchain
│   │   └── riscv64.cmake                   # RISC-V 占位
│   ├── FindFFmpeg.cmake                    # FFmpeg 查找脚本
│   └── FindSDL3.cmake                      # SDL3 查找脚本
│
├── include/pipeline/
│   ├── core/                               # 核心框架公有头文件
│   │   ├── Buffer.h                        # Buffer + MemoryBlock + MemoryPool
│   │   ├── BoundedQueue.h                  # 有界队列模板（Pad 内部使用）
│   │   ├── Clock.h                         # A/V 同步时钟
│   │   ├── Event.h                         # 事件定义（EOS / FLUSH / SEEK 等）
│   │   ├── INode.h                         # INode + SourceNode + TransformNode + SinkNode
│   │   ├── MessageBus.h                    # Message + MessageBus
│   │   ├── Pad.h                           # Pad 基类 + SrcPad + SinkPad
│   │   ├── Pipeline.h                      # Pipeline（状态机 + 7 阶段 play）
│   │   ├── StreamInfo.h                    # StreamInfo 结构体
│   │   └── Types.h                         # 枚举（NodeState、MediaType、MemoryTier 等）
│   │
│   ├── nodes/                              # 节点公有头文件
│   │   ├── DemuxNode.h                     # FFmpeg 解复用
│   │   ├── DecodeNode.h                    # FFmpeg 解码
│   │   ├── EncodeNode.h                    # FFmpeg 编码
│   │   ├── MuxNode.h                       # FFmpeg 复用
│   │   ├── QueueNode.h                     # 缓冲队列节点
│   │   ├── VideoRenderNode.h               # SDL3 视频渲染
│   │   ├── AudioPlayNode.h                 # SDL3 音频播放
│   │   ├── FileSinkNode.h                  # 文件输出
│   │   ├── V4L2CaptureNode.h               # V4L2 视频采集（第三阶段）
│   │   ├── AudioCaptureNode.h              # ALSA 音频采集（第三阶段）
│   │   └── RTSPPushNode.h                  # RTSP 推流（后期）
│   │
│   └── utils/                              # 工具公有头文件
│       ├── FFmpegUtils.h                   # FFmpeg 辅助函数
│       ├── Logger.h                        # 日志
│       └── Error.h                         # 错误码定义
│
├── src/
│   ├── CMakeLists.txt
│   ├── main.cpp                            # 入口
│   │
│   ├── core/                               # 核心框架实现
│   │   ├── Buffer.cpp
│   │   ├── Clock.cpp
│   │   ├── Event.cpp
│   │   ├── INode.cpp
│   │   ├── MessageBus.cpp
│   │   ├── Pad.cpp
│   │   └── Pipeline.cpp
│   │
│   ├── nodes/                              # 节点实现
│   │   ├── DemuxNode.cpp
│   │   ├── DecodeNode.cpp
│   │   ├── EncodeNode.cpp
│   │   ├── MuxNode.cpp
│   │   ├── QueueNode.cpp
│   │   ├── VideoRenderNode.cpp
│   │   ├── AudioPlayNode.cpp
│   │   ├── FileSinkNode.cpp
│   │   ├── V4L2CaptureNode.cpp             # 第三阶段
│   │   ├── AudioCaptureNode.cpp            # 第三阶段
│   │   └── RTSPPushNode.cpp                # 后期
│   │
│   └── utils/                              # 工具实现
│       ├── FFmpegUtils.cpp
│       └── Logger.cpp
│
├── demo/                                   # 演示程序
│   ├── CMakeLists.txt
│   ├── player.cpp                          # 播放本地文件
│   ├── capture_preview.cpp                 # 采集预览
│   ├── capture_record.cpp                  # 采集编码录制
│   ├── file_relay.cpp                      # 文件推流（不转码）
│   └── file_transcode_push.cpp             # 文件推流（转码）
│
└── tests/                                  # 单元测试
    ├── CMakeLists.txt
    ├── test_pipeline.cpp                   # Pipeline 构建、状态机测试
    ├── test_pad_link.cpp                   # Pad 连接、协商测试
    ├── test_buffer.cpp                     # Buffer + MemoryPool 生命周期测试
    ├── test_bounded_queue.cpp              # 队列背压测试
    ├── test_av_sync.cpp                    # A/V 同步测试
    └── test_eos.cpp                        # EOS 传播测试
```

## 18. 嵌入式约束

以下约束贯穿整个框架实现，违反任何一条即为 bug。

1. **不使用 C++ 异常**：编译时加 `-fno-exceptions`。错误通过返回码、`std::optional`、`std::get_if` 传递。`Queue::pop()` 返回 `std::optional`，flush 后返回 `std::nullopt`；参数系统用 `std::variant` + `std::get_if` 替代 `std::any`。

2. **不使用 RTTI**：编译时加 `-fno-rtti`。禁止 `dynamic_cast`、`typeid`。命令系统使用 `typeId()` + switch 分发，不依赖 RTTI。

3. **不使用 `recursive_mutex`**：用普通 `std::mutex` + `std::unique_lock`。

4. **不允许忙等**：所有等待必须用 `condition_variable` 或 `clock_nanosleep`，禁止 `while(!ready) {}`。

5. **线程绑核（预留）**：关键线程（渲染、音频）可通过 `pthread_setaffinity_np` 固定到指定核心，接口在 Node 上预留 `setParam("cpu_affinity", core_id)`。

6. **Buffer 不允许 memcpy**：节点间传递 Buffer 只传 `shared_ptr`，不拷贝数据。唯一例外是 `Buffer::fromAVPacket` / `fromAVFrame` 从 FFmpeg 结构体拷入 MemoryPool 的那一次。

7. **优雅停止**：`stop()` 后所有节点线程必须能退出，不允许有线程永久阻塞在 `pop()` 上。FLUSH 事件必须能唤醒所有阻塞中的线程。

8. **FFmpeg 静态链接**：每个目标架构单独编译一份 FFmpeg 静态库，放在 `third_party/ffmpeg/<arch>/` 下。
