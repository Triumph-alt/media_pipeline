# Media Pipeline Framework 设计文档

## 1. 项目概述

### 1.1 目标

构建一个基于有向无环图（DAG）的音视频全链路处理框架，运行于 Linux / 嵌入式 Linux 平台。用户只需声明节点与连接关系，框架自动完成拓扑管理、Caps 协商、线程调度、数据流转，从而实现采集、编码、解码、解复用、复用、渲染、播放、推流、本地录制等任意组合的音视频处理链路。

### 1.2 核心设计理念

- **DAG 驱动**：Pipeline 内部维护显式有向图，拓扑结构一等公民，Build 阶段完成全图校验
- **节点自由组合**：用户随意连接节点，框架负责协商、调度、数据流转，用户不感知内部细节
- **Pad 一对一，分叉靠多 Pad**：每个 Pad 严格连接一个对端 Pad，分叉通过节点动态创建多个 SrcPad 实现
- **逻辑流即 Route**：每条逻辑输出流拥有一个有界多订阅者 Route；Edge 持有独立 Subscription，速率差由 Route 保留窗口吸收
- **两阶段 Caps 协商**：Build 时静态类型检查，Ready 时动态参数传递，不兼容连接在构建期即报错
- **线程归 Pipeline 管**：节点不持有线程，Pipeline 统一创建和销毁所有节点线程

### 1.3 依赖

| 库 | 用途 |
|---|------|
| FFmpeg (libavformat, libavcodec, libavutil, libswscale, libswresample) | 解复用、复用、编解码、像素/音频格式转换 |
| SDL3 | 视频渲染、音频播放 |
| V4L2（内核接口） | 视频采集 |
| ALSA (libasound) | 音频采集 |
| CMake 3.16+ | 构建系统 |

### 1.4 第一阶段不做的事情

- 不支持硬件编解码加速（VAAPI / V4L2 M2M）
- 不支持 DMA-BUF 零拷贝（Buffer 第一阶段全部拷贝）
- 不支持动态插件加载（.so）
- 不支持运行时格式变化（CapsEvent 只在 Ready 阶段传递一次）
- 不支持 Windows / macOS

### 1.5 典型链路示例

```
本地文件播放（音视频）：
DemuxNode(url) ──video_0──→ DecodeNode ──out_0──→ VideoRenderNode
               ──audio_0──→ DecodeNode ──out_0──→ AudioPlayNode

采集预览：
V4L2CaptureNode ──out_0──→ VideoRenderNode

采集编码，同时推流 + 本地录制（分叉）：
V4L2CaptureNode ──out_0──→ EncodeNode ──out_0──→ MuxNode ──out_0──→ RTSPPushNode
AudioCaptureNode──out_0──→ EncodeNode ──out_1──→ MuxNode ──out_0──→ FileSinkNode
                                        out_1──→ MuxNode(file)

文件转码推流：
DemuxNode(url) ──video_0──→ DecodeNode ──out_0──→ EncodeNode ──out_0──→ MuxNode ──out_0──→ RTSPPushNode
               ──audio_0──→ DecodeNode ──out_0──→ EncodeNode ──out_0──→ MuxNode

直接推流（不转码）：
DemuxNode(url) ──video_0──→ MuxNode ──out_0──→ RTSPPushNode
               ──audio_0──→ MuxNode
```

---

## 2. 整体架构

### 2.1 层次结构

```
Pipeline
├── Graph（显式有向图，邻接表）
│   ├── Node（五类节点）
│   │   └── Pad（SrcPad / SinkPad，严格一对一）
│   └── Edge（连接两个 Pad，持有一个 RouteSubscription）
├── Clock（主时钟，AV Sync）
├── MessageBus（节点向 Pipeline 上报消息）
└── 线程表（每个 Node 对应一个 std::thread）
```

### 2.2 数据流模型

数据流方向：上游节点 → 逻辑 OutputRoute → Edge Subscription → 下游节点。

- 每条逻辑输出流拥有一个静态、有界、多订阅者 `OutputRoute`；同源分叉的多个 SrcPad 共享该 Route。
- 上游对每个 `QueueItem` 只 publish 一次；各 Edge 持有独立 `RouteSubscription` 读取游标。
- `BufferRef` 分叉只增加原子引用计数，所有订阅者共享同一个只读 Buffer payload，不做深拷贝。
- 下游 acquire 得到 `RouteDelivery`；只有节点处理完成并 ack 后，对应订阅游标才推进。
- Route Entry 只有在所有静态可靠订阅者都 ack 后才能回收。
- Route 达到硬容量时 publish 阻塞；最慢订阅者释放空间后唤醒 publisher，背压由此逐级向上游传导。
- CapsEvent、Buffer、EOSEvent 共用同一 Route 序列，保证所有订阅者看到严格一致的顺序。
- stop/error 通过 cancel 全部 Route 唤醒 publish/acquire；自然 EOS 不使用 cancel，而是作为有序 Event 正常处理。
- 每个 Sink 在处理并 ack EOS 后向 Pipeline 报告完成；所有 Sink 完成后 Pipeline 才 stop/cancel，因此不会截断其他静态订阅者尚未处理的 Route 数据。

```
SourceNode ──publish once──→ [OutputRoute]
                              ├─ Subscription A ──acquire/process/ack──→ Sink A
                              └─ Subscription B ──acquire/process/ack──→ Sink B
```

短期消费速率差由 Route 的有界保留窗口吸收；当最慢可靠订阅者落后达到硬容量后，共同生产源最终被反压。这是在可靠传输与有限内存约束下的正式语义。

### 2.3 节点五分类

| 类型 | SinkPad 数量 | SrcPad 数量 | 驱动方式 |
|------|-------------|-------------|---------|
| **SourceNode** | 0 | 动态（≥1） | 独立线程，阻塞采集 |
| **SinkNode** | 动态（≥1） | 0 | 独立线程，阻塞消费 |
| **TransformNode** | 1 | 动态（≥1） | 独立线程，从 SinkPad Subscription acquire 驱动 |
| **DemuxNode** | 0 | 动态（≥1，link 阶段创建） | 独立线程，读文件/URL 驱动 |
| **MuxNode** | 动态（≥1） | 1 | 独立线程，多路 Subscription 通知与 DTS 调度 |

**Pad 数量说明**：说"动态"是指 Build 阶段根据用户 link 调用动态创建，每次 link 在对应节点上增加一个 Pad。

---

## 3. 核心数据结构

### 3.1 Buffer

Buffer 是框架内所有数据的载体，拥有独立的引用计数体系，与 FFmpeg 的 AVFrame / AVPacket 解耦。节点从 FFmpeg 结构中拷贝数据填入 Buffer 后，立即释放原始 FFmpeg 结构。

BufferMeta 中部分基础格式字段与 CapsEvent 重复，**下游初始化以 Ready 阶段 CapsEvent 为准**，BufferMeta 则更多的是随每个 Buffer 携带 packet/frame 级属性，例如 EncodedMeta::flags、AudioRawMeta::nb_samples。同时此字段也为后续运行时格式变化（STREAM_INFO_CHANGED）预留，届时逐帧携带格式信息可处理同一流中途切换分辨率/采样率等场景

Buffer 层只忠实承载 FFmpeg 时间戳：pts/dts 无效时保留 AV_NOPTS_VALUE，duration 无效时为 0，不在此处推算时间；stream_index/pos/side_data 当前不进入 Buffer：流身份由 SrcPad/Edge 表达，seek/HDR/rotation/SEI 等后续单独设计

AudioRaw Buffer 保持 FFmpeg sample_fmt 对应的原始布局；planar 数据按 plane 顺序拼接，下游通过 sample_fmt/channels/nb_samples 解释

```cpp
enum class MediaType {
    VIDEO_RAW,       // 解码后的视频帧（YUV / RGB）
    AUDIO_RAW,       // 解码后的音频帧（PCM）
    VIDEO_ENCODED,   // 编码后的视频 Packet（H264 / H265 等）
    AUDIO_ENCODED,   // 编码后的音频 Packet（AAC / Opus 等）
    CONTAINER,       // 容器封装后的数据（Mux 输出）
};

struct VideoRawMeta {
    int           width, height;
    AVPixelFormat pix_fmt;
    // framerate 不存在这里，由 fromAVFrame() 的参数传入用于计算 duration，
    // meta 本身只保留这一帧的固有属性
};

struct AudioRawMeta {
    int             sample_rate;
    int             channels;
    int             nb_samples;
    AVSampleFormat  sample_fmt;
};

struct EncodedMeta {
    AVCodecID              codec_id;
    int                    width, height;       // 视频
    int                    sample_rate;         // 音频
    int                    channels;            // 音频
    int                    flags;               // AVPacket flags，如 AV_PKT_FLAG_KEY
    std::vector<uint8_t>   extradata;           // SPS/PPS 等
};

using BufferMeta = std::variant<VideoRawMeta, AudioRawMeta, EncodedMeta>;

class Buffer {
public:
    // 数据
    uint8_t*    data     = nullptr;
    size_t      size     = 0;

    // 时间戳
    int64_t     pts      = AV_NOPTS_VALUE;   // 单位：微秒
    int64_t     dts      = AV_NOPTS_VALUE;   // 单位：微秒
    int64_t     duration = 0;                // 单位：微秒

    // 类型与元信息
    MediaType   media_type;
    BufferMeta  meta;

    // 引用计数
    mutable std::atomic<int> ref_count{1};
    void ref() const;
    void unref() const;
    BufferRef share() const;

    // 独立深拷贝工具；正式分叉传输使用共享只读 BufferRef，不调用 clone()
    Buffer* clone() const;

    // 工厂方法：从 FFmpeg 结构拷入数据，之后由调用者释放 FFmpeg 结构
    // time_base：用于将 FFmpeg 时间戳转为微秒
    // codec_id：填入 EncodedMeta（fromAVPacket 专用）
    // framerate：用于计算视频帧 duration（fromAVFrame 专用，从 CapsEvent 传入）
    static Buffer* fromAVPacket(const AVPacket* pkt, MediaType type,
                                AVRational time_base, AVCodecID codec_id = AV_CODEC_ID_NONE);
    static Buffer* fromAVFrame(const AVFrame* frame, MediaType type,
                               AVRational time_base, AVRational framerate = {0, 1});

private:
    ~Buffer() { delete[] data; }
};

// RAII 包装，自动管理 Buffer 生命周期
class BufferRef {
public:
    explicit BufferRef(const Buffer* buf) : buf_(buf) {}
    ~BufferRef() { if (buf_) buf_->unref(); }
    BufferRef(const BufferRef& other);      // ref +1
    BufferRef(BufferRef&& other) noexcept;
    const Buffer* get() const { return buf_; }
    const Buffer* operator->() const { return buf_; }
    BufferRef clone() const { return BufferRef(buf_->clone()); } // 显式深拷贝工具
private:
    const Buffer* buf_;
};
```

### 3.2 Caps（能力描述）

Caps 分两种：TemplateCaps 是节点在定义时静态声明的能力范围（Build 阶段用于类型检查），CapsEvent 是运行时携带真实参数的动态事件（Ready 阶段顺流传递）。

```cpp
// 静态能力描述：只声明能接受/产出的 MediaType 集合
struct TemplateCaps {
    std::vector<MediaType> supported_types;

    bool isCompatibleWith(const TemplateCaps& other) const {
        for (auto t : supported_types)
            for (auto o : other.supported_types)
                if (t == o) return true;
        return false;
    }

    bool contains(MediaType t) const {
        for (auto s : supported_types)
            if (s == t) return true;
        return false;
    }
};

// 动态参数事件：携带运行时确定的真实参数
struct CapsEvent {
    MediaType   media_type;
    // 视频字段
    int         width       = 0;
    int         height      = 0;
    AVPixelFormat pix_fmt   = AV_PIX_FMT_NONE;
    AVRational  framerate   = {0, 1};
    // 音频字段
    int         sample_rate = 0;
    int         channels    = 0;
    AVSampleFormat sample_fmt = AV_SAMPLE_FMT_NONE;
    // 编码字段（encoded 类型时有效）
    AVCodecID   codec_id    = AV_CODEC_ID_NONE;
    std::vector<uint8_t> extradata;   // SPS/PPS 等
};
```

+ isCompatibleWith 函数检验两端 TemplateCaps 是否有交集，Graph::link 阶段使用
+ contains 函数检验某具体类型是否属于本能力集合，主要是 requestPad 里做"hint_type 是否落在已有 pad 能力集合内"的分叉检查，以及 send/receiveCapsEvent 里做"CapsEvent.media_type 是否落在 pad 能力集合内"的校验

### 3.3 Event（事件）

事件与数据使用 variant 表达，并在同一 OutputRoute 有序日志中传递，保证所有订阅者看到一致的相对顺序。

```cpp
struct EOSEvent {};

using Event = std::variant<CapsEvent, EOSEvent>;

// Route 日志中存放的元素：Buffer 或 Event
using QueueItem = std::variant<BufferRef, Event>;
```

### 3.4 OutputRoute、RouteSubscription、RouteDelivery 与 Edge

`OutputRoute` 是逻辑流的数据面：它保存一份有序 `QueueItem` 日志和所有静态可靠订阅者的游标。`Edge` 不再持有独立队列，而是持有源 Route 的一个 `RouteSubscription`。

核心状态：

```text
head_sequence = 当前仍保留的最早 Entry
tail_sequence = 下一次 publish 的序号
subscriber.next_sequence = 该订阅者下一次 acquire 的序号
retained = tail_sequence - min(all subscriber.next_sequence)
```

订阅者 acquire 时不会推进游标；`RouteDelivery::ack()` 才表示本轮节点处理完成。未 ack 的 Delivery 析构只撤销 in-flight 状态，使同一订阅者可重试该项，不会把未处理数据标记为完成。

```cpp
enum class RoutePublishResult {
    PUBLISHED,
    CANCELLED,
    NO_SUBSCRIBERS,
};

class OutputRoute {
public:
    RouteSubscription subscribe();       // 只允许 seal 前调用
    bool seal();                          // build 后订阅集合永久封闭
    RoutePublishResult publishBlocking(QueueItem item);
    void cancel();
    void resize(size_t capacity);
};

class RouteSubscription {
public:
    std::optional<RouteDelivery> acquireBlocking();
    std::optional<RouteDelivery> tryAcquire();
    std::optional<QueueItem> peek() const;
};

class RouteDelivery {
public:
    const QueueItem& item() const;
    bool ack();
};

struct Edge {
    BaseNode* src_node;
    std::string src_pad_name;
    BaseNode* dst_node;
    std::string dst_pad_name;
    RouteSubscription subscription;
};
```

容量按实际 `MediaType` 设置：VIDEO_RAW 4、AUDIO_RAW 50、VIDEO_ENCODED/AUDIO_ENCODED 32、CONTAINER 32。当前容量按条目精确执行；字节预算属于后续扩展。

项目拓扑是静态的：全部 Subscription 在 `link()` 阶段建立，`build()` 成功后全部 Route seal；Running 期间不存在动态 link/unlink 或订阅者增删语义。

## 4. Pad 设计

### 4.1 Pad 基类

Pad 承载两层类型信息：

- **`template_caps_`（TemplateCaps，能力集合，静态）**：构造 / requestPad 时确立，声明"本 pad 可承载的 MediaType 集合"。Build 阶段 `Graph::link` 用它做粗粒度的交集兼容性检查。
- **`actual_type_`（optional<MediaType>，实际类型，动态）**：Ready 阶段 CapsEvent 流经 pad 时由 `BaseNode::sendCapsEvent` / `receiveCapsEvent` 内部设置，代表本 pad 实际承载的具体类型。Ready 之前查询得 `nullopt` 是契约的一部分；Ready 之后所有已连接 pad 的 `actualType()` 必有值。

两者严格分层：`template_caps_` 是能力声明，`actual_type_` 是运行时事实。runLoop 阶段的类型分发一律读 `actualType()`，绝不把能力集合的其中某一个成员单独作为"实际类型"来使用

```cpp
enum class PadDir { SRC, SINK };

class Pad {
public:
    const std::string& name() const { return name_; }
    PadDir             dir()  const { return dir_; }
    BaseNode*          node() const { return node_; }
    TemplateCaps       templateCaps() const { return template_caps_; }

    // 返回具体类型，当然在 Ready 被调用之前是返回 nullopt
    std::optional<MediaType> actualType() const { return actual_type_; }

    bool isConnected() const { return edge_ != nullptr; }
    Edge* edge() const { return edge_; }

protected:
    std::string    name_;
    PadDir         dir_;
    BaseNode*      node_;
    TemplateCaps   template_caps_;
    Edge*          edge_ = nullptr;   // 连接到的 Edge（一对一）

    friend class Graph;  // Graph::link 里写 edge_

private:
    // actual_type 的唯一设值时机就是 BaseNode 的 send/receiveCapsEvent 内部，CapsEvent 流经 pad 且通过 TemplateCaps 校验时
    // 其他任何路径（子类节点、Graph、requestPad 等）都不得直接设置
    void setActualType(MediaType t) { actual_type_ = t; }

    std::optional<MediaType> actual_type_;

    friend class BaseNode;  // 仅为 send/receiveCapsEvent 授权设置 actual_type_
};
```

此处 `setActualType` 设计为 private + friend BaseNode，是因为 actual_type 的唯一合法写入路径就是 BaseNode 的 send/receiveCapsEvent，所以此处通过代码组织让契约在源码层面可见

### 4.2 SrcPad

SrcPad 仍然与 Edge 一对一连接，但它绑定到节点的一条逻辑 `OutputRoute`。Source/Transform 的分叉 SrcPad 共享第一个输出 Pad 的 Route；Demux 的同类型 Pad 共享对应媒体流 Route，不同媒体类型使用不同 Route。上游不再逐 Pad push，而是向 Route publish 一次。

### 4.3 SinkPad

SinkPad 通过 Edge 持有的 `RouteSubscription` 读取数据：

```cpp
std::optional<RouteDelivery> acquireBlocking();
std::optional<RouteDelivery> tryAcquire();
std::optional<QueueItem> peek() const;
```

SinkNode/TransformNode/MuxNode 在处理完成后显式 ack Delivery，Route 容量此时才可能释放。Mux 的 `peek()` 和 `tryAcquire()` 都相对于自己的 Subscription 游标，而不是查询 Route 的全局空闲状态。

## 5. 节点（Node）设计

### 5.1 BaseNode（抽象基类）

节点生命周期由 `stop_requested_` 原子标志 + MessageBus 表达：正常退出由 Pipeline::stop() 置位 `stop_requested_` 触发；出错时节点调用 `postMessage(ERROR)`，内部同步置位 `stop_requested_`，Pipeline 从 MessageBus 侧收集错误文本。BaseNode 本身不维护额外的状态字段。

```cpp
class BaseNode {
public:
    virtual ~BaseNode() = default;

    const std::string& name()  const { return name_; }

    // Pad 访问
    SrcPad*  getSrcPad(const std::string& name);
    SinkPad* getSinkPad(const std::string& name);
    const std::vector<std::unique_ptr<SrcPad>>&  srcPads()  const;
    const std::vector<std::unique_ptr<SinkPad>>& sinkPads() const;

    // 节点元信息（用于 Graph 做静态检查）
    virtual NodeType nodeType() const = 0;

protected:
    // 例：DemuxNode(const std::string& name, const std::string& url)
    //         : BaseNode(name), url_(url) {}
    explicit BaseNode(const std::string& name) : name_(name) {}

    // === 子类实现的生命周期回调 ===

    // Ready 阶段第一步：初始化自身资源
    // 返回 true 表示成功，false 表示失败（失败时应先往 MessageBus 发 ERROR 消息）
    virtual bool onReady() = 0;

    // Ready 阶段：发送/接收 CapsEvent（Route 已在 build 时 seal）
    // 返回 true 表示成功，false 表示失败
    virtual bool onStreamInfo() { return true; }

    // 节点停止或 Ready 回滚时释放资源；必须支持部分初始化状态
    virtual void onStop() = 0;

    // === 子类的运行循环（由 Pipeline 创建的线程调用）===
    virtual void runLoop() = 0;

    // === 基类统一的数据分发（子类调用，不感知下游数量）===
    bool pushToDownstream(Buffer* buf, const std::string& src_pad_name = "");

    // 每条不同的逻辑 Route 只 publish 一次 EOS；同一 Route 的所有订阅者各自 acquire/ack
    void sendEOSDownstream();

    // 将 CapsEvent 向指定 SrcPad 所属 Route 可靠 publish 一次
    bool sendCapsEvent(const std::string& src_pad_name, const CapsEvent& caps);

    // 从指定 SinkPad acquire、校验并 ack CapsEvent
    bool receiveCapsEvent(const std::string& sink_pad_name);

    // 动态创建 Pad（节点构造时声明固定 Pad 时调用，子类构造函数里直接调用）
    SrcPad*  addSrcPad(const std::string& name, TemplateCaps caps);
    SinkPad* addSinkPad(const std::string& name, TemplateCaps caps);

    /*  动态请求 Pad：hint_type 是用户在 link() 调用时传入的类型提示，节点据此判断：
        类型合法则创建并返回新 Pad
        类型不合法（不支持的 MediaType 或与节点已有输出类型冲突）则返回 nullptr
        默认实现返回 nullptr，表示该节点不支持动态创建 Pad
    */
    virtual SrcPad*  requestSrcPad(const std::string& name, MediaType hint_type) { return nullptr; }
    virtual SinkPad* requestSinkPad(const std::string& name, MediaType hint_type) { return nullptr; }

    // Graph::link 后续失败时，只释放本次 request 创建且尚未连接的 Pad
    virtual bool releaseSrcPad(SrcPad* pad);
    virtual bool releaseSinkPad(SinkPad* pad);

    // 成员
    std::string                              name_;
    Pipeline*                                pipeline_ = nullptr;
    std::vector<std::unique_ptr<SrcPad>>     src_pads_;
    std::vector<std::unique_ptr<SinkPad>>    sink_pads_;
    // 收到的 CapsEvent（key: SinkPad 名字，value: 从上游收到的 CapsEvent）
    // 非源节点在 onStreamInfo() 中从 RouteSubscription acquire、校验并 ack 后存入
    std::unordered_map<std::string, CapsEvent> negotiated_caps_;
    std::atomic<bool>                     stop_requested_{false};

    friend class Pipeline;
    friend class Graph;
};
```

几点注意事项：
1. 所有具体节点子类必须在初始化列表里调用 BaseNode 构造函数，将 name 传给 name_，因为 BaseNode 节点没有默认构造函数，所以子类忘记调用会直接编译报错，这样子就不会出现某个节点忘记设置名字导致 name_ 是空字符串的情况
2. 子类的 onReady 函数具体实现取决于自身需要，比如 Source/DemuxNode 就是打开设备/文件，探测流信息，但不发送 CapsEvent；Transform/SinkNode 只做基础初始化，因为此时尚未收到 CapsEvent，还无法确定具体的参数
3. 子类的 onStreamInfo 函数具体实现也是取决于自身需要，比如 Source/DemuxNode 就是构造并发送 CapsEvent；Transform/SinkNode 收到上游 CapsEvent 后初始化处理器，再发出自己的 CapsEvent；默认实现返回 true；Sink 节点无需发送
4. `pushToDownstream` 向一个逻辑 OutputRoute 可靠 publish 一次；同源多个 SrcPad 共享 Route，因此节点不再逐 Pad 分发，也不存在“单路阻塞、多路 tryPush”的分支。不同逻辑流（例如 Demux video/audio）必须显式选择各自 Route。
5. `pushToDownstream` 调用时转移新建 Buffer 的初始所有权给 `BufferRef`/Route。Route Entry 只保存一份共享只读 BufferRef；每个订阅者 acquire 时复制句柄、引用计数加一，不复制 payload。所有订阅者 ack 后 Entry 才回收，最后一个 BufferRef 析构后底层 Buffer 才释放。
6. `sendCapsEvent` 与 `receiveCapsEvent` 是 Ready 阶段 CapsEvent 收发的标准封装：发送端校验同一 Route 上所有 Pad 的 TemplateCaps、设置 actualType 并只 publish 一次；接收端 acquire、校验、存入 negotiated_caps_ 后 ack。校验失败会 postMessage(ERROR)，未完成 Delivery 不推进游标，Ready 回滚随后 cancel 全部 Route。
    + 同一逻辑 Route 的多个 SrcPad 共享一条 Caps 序列，`sendCapsEvent` 通过任一该 Route 的 src_pad_name 定位后只 publish 一次；接收方分别通过各自 Subscription acquire/ack
    + `receiveCapsEvent` 失败场景包括 Route cancel、取得的不是 CapsEvent、或 caps.media_type 不属于 SinkPad.templateCaps
7. 动态请求 Pad 只在 link 时，目标 Pad 不存在时调用，节点构造时已声明的固定 Pad（比如 TransformNode 的 "in"）不走这条路，link 会优先查找已存在的 Pad，只有当 Pad 不存在时，才调用这两个方法，由节点自己决定是否允许动态创建、创建出来的 Pad 应该是什么类型。**需要支持分叉的节点（Source/Transform 的多路输出）和多路输入的节点（DemuxNode、MuxNode）一定需要重写对应的方法**。requestPad 有两种不同的语义模型，各自对应不同的 TemplateCaps 处理方式：
    + Source / Transform 分叉：属于再多一路同源输出，新 pad 的 TemplateCaps 必须和已经存在的 pad 的 TemplateCaps 一致，其最终的 ActualType 自然也落在已有 pad 的能力集合内，因为这种情况本质是同一份处理结果的多路拷贝，所以一个 SourceNode/TransformNode 内所有 SrcPad 的能力声明应当一致，所有 SinkPad 的能力声明应当一致
    + Demux / Mux 多路：属于开一路服务 hint_type 的具体流端口，新 pad 的 TemplateCaps 就是 `{hint_type}`，一个 pad 服务一种流身份。因为在 Demux/Mux 这种节点中，每个 SinkPad 对应容器里一路流，各 pad 天然可能会承载不同类型
8. requestPad 中的 hint_type **只用于 Ready 之前的能力校验与决定新 pad 的 TemplateCaps，不代表"实际类型 actual_type"**。实际类型由 Ready 阶段的 CapsEvent 敲定，requestPad 不应调用 `setActualType` 直接设置 actual_type，哪怕传入的 hint_type 和事后敲定的最终类型一致
9. requestPad 会立即把新 Pad 加入节点，因此 `Graph::link` 后续任一步失败都需要调用 `releaseSrcPad` / `releaseSinkPad` 撤销本次新建 Pad，如果有附属状态的节点必须同步清理，例如 DemuxNode release SrcPad 时同时删除 `pad_to_type_` 项


### 5.2 SourceNode

```cpp
class SourceNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::SOURCE; }

protected:
    explicit SourceNode(const std::string& name) : BaseNode(name) {}
    void runLoop() override {
        while (!stop_requested_.load()) {
            // 子类实现：阻塞采集一帧数据
            auto* buf = capture();
            if (!buf) {
                // EOF：发送 EOS 给所有下游
                sendEOSDownstream();
                break;
            }
            // 基类统一分发，子类不感知下游数量
            pushToDownstream(buf);
        }
    }

    // 子类实现：阻塞采集，返回 nullptr 表示 EOF
    virtual Buffer* capture() = 0;

    // 需要重写，需要支持分叉
    SrcPad* requestSrcPad(const std::string& name, MediaType hint_type) override {
        if (!src_pads_.empty()) {
            const auto& existing = src_pads_[0]->templateCaps();
            if (!existing.contains(hint_type)) {
                return nullptr;
            }
            return addSrcPad(name, existing);   // 复制完整能力集合
        }
        // 首个 pad：从 hint_type 建立最初的能力集合
        return addSrcPad(name, TemplateCaps{{hint_type}});
    }
};
```

需要注意的是用户可能对同一路输出连接多个下游，比如采集到的画面可以一路直接本地预览，一路编码之后传输，甚至可以有别的路用来作别的格式的编码或者其他的处理，**所以 SourceNode 的 requestSrcPad 需要重写，需要支持分叉**，如果`Graph::link` 发现目标 SrcPad 不存在，会调用这里创建
在 SourceNode 下，新 SrcPad 是已有 SrcPad 的同源多路拷贝，所以能力集合必须和已有 pad 的保持一致，hint_type 只用于校验"这次 link 想承载的类型是否在已有能力集合内"，不参与 TemplateCaps 的构造


### 5.3 SinkNode

```cpp
class SinkNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::SINK; }

protected:
    explicit SinkNode(const std::string& name) : BaseNode(name) {}
    void runLoop() override {
        auto* sink_pad = sink_pads_[0].get();
        while (!stop_requested_.load()) {
            auto item = sink_pad->popBlocking();
            if (!item) break;   // flush 唤醒，检查 stop_requested_

            if (std::holds_alternative<BufferRef>(*item)) {
                consume(std::get<BufferRef>(*item).get());
            } else if (std::holds_alternative<Event>(*item)) {
                onEvent(std::get<Event>(*item));
            }
        }
    }

    virtual void consume(Buffer* buf) = 0;

    virtual void onEvent(const Event& event) {
        if (std::holds_alternative<EOSEvent>(event)) {
            postMessage(MessageType::EOS, "");
            return;
        }

        postMessage(MessageType::ERROR,
                    "CapsEvent received in runLoop; sink nodes must consume CapsEvent in onStreamInfo");
    }
};
```

### 5.4 TransformNode

TransformNode 基类不自动创建 SinkPad；具体 Transform 子类必须在构造函数中声明唯一 SinkPad（通常命名为 `"in"`），并给出自己的输入 TemplateCaps。`runLoop()` 以 `sink_pads_[0]` 作为驱动输入。

```cpp
class TransformNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::TRANSFORM; }

protected:
    explicit TransformNode(const std::string& name) : BaseNode(name) {}
    void runLoop() override {
        auto* sink_pad = sink_pads_[0].get();
        std::vector<Buffer*> outputs;   // 复用，避免每次循环构造新 vector
        while (!stop_requested_.load()) {
            auto item = sink_pad->popBlocking();
            if (!item) break;

            if (std::holds_alternative<BufferRef>(*item)) {
                outputs.clear();    // 清空复用，不释放已分配内存
                process(std::get<BufferRef>(*item).get(), outputs);
                for (auto* out : outputs) {
                    pushToDownstream(out);
                }
            } else if (std::holds_alternative<Event>(*item)) {
                onEvent(std::get<Event>(*item));
            }
        }
    }

    // 子类实现：一个输入，产出 0 到 N 个输出，结果写入 outputs
    // 调用前 outputs 已被 clear()，子类直接 push_back 即可
    // DecodeNode：一个 packet → 0 到 N 帧
    // EncodeNode：一帧 → 0 到 1 个 packet
    virtual void process(Buffer* input, std::vector<Buffer*>& outputs) = 0;

    virtual void onEvent(const Event& event) {
        if (std::holds_alternative<EOSEvent>(event)) {
            sendEOSDownstream();
            return;
        }

        postMessage(MessageType::ERROR,
                    "CapsEvent received in runLoop; transform nodes must consume CapsEvent in onStreamInfo");
    }

    // 支持分叉，需要重写函数
    SrcPad* requestSrcPad(const std::string& name, MediaType hint_type) override {
        if (!src_pads_.empty()) {
            const auto& existing = src_pads_[0]->templateCaps();
            if (!existing.contains(hint_type)) {
                return nullptr;
            }
            return addSrcPad(name, existing);   // 复制完整能力集合
        }
        return addSrcPad(name, TemplateCaps{{hint_type}});
    }
};
```

在 TransformNode 下，requestSrcPad 也要支持分叉，比如编码后用户完全可以一路推流，一路本地存储，语义与 SourceNode 的 requestSrcPad 是基本一致的，新 SrcPad 是已有 SrcPad 的同源多路拷贝，能力集合必须一致，`hint_type` 只是用于校验"这次 link 想承载的类型是否在已有能力集合内"

### 5.5 DemuxNode

#### 5.5.1 DemuxNode 抽象基类（当前正式骨架）

`DemuxNode` 位于 `include/pipeline/core/BaseNode.h`，只负责格式无关的公共流程；具体的 FFmpeg 输入上下文由后续 `AVDemuxNode` 实现。

输入地址在构造时确定并保持不可变：

```cpp
class DemuxNode : public BaseNode {
protected:
    DemuxNode(const std::string& name, std::string url)
        : BaseNode(name), url_(std::move(url)) {}

    virtual bool openInput(const std::string& url) = 0;
    virtual bool probeStreams(DemuxProbeResult* result) = 0;
    virtual DemuxReadResult readFrame() = 0;
    virtual void closeInput() = 0;

private:
    const std::string url_;
};

// 未来具体类的公开构造入口
AVDemuxNode(std::string name, std::string url)
    : DemuxNode(std::move(name), std::move(url)) {}
```

当前骨架的职责如下：

1. `requestSrcPad()` 只接受 `VIDEO_ENCODED` / `AUDIO_ENCODED`，Pad 在 `Graph::link()` 阶段创建；同类型多个 Pad 表示同一路流的分叉。
2. `onReady()` 调用 `openInput(url_)` 和 `probeStreams(&result)`；具体类显式返回一路最佳视频/音频 Caps，基类验证类型并校验用户连接的每种 Pad 都有对应流。
3. `onStreamInfo()` 按实际媒体类型调整输出 Route 容量并发送 CapsEvent。
4. `runLoop()` 调用 `readFrame()`，依据 Pad 的 `actualType()` 分发 Buffer；正常 EOF 时广播 EOS。
5. `onStop()` 是唯一资源释放入口。`onReady()` 失败路径不自行调用 `closeInput()`，由 `Graph::ready()` 回滚统一进入 `onStop()`。

当前阶段只要求每种媒体类型选择一路最佳流：一路最佳视频、一路最佳音频；同类型多 Track 的独立选择和路由暂不支持。

错误责任：`openInput` / `probeStreams` / `readFrame` 检测到的 FFmpeg 或输入后端错误由具体类先 `postMessage(ERROR)` 再返回失败；Pad、Caps、流缺失等框架层错误由抽象基类上报，基类不重复补发同一后端错误。

#### 5.5.2 AVDemuxNode（后续具体实现参考）

`AVDemuxNode` 尚未落地。下面保留一份精简参考，重点展示四个钩子怎样接入 FFmpeg；Pad 创建、Caps 发送、Buffer 分发、EOS 和 Ready 回滚仍由 `DemuxNode` 基类处理。

```cpp
struct DemuxProbeResult {
    std::optional<CapsEvent> video; // nullopt：探测成功，但没有视频流
    std::optional<CapsEvent> audio; // nullopt：探测成功，但没有音频流
};

enum class DemuxReadStatus {
    BUFFER,
    END_OF_STREAM,
    CANCELLED,
    ERROR,
};

struct DemuxReadResult {
    DemuxReadStatus status;
    BufferRef buffer;
};

class AVDemuxNode final : public DemuxNode {
public:
    AVDemuxNode(std::string name, std::string url)
        : DemuxNode(std::move(name), std::move(url)) {}

private:
    bool openInput(const std::string& url) override {
        int ret = avformat_open_input(&fmt_ctx_, url.c_str(), nullptr, nullptr);
        if (ret < 0) {
            postMessage(MessageType::ERROR, "avformat_open_input failed", ret);
            return false;
        }
        return true;
    }

    bool probeStreams(DemuxProbeResult* result) override {
        int ret = avformat_find_stream_info(fmt_ctx_, nullptr);
        if (ret < 0) {
            postMessage(MessageType::ERROR, "avformat_find_stream_info failed", ret);
            return false;
        }

        video_stream_idx_ = av_find_best_stream(
            fmt_ctx_, AVMEDIA_TYPE_VIDEO, -1, -1, nullptr, 0);
        audio_stream_idx_ = av_find_best_stream(
            fmt_ctx_, AVMEDIA_TYPE_AUDIO, -1, -1, nullptr, 0);

        if (video_stream_idx_ >= 0) {
            AVStream* st = fmt_ctx_->streams[video_stream_idx_];
            CapsEvent caps;
            caps.media_type = MediaType::VIDEO_ENCODED;
            caps.codec_id   = st->codecpar->codec_id;
            caps.width      = st->codecpar->width;
            caps.height     = st->codecpar->height;
            caps.framerate  = av_guess_frame_rate(fmt_ctx_, st, nullptr);
            copyExtradata(st->codecpar, caps.extradata);
            result->video = std::move(caps);
        }
        if (audio_stream_idx_ >= 0) {
            AVStream* st = fmt_ctx_->streams[audio_stream_idx_];
            CapsEvent caps;
            caps.media_type = MediaType::AUDIO_ENCODED;
            caps.codec_id   = st->codecpar->codec_id;
            caps.sample_rate = st->codecpar->sample_rate;
            caps.channels   = st->codecpar->ch_layout.nb_channels;
            copyExtradata(st->codecpar, caps.extradata);
            result->audio = std::move(caps);
        }
        return true; // 用户请求的流是否存在，由基类 onReady() 校验
    }

    DemuxReadResult readFrame() override {
        AVPacket* pkt = av_packet_alloc();
        if (!pkt) {
            postMessage(MessageType::ERROR, "av_packet_alloc failed");
            return {DemuxReadStatus::ERROR, {}};
        }

        while (true) {
            int ret = av_read_frame(fmt_ctx_, pkt);
            if (ret == AVERROR_EOF) {
                av_packet_free(&pkt);
                return {DemuxReadStatus::END_OF_STREAM, {}};
            }
            if (ret < 0) {
                av_packet_free(&pkt);
                if (interrupt_requested_.load()) {
                    return {DemuxReadStatus::CANCELLED, {}};
                }
                postMessage(MessageType::ERROR, "av_read_frame failed", ret);
                return {DemuxReadStatus::ERROR, {}};
            }

            MediaType type;
            if (pkt->stream_index == video_stream_idx_) {
                type = MediaType::VIDEO_ENCODED;
            } else if (pkt->stream_index == audio_stream_idx_) {
                type = MediaType::AUDIO_ENCODED;
            } else {
                av_packet_unref(pkt); // 无关流在具体类内部跳过，不返回 SKIP
                continue;
            }

            AVStream* st = fmt_ctx_->streams[pkt->stream_index];
            Buffer* buffer = Buffer::fromAVPacket(
                pkt, type, st->time_base, st->codecpar->codec_id);
            av_packet_free(&pkt);
            if (!buffer) {
                postMessage(MessageType::ERROR, "Buffer::fromAVPacket failed");
                return {DemuxReadStatus::ERROR, {}};
            }
            return {DemuxReadStatus::BUFFER, BufferRef{buffer}};
        }
    }

    void closeInput() override {
        if (fmt_ctx_) avformat_close_input(&fmt_ctx_);
    }

    AVFormatContext* fmt_ctx_ = nullptr;
    int video_stream_idx_ = -1;
    int audio_stream_idx_ = -1;
};
```

示例中的 `copyExtradata()` 代表“判空后把 `codecpar->extradata` 深拷贝到 vector”的小工具，错误文本实际实现时应附带 `av_strerror()`。`probeStreams()` 通过 `DemuxProbeResult` 显式返回探测结果；`readFrame()` 通过 `DemuxReadResult` 明确区分 Buffer、自然 EOF、主动取消和错误。无关 subtitle/data/未选 Track 由具体类内部循环跳过即可。示例中的 `interrupt_requested_` 是后续 FFmpeg interrupt callback 使用的取消标志，本轮只先固定 `CANCELLED` 的接口语义。

### 5.6 MuxNode

#### 5.6.1 MuxFormat 与流式输出边界

```cpp
enum class MuxFormat {
    MPEGTS,
    FLV,
    MP4,   // 在本项目中固定表示 fragmented MP4
};
```

通用 `MuxNode` 只面向可以顺序输出的容器字节流：MPEG-TS、FLV 和 fragmented MP4。**项目文档和接口中的“MP4”均指 fMP4**；需要 seek 回写文件头的传统 MP4 不属于该抽象，未来如有需要使用专门节点处理。

格式由未来具体类的公开构造函数接收，再转发给基类保存：

```cpp
AVMuxNode(std::string name, MuxFormat format)
    : MuxNode(std::move(name), format) {}
```

`format_` 是基类中的 `const MuxFormat`，不存在 `setFormat()`，也不向框架使用者暴露 FFmpeg muxer 字符串。枚举到 FFmpeg muxer 名和 fMP4 movflags 的映射属于后续 `AVMuxNode`。

#### 5.6.2 MuxNode 抽象基类（当前正式骨架）

`MuxNode` 位于 `include/pipeline/core/BaseNode.h`。当前阶段明确限制为**单输出**：

- 构造时固定创建一个 `{CONTAINER}` SrcPad，名字为 `out_0`；
- 不实现动态 `requestSrcPad()`，不支持 Mux 容器字节流分叉；
- 输入 Pad 仍通过 `requestSinkPad()` 动态创建，只接受编码视频或编码音频；
- 多输出与不同下游消费速率的背压策略以后统一设计。

```cpp
class MuxNode : public BaseNode {
protected:
    MuxNode(const std::string& name, MuxFormat format)
        : BaseNode(name), format_(format) {
        addSrcPad("out_0", TemplateCaps{{MediaType::CONTAINER}});
    }

    virtual bool allocateContext(MuxFormat format) = 0;
    virtual bool addStream(const CapsEvent& caps, int* stream_index) = 0;
    virtual bool writeHeader() = 0;
    virtual bool writePacket(Buffer* buf, int stream_index) = 0;
    virtual bool writeTrailer() = 0;
    virtual void closeContext() = 0;

    // 具体 Mux 后端的输出入口：复制字节到 pending，不在 callback 中 publish Route。
    bool appendContainerBytes(const uint8_t* data, size_t size);

private:
    void flushPendingOutput();

    const MuxFormat format_;
    std::vector<uint8_t> pending_output_;
};
```

`onStreamInfo()` 的固定流程：

1. `allocateContext(format_)`；
2. 要求至少存在一个 SinkPad，并校验每个 SinkPad 都已连接；
3. 逐个 `receiveCapsEvent()` 并 `addStream()`，建立 Pad 到输出 stream 的映射；
4. resize `out_0` 所属 Route 到 CONTAINER 容量；
5. 向 `out_0` 发送 `MediaType::CONTAINER` CapsEvent；
6. 调用 `writeHeader()`。

Ready 任一步失败都只返回 `false`，资源由 `Graph::ready()` 回滚后统一通过 `onStop()` / `closeContext()` 释放，不在失败分支重复 close。

#### 5.6.3 pending 输出与 Header 死锁规避

FFmpeg 自定义 AVIO callback 得到的是临时容器字节，未来 `AVMuxNode` 的 callback 只能调用：

```cpp
appendContainerBytes(data, size);
```

该 helper 立即复制字节到 `pending_output_`，但不在 Ready 阶段继续生成任意数量的容器 Buffer。Header 仍暂存到 runLoop，避免 Ready 阶段在有界 Route 达到容量后等待尚未启动的下游线程。

发送顺序为：

```text
Ready:   CONTAINER Caps → writeHeader() → 字节暂存 pending
Running: flush Header bytes
         → 每次 writePacket() 成功后 flush 本次字节
         → 所有有效输入 EOS
         → writeTrailer() → flush trailer bytes → 输出 EOS
```

`flushPendingOutput()` 把 pending 字节复制为一个 `MediaType::CONTAINER` Buffer，并向 CONTAINER OutputRoute 可靠 publish 一次；未来多个输出订阅者共享该 Route，不会静默丢弃容器字节。当前 FileSink / RTSPPush 只按顺序消费字节，不读取 CONTAINER Buffer 的 meta；`ContainerMeta` 延后设计。

#### 5.6.4 EOS 与错误责任

Graph::link() 会回滚失败连接中新创建的动态 Pad，不留下未连接残留；Mux Ready 还会拒绝零输入或任何未连接的 SinkPad。因此只要 Mux 进入 Running，`sink_pads_` 就是本轮完整的输入集合。完成条件为：

```cpp
eos_pads_.size() == sink_pads_.size()
```

所有输入收到 EOS 后，必须先成功写 Trailer、发送 Trailer 字节，再向 `out_0` 发送 EOS。

错误责任与其他节点保持一致：

- FFmpeg/容器后端错误由具体 `AVMuxNode` 上报后返回失败；
- Pad、Caps、无有效输入、缺少输出连接等框架错误由 `MuxNode` 上报；
- 抽象基类看到格式钩子失败时只退出或触发 Ready 回滚，不重复发送泛化错误。

#### 5.6.5 AVMuxNode（后续具体实现参考）

`AVMuxNode` 尚未落地。下面示例只展示 FFmpeg 后端怎样实现基类钩子；输入 Caps 收集、固定 `out_0`、pending flush、Trailer 后 EOS 和回滚仍由 `MuxNode` 处理。首个具体实现可以只完成 MPEG-TS，再逐步加入 FLV 和 fMP4。

```cpp
class AVMuxNode final : public MuxNode {
public:
    AVMuxNode(std::string name, MuxFormat format)
        : MuxNode(std::move(name), format) {}

private:
    static int writeCallback(void* opaque, const uint8_t* data, int size) {
        auto* self = static_cast<AVMuxNode*>(opaque);
        return self->appendContainerBytes(data, static_cast<size_t>(size))
             ? size : AVERROR_EXTERNAL;
    }

    bool allocateContext(MuxFormat format) override {
        const char* muxer_name = nullptr;
        switch (format) {
            case MuxFormat::MPEGTS: muxer_name = "mpegts"; break;
            case MuxFormat::FLV:    muxer_name = "flv";    break;
            case MuxFormat::MP4:    muxer_name = "mp4";    break;
        }

        int ret = avformat_alloc_output_context2(
            &fmt_ctx_, nullptr, muxer_name, nullptr);
        if (ret < 0 || !fmt_ctx_) {
            postMessage(MessageType::ERROR,
                        "avformat_alloc_output_context2 failed", ret);
            return false;
        }

        uint8_t* avio_buffer = static_cast<uint8_t*>(av_malloc(4096));
        if (!avio_buffer) {
            postMessage(MessageType::ERROR, "av_malloc AVIO buffer failed");
            return false;
        }
        avio_ctx_ = avio_alloc_context(
            avio_buffer, 4096, 1, this,
            nullptr, &AVMuxNode::writeCallback, nullptr);
        if (!avio_ctx_) {
            av_free(avio_buffer);
            postMessage(MessageType::ERROR, "avio_alloc_context failed");
            return false;
        }
        fmt_ctx_->pb = avio_ctx_;
        return true;
    }

    bool addStream(const CapsEvent& caps, int* stream_index) override {
        AVStream* st = avformat_new_stream(fmt_ctx_, nullptr);
        if (!st) {
            postMessage(MessageType::ERROR, "avformat_new_stream failed");
            return false;
        }

        st->codecpar->codec_type =
            caps.media_type == MediaType::VIDEO_ENCODED
                ? AVMEDIA_TYPE_VIDEO : AVMEDIA_TYPE_AUDIO;
        st->codecpar->codec_id    = caps.codec_id;
        st->codecpar->width       = caps.width;
        st->codecpar->height      = caps.height;
        st->codecpar->sample_rate = caps.sample_rate;
        copyExtradata(caps.extradata, st->codecpar);
        *stream_index = st->index;
        return true;
    }

    bool writeHeader() override {
        AVDictionary* options = nullptr;
        if (formatIsMP4()) {
            // MuxFormat::MP4 在本项目中只能输出 fragmented MP4。
            av_dict_set(&options, "movflags",
                        "frag_keyframe+empty_moov+default_base_moof", 0);
        }
        int ret = avformat_write_header(fmt_ctx_, &options);
        av_dict_free(&options);
        if (ret < 0) {
            postMessage(MessageType::ERROR, "avformat_write_header failed", ret);
            return false;
        }
        return true; // callback 产生的 Header 字节已进入基类 pending_output_
    }

    bool writePacket(Buffer* buf, int stream_index) override {
        AVPacket* pkt = toAVPacket(buf); // 深拷贝 payload，时间戳仍为微秒
        if (!pkt) {
            postMessage(MessageType::ERROR, "toAVPacket failed");
            return false;
        }
        pkt->stream_index = stream_index;
        AVStream* st = fmt_ctx_->streams[stream_index];
        pkt->pts = rescaleUs(buf->pts, st->time_base);
        pkt->dts = rescaleUs(buf->dts, st->time_base);
        pkt->duration = rescaleUs(buf->duration, st->time_base);

        int ret = av_interleaved_write_frame(fmt_ctx_, pkt);
        av_packet_free(&pkt);
        if (ret < 0) {
            postMessage(MessageType::ERROR,
                        "av_interleaved_write_frame failed", ret);
            return false;
        }
        return true; // callback 产生的容器字节由基类随后 flush
    }

    bool writeTrailer() override {
        int ret = av_write_trailer(fmt_ctx_);
        if (ret < 0) {
            postMessage(MessageType::ERROR, "av_write_trailer failed", ret);
            return false;
        }
        return true;
    }

    void closeContext() override {
        if (avio_ctx_) {
            av_freep(&avio_ctx_->buffer);
            avio_context_free(&avio_ctx_);
        }
        if (fmt_ctx_) avformat_free_context(fmt_ctx_);
        fmt_ctx_ = nullptr;
    }

    AVFormatContext* fmt_ctx_ = nullptr;
    AVIOContext* avio_ctx_ = nullptr;
};
```

示例中的 `copyExtradata()`、`toAVPacket()`、`rescaleUs()` 和 `formatIsMP4()` 是待具体实现的小工具，用于突出主流程而省略边界检查。关键约束是：AVIO callback 只调用 `appendContainerBytes()`，不得自行创建框架 Buffer、publish Route 或发送 EOS；`MuxFormat::MP4` 必须配置成 fragmented MP4，传统 MP4 不通过通用 `MuxNode → CONTAINER → Sink` 链路实现。

---

## 6. Graph（有向图）

Graph 显式维护静态 DAG：`nodes_` 持有全部 Node，`edges_` 持有全部连接边，`topo_order_` 保存 build 后的拓扑序。Route 只替换 Edge 的数据面；Edge 的 `src_node/dst_node` 仍是图结构、拓扑排序、孤立节点检测和逆拓扑资源释放的依据。

### 6.1 数据结构

```cpp
class Graph {
public:
    void addNode(std::unique_ptr<BaseNode> node);
    bool link(BaseNode* src, const std::string& src_pad_name,
              BaseNode* dst, const std::string& dst_pad_name,
              MediaType hint_type);
    bool build();
    bool ready();
    const std::vector<BaseNode*>& topoOrder() const;
    void cancelAllRoutes();

private:
    std::vector<std::unique_ptr<BaseNode>> nodes_;  // DAG 顶点所有权
    std::vector<std::unique_ptr<Edge>> edges_;      // DAG 边 + RouteSubscription
    std::vector<BaseNode*> topo_order_;             // Kahn 拓扑排序结果
};
```

### 6.2 link 流程

`link(src, src_pad_name, dst, dst_pad_name, hint_type)` 在 NULL_STATE 阶段建立一条静态图边和一个静态订阅关系：

1. **查找或创建 SrcPad**：优先按名字查找；已存在但已连接则失败。不存在时调用 `src->requestSrcPad()`：
   - Source / Transform 的同源分叉 Pad 复制已有完整 TemplateCaps，并共享已有 OutputRoute；
   - Demux 只接受 VIDEO_ENCODED / AUDIO_ENCODED；同类型 Pad 共享同一最佳 Track 的 Route，不同类型创建不同 Route；
   - 节点拒绝请求时 link 失败。
2. **查找或创建 SinkPad**：固定 Pad 直接命中；不存在时调用 `dst->requestSinkPad()`。已连接或节点拒绝时失败。
3. **静态能力校验**：两端 `TemplateCaps` 必须有 MediaType 交集；`hint_type` 仅用于 requestPad 的创建/能力校验，绝不设置 `actualType`。
4. **创建 RouteSubscription 与 Edge**：在 SrcPad 所属 OutputRoute 上创建一个 Subscription；创建 Edge，写入 `src_node/src_pad_name/dst_node/dst_pad_name` 与 Subscription，然后把 Edge 连接到两个 Pad。Edge 仍是一对一图边。
5. **事务性回滚**：若第 2–4 步失败，仅释放本次 request 新建且尚未连接的 Pad；已有固定 Pad 不删除。Demux 回滚 SrcPad 时同步清理 `pad_to_type_`。Subscription 只在成功 Edge 中保留，build 前可随失败连接一并释放。

因此，用户仍通过申请多个 SrcPad 表达分叉；Graph 仍维护多条独立 Edge；只是这些同源 Edge 的数据面订阅同一个 OutputRoute。

### 6.3 build 流程

1. Kahn 拓扑排序与环路检测。
2. 孤立节点检测。
3. seal 所有已连接 OutputRoute。seal 后订阅集合永久不可修改；Pipeline 的 `addNode()`/`link()` 也只允许在 NULL_STATE 调用。

### 6.4 Ready 流程

按拓扑顺序穿插执行：

1. `node->onReady()`；
2. `node->onStreamInfo()`：上游根据实际媒体类型 resize 逻辑 Route 并 publish 一次 CapsEvent；下游 acquire、校验并 ack CapsEvent。

Route 在节点构造时以临时容量 8 建立，并在 onStreamInfo 确定实际类型后 resize。若 Ready 任一步失败，Graph 先 cancel 全部 Route，唤醒可能的 publish/acquire，再按拓扑逆序调用 touched 节点的 `onStop()`。

### 6.5 stop/cancel

Pipeline stop 先设置所有节点的 `stop_requested_`，随后 `Graph::cancelAllRoutes()`。cancel 清空未完成日志并唤醒全部 publisher、subscriber 及 Mux 外部等待，节点线程再被 join。cancel 是强制停止语义，不代替正常 EOS。

---

## 7. Pipeline

### 7.1 Pipeline 职责

- 持有 Graph、Clock、MessageBus
- 驱动三阶段启动：Build → Ready → Running
- 统一创建和管理所有节点线程
- 内部运行 MessageBus 监听线程，统一处理所有消息类型

### 7.2 Pipeline 状态机

```
NULL_STATE ──(build)──→ BUILT ──(play)──→ RUNNING ──(stop)──→ STOPPING ──→ STOPPED
                                                                                │
                                                                       (destroy)↓
                                                                            NULL_STATE
```

| 状态 | 含义 |
|------|------|
| NULL_STATE | 未构建 |
| BUILT | Graph 构建完成，静态检查通过 |
| RUNNING | 所有线程运行中 |
| STOPPING | `stop()` 正在执行中（join 线程等耗时操作进行中） |
| STOPPED | 已停止，所有线程已退出 |
| ERROR | 某节点报错 |

`state_` 是 `std::atomic<PipelineState>`。`STOPPING` 这个中间态专门用于保护 `stop()`——
`waitEOS()` 自然结束时内部会触发 `stop()`，用户也可能同时主动调用 `stop()`，这两条路径
在我们的设计语义下天然可能并发发生（不是因为要支持用户随意多线程调用所有接口，而是
"等待自然结束"和"主动打断"这两件事本身就可能同时触发）。`stop()` 内部用 CAS（
`compare_exchange_strong`）从 `RUNNING` 切到 `STOPPING`，只有切换成功的那一个调用者才会
真正执行 join 线程等危险操作，另一个会直接安全返回，不会出现重复 join 导致崩溃的情况。

`build()`/`play()` 不需要这种保护——它们是用户在搭建/启动阶段单线程顺序调用的，场景上
不存在被并发触发的可能，不需要为它们引入中间态或 CAS。整个工程的使用前提是：
`addNode`/`link`/`build`/`play` 在用户的单线程脚本里顺序执行；只有 `stop()` 因为
"自然结束"与"主动打断"两条路径的天然并发性，才需要专门保护。

### 7.3 用户输入到 stop() 的链路

框架不内置任何输入监听。用户在自己的 main 函数里自由决定触发方式（SDL 事件、getchar、信号处理、网络命令等），然后调用 `pipeline.stop()`。

用户侧暴露两个接口：

```cpp
if (!pipeline.play()) {
    fprintf(stderr, "pipeline play failed: %s\n", pipeline.lastError().c_str());
    return -1;
}
pipeline.waitEOS();   // 阻塞等待自然结束或出错，内部自动调 stop()

// 或者
if (!pipeline.play()) {
    fprintf(stderr, "pipeline play failed: %s\n", pipeline.lastError().c_str());
    return -1;
}
// ... 用户自己的事件循环 ...
pipeline.stop();      // 用户主动停止
```

**waitEOS() 流程**：
1. 阻塞在 `eos_cv_` 上
2. 条件：`active_sink_count_ == 0`（所有 Sink 正常 EOS）、`error_occurred_`（有节点报错），
   或者 `state_` 已经不是 `RUNNING`（说明已经被外部 `stop()` 打断）
3. 唤醒后调用 `stop()`（已经停止过的情况下，`stop()` 内部 CAS 直接返回，不会重复执行）

**stop() 流程**：
1. CAS：仅当 `state_ == RUNNING` 时才能切换到 `STOPPING`，否则直接返回（已经停止/正在停止）
2. 设置所有节点 `stop_requested_.store(true)`
3. cancel 所有 OutputRoute，唤醒阻塞中的 publish/acquire 和 Mux 外部等待
4. join 所有节点线程
5. 停止 MessageBus 监听线程（`bus_running_ = false`，notify，join）
6. 按拓扑逆序调用 `onStop()` 释放资源
7. `state_ = STOPPED`，notify `eos_cv_`

这两种用法也可以同时使用——比如主线程调用 `waitEOS()` 阻塞等待，另一条监听路径（信号处理、另一个线程的事件循环等）检测到退出意图后调用 `stop()`。这两条路径谁先触发不确定，**`stop()`内部的 CAS 保护保证不管谁先到，只有一次真正的清理会被执行，另一次安全地什么都不做**

**节点侧责任分工**：
- `runLoop` 退出前：数据层面收尾（flush 编解码器缓冲区，把未处理完的数据推出去）
- `onStop()`：资源层面释放（`avcodec_free_context`、`SDL_DestroyRenderer`、关闭文件句柄等）

### 7.4 Pipeline 实现骨架

```cpp
class Pipeline {
public:
    Pipeline() = default;
    ~Pipeline() { stop(); }   // stop() 内部 CAS 自行处理所有状态，这里不需要额外判断

    template<typename T, typename... Args>
    T* addNode(const std::string& name, Args&&... args);

    bool link(BaseNode* src, const std::string& src_pad,
             BaseNode* dst, const std::string& dst_pad,
             MediaType hint_type = MediaType::CONTAINER);

    bool build();
    bool play();
    void waitEOS();
    void stop();

    // 事后查询最近一次 ERROR 消息的文本内容
    std::string lastError() {
        std::lock_guard lock(error_mutex_);
        return last_error_;   // 返回拷贝，不持锁过久
    }

    Clock* clock() { return &clock_; }
    MessageBus* bus() { return &bus_; }

private:
    void messageBusLoop();   // MessageBus 监听线程主循环

    Graph                                           graph_;
    Clock                                           clock_;
    MessageBus                                      bus_;
    std::unordered_map<BaseNode*, std::thread>      threads_;
    std::thread                                     bus_thread_;        // MessageBus 监听线程
    std::atomic<bool>                               bus_running_{false};

    // EOS / Error 状态
    std::atomic<int>                                active_sink_count_{0};
    std::atomic<bool>                               error_occurred_{false};
    std::mutex                                      error_mutex_;       // 保护 last_error_
    std::string                                     last_error_;
    std::mutex                                      eos_mutex_;
    std::condition_variable                         eos_cv_;

    // atomic：build()/play() 不需要 CAS 保护（场景上不会被并发调用），
    // 但 stop() 需要用 CAS 判断+切换状态，所以 state_ 本身必须是 atomic
    std::atomic<PipelineState>                      state_{PipelineState::NULL_STATE};
};
```

### 7.5 build()

```cpp
bool Pipeline::build() {
    // 简单 if 判断即可，不需要 CAS：build() 是用户在搭建阶段单线程顺序调用的，
    // 场景上不存在被并发触发的可能
    if (state_ != PipelineState::NULL_STATE) return false;

    if (!graph_.build()) {
        state_ = PipelineState::ERROR;
        return false;
    }
    state_ = PipelineState::BUILT;
    return true;
}
```

### 7.6 play()

```cpp
bool Pipeline::play() {
    // 同样只需要简单 if 判断，理由同 build()
    if (state_ != PipelineState::BUILT) return false;

    // 重置墙钟基准（无音频回退模式下的时钟以 play 时刻为零点）
    clock_.reset();

    // 先启动 MessageBus 监听线程：Ready 阶段节点 postMessage(ERROR/WARNING/INFO)
    // 需要有人接收，否则 lastError() 在 Ready 失败路径上永远为空。
    // 顺序必须早于 graph_.ready()。
    bus_running_ = true;
    bus_thread_ = std::thread([this]() { messageBusLoop(); });

    if (!graph_.ready()) {
        // Ready 失败：先把 bus 收干净，保证 Ready 期间的 ERROR 消息全部落入 last_error_，
        // 然后再置 ERROR 返回。bus_running_ 翻为 false 后 notify()，
        // waitMessage 的“队列非空优先返回消息”语义保证 pending 消息不会丢。
        bus_running_ = false;
        bus_.notify();
        if (bus_thread_.joinable()) bus_thread_.join();
        state_ = PipelineState::ERROR;
        return false;
    }

    // 统计 SinkNode 数量，用于 EOS 计数
    for (auto* node : graph_.topoOrder()) {
        if (node->nodeType() == NodeType::SINK)
            active_sink_count_++;
    }

    // 按拓扑逆序启动节点线程（Sink 先，Source 后）
    auto& order = graph_.topoOrder();
    for (auto it = order.rbegin(); it != order.rend(); ++it) {
        threads_[*it] = std::thread([node = *it]() { node->runLoop(); });
    }
    state_ = PipelineState::RUNNING;
    return true;
}
```

**bus 提前启动的语义扩张**：Ready 阶段的 WARNING / INFO 也会走用户注册的 observer 回调，与 Running 阶段完全同构，用户不必区分两段。Ready 阶段发送 EOS 属于节点作者违约（Sink 线程尚未启动），当前不做框架防御。

### 7.7 stop()

```cpp
void Pipeline::stop() {
    // CAS：只有当前确实是 RUNNING 时才能切换到 STOPPING，且这个判断+切换是原子的。
    // "自然结束触发的 stop()" 和 "用户主动调用的 stop()" 可能几乎同时发生，
    // 这里保证只有一个调用者会真正执行下面的清理，另一个直接返回。
    PipelineState expected = PipelineState::RUNNING;
    if (!state_.compare_exchange_strong(expected, PipelineState::STOPPING)) {
        return;   // 已经停止/正在停止/从未运行，直接返回
    }

    // 1. 设置所有节点退出标志
    for (auto& [node, _] : threads_)
        node->stop_requested_.store(true);

    // 2. cancel 所有 OutputRoute，唤醒阻塞中的 publish/acquire
    graph_.cancelAllRoutes();

    // 3. join 所有节点线程（join 遍历顺序不承诺；线程退出已由 stop_requested_ + queue flush 触发）
    for (auto& [node, thread] : threads_)
        if (thread.joinable()) thread.join();

    // 4. 停止 MessageBus 监听线程
    bus_running_ = false;
    bus_.notify();
    if (bus_thread_.joinable()) bus_thread_.join();

    // 5. 按拓扑逆序调用 onStop()（资源层面释放）
    auto& order = graph_.topoOrder();
    for (auto it = order.rbegin(); it != order.rend(); ++it)
        (*it)->onStop();

    state_ = PipelineState::STOPPED;
    eos_cv_.notify_all();   // 唤醒可能正阻塞在 waitEOS() 的线程
}
```

### 7.8 waitEOS()

```cpp
void Pipeline::waitEOS() {
    std::unique_lock lock(eos_mutex_);
    eos_cv_.wait(lock, [this] {
        return active_sink_count_.load() == 0      // 所有 Sink 正常 EOS
            || error_occurred_.load()              // 有节点报错
            || state_.load() != PipelineState::RUNNING;  // 已被外部 stop() 打断
    });
    lock.unlock();
    stop();   // 已经停止过的情况下，内部 CAS 直接返回，不会重复执行
}
```

### 7.9 messageBusLoop()

Pipeline 内部的 MessageBus 监听线程，统一处理所有消息类型：

```cpp
void Pipeline::messageBusLoop() {
    while (true) {
        auto msg = bus_.waitMessage(bus_running_);
        if (!msg) break;

        switch (msg->type) {
            case MessageType::EOS:
                if (--active_sink_count_ == 0)
                    eos_cv_.notify_all();
                break;

            case MessageType::ERROR:
                error_occurred_ = true;
                {
                    std::lock_guard lock(error_mutex_);
                    last_error_ = msg->text;
                }
                eos_cv_.notify_all();
                break;

            case MessageType::WARNING:
            case MessageType::INFO:
                bus_.notifyObserver(*msg);
                break;
        }
    }
}
```

### 7.10 线程启动与停止顺序

**启动顺序**（play）：
1. **MessageBus 监听线程**（最先启动，早于 `graph_.ready()`，Ready 阶段节点 `postMessage(ERROR/WARNING/INFO)` 才有人接收）
2. `graph_.ready()` 若失败：`bus_running_=false → notify → join` drain 所有 Ready 期消息后置 ERROR 返回
3. SinkNode 线程
4. TransformNode 线程
5. SourceNode 线程（最后启动）

**停止顺序**（stop）：
1. 所有节点 `stop_requested_.store(true)`
2. cancel 所有 OutputRoute
3. join 所有节点线程（join 遍历顺序不承诺并且无所谓，因为线程退出已由 `stop_requested_` + queue flush 触发）
4. 停止 MessageBus 监听线程
5. 按拓扑逆序调用 onStop()

MessageBus 监听线程最后停止，保证节点退出过程中 postMessage 的消息（如 EOS、WARNING）仍能被处理。

**为什么 bus 必须早于 `graph_.ready()`**：`onReady()` / `onStreamInfo()` 可能执行真实设备/文件初始化（DemuxNode 打开 URL、DecodeNode 调 `avcodec_open2` 等），失败时会 `postMessage(ERROR, "...")`。bus 若尚未启动，消息只落到 queue 无人消费，`last_error_` 永远为空，§7.3 承诺的 `pipeline.lastError()` 就不可用。

---

## 8. Caps 两阶段协商

### 8.1 第一阶段：静态协商（Build 时）

每个 Pad 声明 TemplateCaps（支持的 MediaType 集合）。`link()` 时检查两端 TemplateCaps 是否有交集，无交集则 `link()` 返回 false。

各节点 TemplateCaps 声明示例：

```
DecodeNode
  SinkPad：{ VIDEO_ENCODED, AUDIO_ENCODED }   // 接受编码后的音频或视频
  SrcPad： { VIDEO_RAW, AUDIO_RAW }           // 产出解码后的原始数据

EncodeNode
  SinkPad：{ VIDEO_RAW, AUDIO_RAW }           // 接受原始音频或视频
  SrcPad： { VIDEO_ENCODED, AUDIO_ENCODED }   // 产出编码后的数据

MuxNode
  SinkPad：{ VIDEO_ENCODED, AUDIO_ENCODED }   // 接受编码后的音频或视频
  SrcPad： { CONTAINER }                      // 产出容器封装流

DemuxNode
  SrcPad： { VIDEO_ENCODED, AUDIO_ENCODED }   // 产出编码后的音频或视频（具体类型由文件决定）

VideoRenderNode
  SinkPad：{ VIDEO_RAW }                      // 只接受原始视频帧

AudioPlayNode
  SinkPad：{ AUDIO_RAW }                      // 只接受原始音频帧

FileSinkNode / RTSPPushNode
  SinkPad：{ CONTAINER }                      // 只接受容器封装流
```

静态协商只做粗粒度的 MediaType 交集检查，无法在 Build 阶段区分"这个 DecodeNode 是视频解码器还是音频解码器"，具体类型由 Ready 阶段收到的 CapsEvent 决定。这是设计上的合理取舍。

```
错误示例：
  EncodeNode 的 SrcPad 声明 { VIDEO_ENCODED, AUDIO_ENCODED }
  VideoRenderNode 的 SinkPad 声明 { VIDEO_RAW }
  → 无交集 → Build 报错：
    "EncodeNode.out_0 (VIDEO_ENCODED|AUDIO_ENCODED) is incompatible with
     VideoRenderNode.in (VIDEO_RAW)"
```

**Pad 的两层类型信息**：

| 层次 | 存储 | 生效时机 | 用途 |
|-----|-----|--------|-----|
| 能力集合 | `Pad::template_caps_`（TemplateCaps） | 构造 / requestPad 时确立 | Build 阶段 `Graph::link` 的粗粒度兼容性检查、Ready 阶段 send/receiveCapsEvent 的精确校验 |
| 实际类型 | `Pad::actual_type_`（optional<MediaType>） | Ready 阶段 CapsEvent 流经 pad 时 | Ready 阶段完成后 runLoop 里的类型分发判断（如 DemuxNode 按 media_type 分发） |

分层规则：
1. `actual_type` 的**唯一设值时机 = Ready 阶段 send/receiveCapsEvent 中 CapsEvent 流经时**（`setActualType` 是 Pad 的 private 方法 + friend BaseNode 授权）
2. Ready 之前 `pad->actualType()` 一律返回 `nullopt`，是契约的一部分，任何代码不得依赖"Ready 前 actualType 有值"
3. Ready 之后所有**已连接**的 pad 的 `actualType()` 必有值；如果 Ready 走完某个 pad 依然是 nullopt，说明该 pad 未连接或该 pad 从未收/发过 CapsEvent，视为节点作者违约
4. runLoop 阶段的类型判断**只查 `actualType()`**，绝对不能从 TemplateCaps 里取某个元素当实际类型用
5. **requestPad 里不调 `setActualType`**

### 8.2 第二阶段：动态协商（Ready 时）

Ready 阶段的 CapsEvent 传递按拓扑顺序逐节点完成。每个节点只在自己的 `onStreamInfo()` 中消费上游 CapsEvent、完成本节点初始化，并在需要时向下游发送新的 CapsEvent，顺流传递：

比如 DemuxNode：
1. 在 onReady() 中 avformat_open_input，获取真实参数，随后将 CapsEvent 内容记录到成员变量，暂不发送
2. build 已为每条 Edge 建立静态 Subscription 并 seal Route
3. 在 onStreamInfo() 中按逻辑流 resize VIDEO/AUDIO OutputRoute，并且每条不同 Route 只处理一次
4. sendCapsEvent("video_0", {...}) 定位 VIDEO Route 并只 publish 一次 CapsEvent；该 Route 的所有静态订阅者分别 acquire/ack

又比如 DecodeNode：
1. onReady() 无特殊操作，就是等待 CapsEvent
2. build 已建立输入 Subscription 和逻辑输出 Route
3. 在 onStreamInfo() 中，首先从 SinkPad Subscription acquire 并 ack CapsEvent，然后用 codec_id 查找解码器
4. 然后分配上下文：avcodec_alloc_context3(codec)，并填充 extradata（SPS/PPS）到 ctx_->extradata
5. 调用 avcodec_open2(ctx_, codec, nullptr)
6. **必须等 avcodec_open2() 完成后，才能从 ctx_ 中读取输出参数，因为输出的 pix_fmt、width、height、sample_fmt 由解码器决定，不能直接透传输入 CapsEvent 的值**
7. 从 ctx_ 读取实际输出参数，构造输出 CapsEvent
    + 视频：{ VIDEO_RAW, pix_fmt=ctx_->pix_fmt, width=ctx_->width, height=ctx_->height }
    + 音频：{ AUDIO_RAW, sample_fmt=ctx_->sample_fmt, sample_rate=ctx_->sample_rate, channels=ctx_->ch_layout.nb_channels }
8. resize 自己的逻辑输出 Route 到 selectRouteCapacity(media_type)
9. 最后 sendCapsEvent("out_0", out_caps) 向逻辑输出 Route publish 一次 CapsEvent

再比如 VideoRenderNode，直接就是：
1. 在 onStreamInfo() 中，从 SinkPad Subscription acquire 并 ack CapsEvent
2. 用 width / height / pix_fmt 初始化 SDL 窗口和纹理
3. 它没有下游，没有 SrcPad，不需要构造输出 CapsEvent

**BaseNode 的两个 CapsEvent helper**：
- `bool sendCapsEvent(src_pad_name, caps)`：定位 SrcPad 所属 Route，校验共享该 Route 的所有 Pad TemplateCaps，设置 actualType 后可靠 publish 一次。
- `bool receiveCapsEvent(sink_pad_name)`：**popBlocking → 校验取到的是 Event 且是 CapsEvent → 校验 caps.media_type ∈ SinkPad.templateCaps → setActualType → 存入 negotiated_caps_**。任一步失败 postMessage(ERROR) 并返回 false。

两个 helper 对称、返回 bool。**上游节点的 onStreamInfo 里凡是需要发/收 CapsEvent 的位置一律通过它们**（不要绕开自己 popBlocking 手写"取 + 校验"），保证：
- Pad 的 actual_type 在唯一路径下被设置
- CapsEvent 与 Pad 能力集合的匹配错误一律在 Ready 阶段抓住
- helper 返回 false 时 `onStreamInfo` 直接把 false 传出去触发 Ready 事务性回滚，`lastError()` 可查

DecodeNode::onStreamInfo() 参考实现：

```cpp
bool DecodeNode::onStreamInfo() override {
    // 1. 用 receiveCapsEvent 从 SinkPad Subscription acquire/ack CapsEvent
    //    内部完成：popBlocking → 校验类型 → setActualType → 存入 negotiated_caps_["in"]
    if (!receiveCapsEvent("in")) return false;
    const CapsEvent& in_caps = negotiated_caps_["in"];

    // 2. 打开解码器
    codec_ = avcodec_find_decoder(in_caps.codec_id);
    ctx_   = avcodec_alloc_context3(codec_);
    // 填充 extradata
    if (!in_caps.extradata.empty()) {
        ctx_->extradata = (uint8_t*)av_malloc(in_caps.extradata.size());
        memcpy(ctx_->extradata, in_caps.extradata.data(), in_caps.extradata.size());
        ctx_->extradata_size = (int)in_caps.extradata.size();
    }
    if (avcodec_open2(ctx_, codec_, nullptr) < 0) {
        postMessage(MessageType::ERROR, "DecodeNode: failed to open decoder");
        return false;
    }

    // 3. 解码器打开成功后，才能构造输出 CapsEvent
    //    输出参数由解码器决定，不能直接透传输入 CapsEvent 的值
    CapsEvent out_caps;
    out_caps.media_type  = (in_caps.media_type == MediaType::VIDEO_ENCODED)
                           ? MediaType::VIDEO_RAW : MediaType::AUDIO_RAW;
    out_caps.width       = ctx_->width;
    out_caps.height      = ctx_->height;
    out_caps.pix_fmt     = ctx_->pix_fmt;        // 解码器决定，不是输入指定的
    out_caps.sample_rate = ctx_->sample_rate;
    out_caps.channels    = ctx_->ch_layout.nb_channels;
    out_caps.sample_fmt  = ctx_->sample_fmt;

    // 4. resize 逻辑输出 Route，再 publish 一次 CapsEvent
    auto* output_pad = getSrcPad("out_0");
    output_pad->route()->resize(selectRouteCapacity(out_caps.media_type));
    if (!sendCapsEvent("out_0", out_caps)) return false;
    return true;
}
```

### 8.3 CapsEvent 传递链路示意

```
DemuxNode ──[VIDEO_ENCODED CapsEvent]──→ DecodeNode ──[VIDEO_RAW CapsEvent]──→ VideoRenderNode
          ──[AUDIO_ENCODED CapsEvent]──→ DecodeNode ──[AUDIO_RAW CapsEvent]──→ AudioPlayNode

DemuxNode ──[VIDEO_ENCODED CapsEvent]──→ MuxNode    （直接推流，无需解码，Mux 用 CapsEvent 初始化输出容器）
```

---

## 9. AV Sync（音视频同步）

### 9.1 设计原则

- 以音频播放进度为主时钟（Master Clock）
- 视频渲染以主时钟为参考，通过等待或丢帧保持同步
- Clock 对象挂在 Pipeline 上，节点通过 `pipeline_->clock()` 访问，节点间不直接依赖

### 9.2 Clock

```cpp
class Clock {
public:
    // AudioPlayNode 每次送数据给 SDL 后调用，更新主时钟
    void setAudioPosition(int64_t pts_us) {
        audio_base_pts_us_.store(pts_us);
        // 同时记录此时的系统时间，用于插值
        audio_base_wall_us_.store(nowUs());
    }

    // 获取当前主时钟位置（微秒）
    // = 最后一次 setAudioPosition 的 pts + 距离上次调用的系统时间差
    int64_t getPositionUs() const {
        if (!has_audio_.load()) {
            // 无音频：回退到系统墙钟
            return nowUs() - wall_start_us_;
        }
        int64_t elapsed = nowUs() - audio_base_wall_us_.load();
        return audio_base_pts_us_.load() + elapsed;
    }

    void setHasAudio(bool has) { has_audio_.store(has); }
    // 由 Pipeline::play() 在启动节点线程之前调用。
    // 只重置墙钟基准，不重置 audio_base_pts_us_ / has_audio_ ——
    // 当前 Pipeline 用完即扔的语义下够用；未来若支持 Pipeline 复用（二次 play）
    // 需要扩展为全量 reset，把 audio_base_* 和 has_audio_ 也一并清零。
    void reset() { wall_start_us_ = nowUs(); }

private:
    std::atomic<bool>    has_audio_{false};
    std::atomic<int64_t> audio_base_pts_us_{0};
    std::atomic<int64_t> audio_base_wall_us_{0};
    int64_t              wall_start_us_{0};

    static int64_t nowUs() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
};
```

### 9.3 AudioPlayNode 更新时钟

AudioPlayNode 使用 SDL3 的真实播放位置更新时钟，而不是写入量：

```cpp
void AudioPlayNode::consume(Buffer* buf) {
    SDL_PutAudioStreamData(sdl_stream_, buf->data, buf->size);

    // 用 SDL3 查询真实已播放量，计算当前播放位置
    // SDL_GetAudioStreamAvailable 返回还在 SDL 内部缓冲区中未播出的字节数
    int buffered_bytes = SDL_GetAudioStreamAvailable(sdl_stream_);
    int bytes_per_sample = SDL_AUDIO_BYTESIZE(spec_.format) * spec_.channels;
    int buffered_samples = buffered_bytes / bytes_per_sample;

    // 已提交总样本数 - 还未播出的样本数 = 当前已播出的样本数
    submitted_samples_ += buf->size / bytes_per_sample;
    int64_t played_samples = submitted_samples_ - buffered_samples;

    int64_t audio_pts_us = (played_samples * 1000000LL) / spec_.freq;
    pipeline_->clock()->setAudioPosition(audio_pts_us);
}
```

### 9.4 VideoRenderNode 同步逻辑

```cpp
void VideoRenderNode::consume(Buffer* buf) {
    int64_t frame_pts_us = buf->pts;
    int64_t clock_us     = pipeline_->clock()->getPositionUs();
    int64_t diff_us      = frame_pts_us - clock_us;

    if (diff_us > 100000) {
        // 视频超前超过 100ms：等待
        std::this_thread::sleep_for(std::chrono::microseconds(diff_us - 100000));
    } else if (diff_us < -50000) {
        // 视频落后超过 50ms：丢帧
        return;
    }
    // 渲染
    SDL_UpdateYUVTexture(...);
    SDL_RenderCopy(...);
    SDL_RenderPresent(renderer_);
}
```

---

## 10. MessageBus

所有节点通过统一的 `postMessage()` 接口上报消息，Pipeline 内部运行独立的 MessageBus 监听线程处理所有消息类型。

### 10.1 MessageBus 类

```cpp
enum class MessageType { EOS, ERROR, WARNING, INFO };

class MessageBus {
public:
    struct Message {
        MessageType  type;
        BaseNode*    sender;
        std::string  text;
        int          code = 0;
    };

    using ObserverCallback = std::function<void(const Message&)>;

    // 节点调用，线程安全
    void post(Message msg);

    // Pipeline 监听线程调用，阻塞等待
    // running 为 false 且队列空时返回 nullopt，监听线程据此退出
    std::optional<Message> waitMessage(std::atomic<bool>& running);

    // 用户注册观测回调，只接收 WARNING 和 INFO
    // 回调在 MessageBus 监听线程里执行，用户保证轻量
    void setObserver(ObserverCallback cb);

    // 唤醒可能阻塞在 waitMessage 上的线程（stop 时用）
    void notify();

    // 触发用户观测回调（Pipeline messageBusLoop 内部调用）
    // 只有 WARNING / INFO 消息走这条路
    // 注意：持有 mutex_ 时调用，回调内部不能再调 post()，否则死锁
    void notifyObserver(const Message& msg) {
        std::lock_guard lock(mutex_);
        if (observer_) observer_(msg);
    }

private:
    std::queue<Message>      queue_;
    std::mutex               mutex_;
    std::condition_variable  cv_;
    ObserverCallback         observer_ = nullptr;
};
```

### 10.2 节点侧统一上报接口

BaseNode 提供 `postMessage` 方法，节点通过它统一上报所有消息：

```cpp
void BaseNode::postMessage(MessageType type, const std::string& text, int code) {
    pipeline_->bus()->post({type, this, text, code});
    if (type == MessageType::ERROR)
        stop_requested_.store(true);
}
```

节点调用示例：

```cpp
postMessage(MessageType::EOS, "");
postMessage(MessageType::ERROR, "avcodec_open2 failed", -1);
postMessage(MessageType::WARNING, "queue full, dropping frame");
postMessage(MessageType::INFO, "decoder initialized");
```

### 10.3 Pipeline 侧处理逻辑

Pipeline 内部的 `messageBusLoop()` 统一处理所有消息类型：

| 消息类型 | 处理逻辑 |
|---------|---------|
| EOS | `active_sink_count_--`，为 0 则 `eos_cv_.notify_all()` |
| ERROR | `error_occurred_ = true`，记录 `last_error_`，`eos_cv_.notify_all()` |
| WARNING | 透传给用户观测回调 |
| INFO | 透传给用户观测回调 |

### 10.4 用户侧观测

```cpp
pipeline.bus()->setObserver([](const MessageBus::Message& msg) {
    if (msg.type == MessageType::WARNING)
        fprintf(stderr, "[WARN] %s: %s\n", msg.sender->name().c_str(), msg.text.c_str());
    else
        fprintf(stdout, "[INFO] %s: %s\n", msg.sender->name().c_str(), msg.text.c_str());
});
```

观测回调在 MessageBus 监听线程里执行，用户保证回调轻量，不做耗时操作。

---

## 11. 完整使用示例

### 11.1 本地文件播放

```cpp
Pipeline pipeline;

auto* demux   = pipeline.addNode<AVDemuxNode>("demux", "input.mp4");
auto* vdecode = pipeline.addNode<DecodeNode>("vdecode");
auto* adecode = pipeline.addNode<DecodeNode>("adecode");
auto* vrender = pipeline.addNode<VideoRenderNode>("vrender");
auto* aplay   = pipeline.addNode<AudioPlayNode>("aplay");

if (!pipeline.link(demux,   "video_0", vdecode, "in", MediaType::VIDEO_ENCODED) ||
    !pipeline.link(demux,   "audio_0", adecode, "in", MediaType::AUDIO_ENCODED) ||
    !pipeline.link(vdecode, "out_0",   vrender, "in") ||
    !pipeline.link(adecode, "out_0",   aplay,   "in")) {
    // Build/Link 阶段详细错误报告机制后续单独设计；当前仅根据返回值处理失败
    return -1;
}

pipeline.build();
if (!pipeline.play()) {
    fprintf(stderr, "pipeline play failed: %s\n", pipeline.lastError().c_str());
    return -1;
}
pipeline.waitEOS();
```

### 11.2 采集编码，同时推流 + 本地录制（分叉）

```cpp
Pipeline pipeline;

auto* vcap    = pipeline.addNode<V4L2CaptureNode>("vcap");
auto* acap    = pipeline.addNode<AudioCaptureNode>("acap");
auto* venc    = pipeline.addNode<EncodeNode>("venc");
auto* aenc    = pipeline.addNode<EncodeNode>("aenc");
auto* mux_r   = pipeline.addNode<AVMuxNode>("mux_rtsp", MuxFormat::FLV);
auto* mux_f   = pipeline.addNode<AVMuxNode>("mux_file", MuxFormat::MP4);
auto* rtsp    = pipeline.addNode<RTSPPushNode>("rtsp");
auto* file    = pipeline.addNode<FileSinkNode>("file");

vcap->setDevice("/dev/video0");
acap->setDevice("hw:0,0");
venc->setCodec("libx264");
aenc->setCodec("aac");
rtsp->setUrl("rtsp://...");
file->setPath("output.mp4");

// vcap 分叉：一路推流，一路本地录制
pipeline.link(vcap, "out_0", venc, "in");
pipeline.link(acap, "out_0", aenc, "in");
pipeline.link(venc, "out_0", mux_r, "video_in", MediaType::VIDEO_ENCODED);
pipeline.link(venc, "out_1", mux_f, "video_in", MediaType::VIDEO_ENCODED);  // venc 的第二个 SrcPad，分叉新建
pipeline.link(aenc, "out_0", mux_r, "audio_in", MediaType::AUDIO_ENCODED);
pipeline.link(aenc, "out_1", mux_f, "audio_in", MediaType::AUDIO_ENCODED);  // aenc 的第二个 SrcPad，分叉新建
pipeline.link(mux_r, "out_0", rtsp, "in");
pipeline.link(mux_f, "out_0", file, "in");

pipeline.build();
if (!pipeline.play()) {
    fprintf(stderr, "pipeline play failed: %s\n", pipeline.lastError().c_str());
    return -1;
}
pipeline.waitEOS();
```

### 11.3 文件直接推流（不转码）

```cpp
Pipeline pipeline;

auto* demux = pipeline.addNode<AVDemuxNode>("demux", "input.mp4");
auto* mux   = pipeline.addNode<AVMuxNode>("mux", MuxFormat::FLV);
auto* rtsp  = pipeline.addNode<RTSPPushNode>("rtsp");

rtsp->setUrl("rtmp://live.example.com/stream");

pipeline.link(demux, "video_0", mux, "video_in", MediaType::VIDEO_ENCODED);
pipeline.link(demux, "audio_0", mux, "audio_in", MediaType::AUDIO_ENCODED);
pipeline.link(mux, "out_0", rtsp, "in");

pipeline.build();
if (!pipeline.play()) {
    fprintf(stderr, "pipeline play failed: %s\n", pipeline.lastError().c_str());
    return -1;
}
pipeline.waitEOS();
```

---

## 12. EOS 传播流程

EOS 一般只作用于读取/播放本地音视频文件，通常只从 DemuxNode 发出，顺流传递到所有 Sink，Pipeline 在所有 Sink 收到 EOS 后停止

DemuxNode 读到文件末尾调用 sendEOSDownstream()，每条视频/音频逻辑 Route 各 publish 一次 EOS；同一 Route 的所有静态订阅者按自己的游标有序取得 EOS

TransformNode 的 runLoop 收到 EOS
  → onEvent(EOSEvent{}) → sendEOSDownstream() → 继续往下传

诸如 VideoRenderNode 和 AudioPlayNode 这些 SinkNode 收到 EOS，直接 postMessage(MessageType::EOS, "")上报。**一般来说 SinkNode 肯定是只有一路 SinkPad 的，所以收到 EOS 直接上报，不需要考虑说还要等所有 SinkPad 都收到 EOS**

Pipeline::messageBusLoop 收到 EOS，active_sink_count_--，如果确认所有 Sink 都已 EOS，则 notify eos_cv_

Pipeline::waitEOS
  → 等待 eos_cv_
  → 调用 stop()

当链路包含 MuxNode 时，Mux 在 Ready 阶段已经确认所有 SinkPad 均已连接；全部输入收到 EOS 后先写 trailer，将 trailer 产生的 pending 容器字节发送到 `out_0`，最后再向下游发送 EOS。

---

## 13. 已知问题与后续优化

1. **Route 容量与内存预算**：当前 Route 先按条目数硬限，`VIDEO_RAW=4`、`VIDEO_ENCODED=32`、`AUDIO_RAW=50`、`AUDIO_ENCODED=32`、`CONTAINER=32`。ENCODED 已从旧 tryPush 时代的 128 下调到 32；RAW audio 和 CONTAINER 要等 AudioPlay/Mux 的设备缓冲与实际输出块大小确定后再调。后续增加 payload 字节上限、节点级总内存预算和高低水位监控。
2. **分叉可靠传输已完成，但端到端零拷贝未完成**：同源分叉已共享只读 BufferRef，Route Entry 只保存一份 payload；FFmpeg AVPacket/AVFrame 与框架 Buffer、以及未来 V4L2/硬件 surface 之间仍会复制。后续需要 DMA-BUF/硬件帧等外部存储模型。
3. **有损订阅策略暂不支持**：当前所有静态订阅者都可靠。未来实时预览、统计等确需丢帧时，应在 EdgePolicy 中显式表达 `LATEST_ONLY`/drop 策略；Caps、EOS、格式变化等控制事件仍必须可靠。
4. **Buffer/输出所有权类型化**：输入已经通过 `const Buffer*` 和 RouteDelivery ack 收紧；`TransformNode::process` 的 `std::vector<Buffer*>` 输出仍依赖调用约定，后续可改为 `std::vector<BufferRef>`。
5. **Route 通知回调限制**：当前只在 Ready/onStreamInfo 阶段注册，用于唤醒 Mux 多输入调度；若未来需要运行期变更，必须定义通知列表的线程安全边界。
6. **link/build 错误报告**：核心库当前只返回 bool，不直接 fprintf，也不适合走运行期 MessageBus；详细错误报告机制仍需独立设计。
7. **Caps/动态格式变化**：Decoder 首帧前可能不知道真实 pix_fmt；当前两阶段 Caps 一次性协商不足以覆盖运行期分辨率/格式变化，需单独设计 preroll 或 generation Caps。
8. **AV Sync 与 AudioPlayNode**：Audio Clock 当前还不能代表设备真实消费位置；音频提交背压、swr delay/flush、EOS drain、完整 channel layout 需要与统一 Clock 一起重构。
9. **媒体兼容性**：Packet side data、`pkt_timebase`、`best_effort_timestamp`、send/receive EAGAIN、非 YUV420P swscale、色彩空间/HDR 等仍需按具体节点补全。
10. **Demux/Mux 边界**：同类型多 Track 暂不支持，每种媒体只选一路最佳流；Mux 当前固定 `out_0`，虽 Route 已支持可靠多订阅者输出，但动态 Mux 输出 Pad、最终交织策略、阻塞网络 I/O interrupt callback 和具体 AVMuxNode 仍待实现。
11. **传统 MP4**：项目中 `MuxFormat::MP4` 固定表示 fragmented MP4；需要 seek 回文件头的传统 MP4 应使用专用节点，而不是通用 MuxNode。

---

## 14. 文件结构

```
media-pipeline/
├── include/pipeline/
│   ├── core/
│   │   ├── Buffer.h             # Buffer、BufferRef、MediaType
│   │   ├── Caps.h               # TemplateCaps、CapsEvent
│   │   ├── Event.h              # EOSEvent、CapsEvent
│   │   ├── Pad.h                # Pad、SrcPad、SinkPad
│   │   ├── Edge.h               # Edge，持有 RouteSubscription
│   │   ├── OutputRoute.h         # 有界多订阅者 Route、Subscription、Delivery
│   │   ├── BaseNode.h           # BaseNode + SourceNode + SinkNode + TransformNode
│   │   │                          # + DemuxNode + MuxNode 五个节点基类
│   │   ├── Graph.h              # 显式邻接表、拓扑排序、静态协商
│   │   ├── Pipeline.h           # 三阶段启动、线程管理
│   │   ├── Clock.h              # 主时钟，AV Sync
│   │   └── MessageBus.h         # 节点→Pipeline 消息上报
│   └── nodes/
│       ├── V4L2CaptureNode.h    # V4L2 视频采集
│       ├── AudioCaptureNode.h   # ALSA 音频采集
│       ├── DecodeNode.h         # FFmpeg 解码
│       ├── EncodeNode.h         # FFmpeg 编码
│       ├── AVDemuxNode.h        # FFmpeg 解复用（继承 DemuxNode 基类）
│       ├── AVMuxNode.h          # FFmpeg 复用（继承 MuxNode 基类）
│       ├── VideoRenderNode.h    # SDL3 视频渲染
│       ├── AudioPlayNode.h      # SDL3 音频播放
│       ├── FileSinkNode.h       # 本地文件写入
│       └── RTSPPushNode.h       # RTSP/RTMP 推流
├── src/
│   ├── core/
│   └── nodes/
├── demo/
│   ├── player.cpp               # 本地播放
│   ├── recorder.cpp             # 采集录制
│   ├── pusher.cpp               # 推流
│   └── transcoder.cpp           # 转码
└── tests/
    ├── test_graph.cpp
    ├── test_caps.cpp
    ├── test_buffer.cpp
    └── test_av_sync.cpp
```
