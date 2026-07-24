# Media Pipeline Framework 设计文档

## 1. 项目概述

### 1.1 目标

构建一个基于有向无环图（DAG）的音视频全链路处理框架，运行于 Linux / 嵌入式 Linux 平台。用户只需声明节点与连接关系，框架自动完成拓扑管理、Caps 协商、线程调度、数据流转，从而实现采集、编码、解码、解复用、复用、渲染、播放、推流、本地录制等任意组合的音视频处理链路。

### 1.2 核心设计理念

- **DAG 驱动**：Pipeline 内部维护显式有向图，拓扑结构一等公民，Build 阶段完成全图校验
- **节点自由组合**：用户随意连接节点，框架负责协商、调度、数据流转，用户不感知内部细节
- **Pad 一对一，分叉靠多 Pad**：每个 Pad 严格连接一个对端 Pad，分叉通过节点动态创建多个 SrcPad 实现
- **逻辑流即 Route**：每条逻辑输出流拥有一个有界多订阅者 Route；Edge 持有独立 Subscription，速率差由 Route 保留窗口吸收
- **Build 静态能力 + Running 有序 Caps**：Build 时只检查 MediaType；Running 中 CapsEvent 是完整格式配置边界，不兼容的格式字段由实际消费者在使用点报错
- **线程归 Pipeline 管**：节点不持有线程，Pipeline 统一创建和销毁所有节点线程

### 1.3 依赖

| 库 | 用途 |
|---|------|
| FFmpeg (libavformat, libavcodec, libavutil, libswscale, libswresample) | 解复用、复用、编解码、像素/音频格式转换 |
| SDL3 | 视频渲染、音频播放 |
| V4L2（内核接口） | 视频采集 |
| ALSA (libasound) | 音频采集 |
| CMake 3.16+ | 构建系统 |

### 1.4 当前阶段不做的事情

- 不支持硬件编解码加速（VAAPI / V4L2 M2M）
- 不支持 DMA-BUF 零拷贝（Buffer 第一阶段全部拷贝）
- 不支持动态插件加载（.so）
- 不支持 Windows / macOS，VideoRenderNode 的 SDL 视频线程模型只面向 Linux / 嵌入式 Linux
- 不支持 PTS discontinuity、Caps generation 和通用采集 Source 的运行期 Caps 生产模型；这些需要结合 V4L2/音频采集的设备协商语义单独设计
- 不支持 VIDEO_RAW 的非 YUV420P/YUVJ420P 渲染；VideoRenderNode 当前明确拒绝该类 Caps，swscale 路径后续实现
- **Mux Header 冻结合同**：所有输入初始 encoded Caps 到齐后建立 Header 并固定 Pad→输出 stream 映射；同一输入 Pad 后续出现 encoded CapsEvent 是协议错误，必须拒绝

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

Pipeline 统一创建和管理每个 Node 的工作线程，但是具有线程亲和性的资源（如 SDL 视频相关资源）仍由所属节点线程持有完整生命周期，使用和销毁均在该节点工作线程内完成，不套用“Ready 创建、join 后 onStop 释放”的普通资源生命周期。

**进程约束**：同一进程同一时刻至多允许一个存活的 `Pipeline` 实例。`Pipeline` 构造/析构直接管理 SDL 基础设施的 `SDL_Init(0)` / `SDL_Quit()`；不支持两个 Pipeline 并存。节点只管理各自 VIDEO/AUDIO 等具体子系统，不改变此约束。

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

`CapsEvent` 是流格式的唯一权威，并作为 Running 阶段 Route 中有位置的配置边界：每份 Caps 必须完整、准确地解释其后、至下一份 Caps 前的所有 Buffer。BufferMeta 不重复流级格式，只保留逐 Buffer 变化的事实与存储布局：例如 EncodedMeta::flags、AudioRawMeta::nb_samples；当前紧密连续 VideoRaw Buffer 没有额外的逐帧 layout。Buffer 在任何 active Caps 之前到达，或其 media_type 与 active Caps 不一致，都是协议错误。

Buffer 层只忠实承载 FFmpeg 时间戳：pts/dts 无效时保留 AV_NOPTS_VALUE，duration 无效时为 0，不在此处推算时间；stream_index/pos/side_data 当前不进入 Buffer：流身份由 SrcPad/Edge 表达，seek/HDR/rotation/SEI 等后续单独设计。

AudioRaw Buffer 保持 FFmpeg sample_fmt 对应的原始布局；planar 数据按 plane 顺序拼接，消费者使用 active AudioRaw Caps 的 sample_fmt/channel_layout 和逐 Buffer 的 nb_samples 解释。

```cpp
enum class MediaType {
    VIDEO_RAW,       // 解码后的视频帧（YUV / RGB）
    AUDIO_RAW,       // 解码后的音频帧（PCM）
    VIDEO_ENCODED,   // 编码后的视频 Packet（H264 / H265 等）
    AUDIO_ENCODED,   // 编码后的音频 Packet（AAC / Opus 等）
    CONTAINER,       // 容器封装后的数据（Mux 输出）
};

struct VideoRawMeta {
    // 当前由 fromAVFrame() 生成紧密连续存储；流格式由 active VIDEO_RAW Caps 描述。
};

struct AudioRawMeta {
    int nb_samples;   // 逐帧属性；采样率、sample_fmt、channel_layout 属于 active Caps
};

struct EncodedMeta {
    int flags;        // 逐 Packet 属性；codec、extradata、流参数属于 active Caps
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

Caps 分两种：TemplateCaps 是节点在定义时静态声明的能力范围，只在 Build 阶段用于 MediaType 兼容性检查；CapsEvent 是 Running 阶段在 Route 中有序传递的真实流配置边界。

Caps 字段是否足够由真正使用它的生产者或消费者在自己的边界判断：例如 Decode 至少需要 encoded 的 codec_id，VideoRender 需要 RAW 视频的 width/height/pix_fmt，AudioPlay 需要 RAW 音频的 sample_rate/sample_fmt/受支持 channel_layout，具体 Mux 后端可对写 Header 所需参数再作容器特定校验

需要注意的是，**CapsEvent 完整并不代表着所有字段非零**，正确的定义应该是**该 MediaType 的消费者为了配置自己、正确解释后续 Buffer 所必需的字段都已存在且准确**。而"必需字段集"是逐 MediaType 不同的:
- RAW 视频: 没有 w/h/pix_fmt 消费者连 Buffer 的字节都读不了，所以 RAW 视频要求 w>0/h>0/pix_fmt 天经地义。(RAW 视频没要求 framerate,因为它只影响 duration、不影响布局)
- ENCODED 视频:解码器根本不依赖 caps 里的 w/h。H.264/HEVC 的尺寸在码流的 SPS/VPS 里,avcodec_open2 时 ctx_->width=0 完全合法,解码器解出第一帧时自己从 SPS 填出真实尺寸，所以对 encoded 视频,codec_id 才是唯一普遍必需的字段(挑哪个解码器);w/h 只是描述性提示
所以 **CapsEvent 完整”是指它对于特定节点，特定场景下就是一个完整的格式表达，能够完整的解释后续 Buffer 所必须的格式**，未知的 encoded 提示字段不是传输层错误，这也**绝对不等同于 best-effort**

`VIDEO_ENCODED.framerate` 保留为 Demux 提供给 Decode 的 nominal timing hint，当前用于推导输出视频 Buffer 的 duration。它不属于 payload 格式，不传播到 VIDEO_RAW Caps，不参与 `hasSameFormat()`，其单独变化不构成 Caps 配置边界。

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

// 可复制的框架声道布局值类型；转换到 FFmpeg 时临时 materialize AVChannelLayout。
struct ChannelLayout {
    AVChannelOrder order = AV_CHANNEL_ORDER_UNSPEC;
    int channels = 0;
    uint64_t mask = 0;
    std::vector<AVChannel> custom_order;

    static bool fromAV(const AVChannelLayout& source, ChannelLayout* out);
    bool toAV(AVChannelLayout* out) const;
    bool isValid() const;
};

// Running Route 中有位置的真实流配置边界
struct CapsEvent {
    MediaType media_type;

    // VIDEO_RAW / VIDEO_ENCODED 的 payload 格式字段
    int width = 0;
    int height = 0;
    AVPixelFormat pix_fmt = AV_PIX_FMT_NONE;

    // VIDEO_ENCODED 的 nominal timing hint；Decode 仅用它推导输出 Buffer duration。
    // 它不是格式字段，不传播到 VIDEO_RAW Caps，也不参与 hasSameFormat()。
    AVRational framerate = {0, 1};

    // AUDIO_RAW / AUDIO_ENCODED
    int sample_rate = 0;
    AVSampleFormat sample_fmt = AV_SAMPLE_FMT_NONE;
    ChannelLayout channel_layout;

    // VIDEO_ENCODED / AUDIO_ENCODED
    AVCodecID codec_id = AV_CODEC_ID_NONE;
    std::vector<uint8_t> extradata;

    // 比较同一 MediaType 下的 Caps 声明值；不替代节点自己的重配决策。
    bool hasSameFormat(const CapsEvent& other) const;
};
```

+ `isCompatibleWith` 检验两端 TemplateCaps 是否有交集，Graph::link 阶段使用。
+ `contains` 检验具体 MediaType 是否落在 Pad 能力集合内，用于 requestPad 的 hint_type 校验和 Running Caps 经过 Pad 时的类型校验。
+ `hasSameFormat` 只比较同一 MediaType 下需要重新解释 payload 或重配节点的字段：VIDEO_RAW 比较 width/height/pix_fmt，AUDIO_RAW 比较 sample_rate/sample_fmt/channel_layout，VIDEO_ENCODED 比较 codec_id/width/height/extradata，AUDIO_ENCODED 比较 codec_id/sample_rate/channel_layout/extradata。VIDEO_ENCODED 的 framerate 是 duration 推导 hint，不参与比较；该函数不决定任何节点是否必须重配。

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

- **`template_caps_`（TemplateCaps，能力集合，静态）**：构造 / requestPad 时确立，声明“本 Pad 可承载哪些 MediaType”。Build 阶段 `Graph::link` 只用它做交集兼容性检查。
- **`actual_type_`（optional<MediaType>，实际类型，运行期冻结）**：Ready 前及 Running 中首份 Caps 到达前为 `nullopt`。首份 Caps 通过 TemplateCaps 校验后选定本次运行中该 Pad/逻辑 Route 实际承载的 MediaType，并在后续生命周期内保持不变；后续 Caps 只能在该固定 MediaType 内更新格式字段，跨 MediaType 是协议错误。后续 Buffer 的完整格式解释始终以该输入 Pad 最近成功应用的 active Caps 为准。

两者严格分层：`template_caps_` 是能力声明，`actual_type_` 是首份 Caps 选定并冻结的运行期事实。运行中的类型分发只能读 `actualType()`，不得从 TemplateCaps 中任选一个类型冒充实际类型。

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
    // BaseNode 仅在首份 Caps 选定实际类型时写入；之后该值冻结。
    void setActualType(MediaType t) { actual_type_ = t; }

    std::optional<MediaType> actual_type_;

    friend class BaseNode;
};
```

`setActualType` 为 private + friend BaseNode：首份 Caps 通过 TemplateCaps 校验后，由 BaseNode 将对应 Pad/逻辑 Route 的实际 MediaType 选定并冻结。后续 Caps 只能先比对该固定值是否相同；不得通过新 Caps 改写 actualType。

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

    // Ready 阶段：只初始化不依赖上游格式的资源。
    virtual bool onReady() = 0;

    // 节点停止或 Ready 回滚时释放资源；必须支持部分初始化状态。
    virtual void onStop() = 0;

    // Running 阶段收到完整输入 Caps 时调用；成功后 BaseNode 才更新 active Caps。
    // Transform 可向 outputs 加入重配前必须先发布的 delayed 输出；Sink 传 nullptr。
    virtual bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                        std::vector<QueueItem>* outputs) { return true; }

    // === 基类统一的数据分发 ===
    // QueueItem 是唯一有序发布项：Buffer、Caps、EOS 使用同一入口。
    bool publishOutputItem(QueueItem&& item, const std::string& src_pad_name = "");
    bool pushToDownstream(BufferRef&& buf, const std::string& src_pad_name = "");
    bool sendCapsEvent(const std::string& src_pad_name, const CapsEvent& caps);
    bool sendEOSDownstream();

    // 校验 media_type、调用 onCaps，成功后将 Caps 写入指定 SinkPad 的 active Caps。
    bool applyCapsEvent(const std::string& sink_pad_name, const CapsEvent& caps,
                        std::vector<QueueItem>* outputs = nullptr);

    // === 节点工作循环（由 Pipeline 创建的线程调用）===
    virtual void runLoop() = 0;

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
    // 每个 SinkPad 最近一次成功应用的完整 Caps，是其后续 Buffer 的唯一格式权威。
    std::unordered_map<std::string, CapsEvent> active_caps_;
    std::atomic<bool>                     stop_requested_{false};

    friend class Pipeline;
    friend class Graph;
};
```

几点注意事项：
1. 所有具体节点子类必须在初始化列表里调用 BaseNode 构造函数，将 name 传给 name_；BaseNode 没有默认构造函数，遗漏会直接编译失败。
2. `onReady()` 只初始化不依赖输入格式的资源：例如 Demux 打开并探测文件，AudioPlay 建立固定 canonical SDL 提交端，Mux 建立容器上下文；不得在此阶段收发 Caps。
3. `onCaps()` 在对应节点的 Running worker 内串行执行。生产者必须先发布完整 Caps，再发布受其管辖的 Buffer；消费者只有 `onCaps()` 成功后才 ack 该 Caps Delivery 并更新 active Caps。首份 Caps 同时选定 actualType；后续 Caps 只能在该固定 MediaType 内更新格式字段。
4. `publishOutputItem()` 是唯一有序发布入口，负责 Route 定位、首份 Caps 的 TemplateCaps 类型校验、后续 Caps 的 actualType 一致性校验、Route 容量选择，以及 `QueueItem` 的 RAII 所有权转移。传输层不判断字段是否足够解释流格式。
5. `pushToDownstream(BufferRef&&)` 是 Buffer 的便捷发布入口：调用方显式 `std::move` 交出引用；成功时引用进入 Route Entry，Route 缺失、输出歧义、无订阅或 cancel 等失败路径同样由入口内 RAII 释放。
6. Route Entry 只保存一份共享只读 BufferRef；每个订阅者 acquire 时复制句柄、引用计数加一，不复制 payload。所有订阅者 ack 后 Entry 才回收，最后一个 BufferRef 析构后底层 Buffer 才释放。
7. `sendCapsEvent()` 只定位指定的已连接逻辑 Route，随后委托 `publishOutputItem()` 对整条共享 Route 执行首份 TemplateCaps 校验或后续 actualType 冻结校验；`applyCapsEvent()` 对 SinkPad 执行同样的首份选择/后续比对，并把字段充分性判断委托给节点 `onCaps()`。Buffer 在 active Caps 之前到达，或 media_type 与 active Caps 不一致，节点必须 postMessage(ERROR)。
8. 动态请求 Pad 只在 link 时，目标 Pad 不存在时调用，节点构造时已声明的固定 Pad（比如 TransformNode 的 "in"）不走这条路，固定 Pad 优先命中，只有当 Pad 不存在时，才调用这两个方法，节点决定是否允许创建以及新 Pad 的 TemplateCaps。**需要支持分叉的节点（Source/Transform 的多路输出）和多路输入的节点（DemuxNode、MuxNode）一定需要重写对应的方法**。requestPad 有两种不同的语义模型，各自对应不同的 TemplateCaps 处理方式：
    + Source / Transform 分叉：属于再多一路同源输出，新 pad 的 TemplateCaps 必须和已经存在的 pad 的 TemplateCaps 一致，其最终的 ActualType 自然也落在已有 pad 的能力集合内，因为这种情况本质是同一份处理结果的多路拷贝，所以一个 SourceNode/TransformNode 内所有 SrcPad 的能力声明应当一致，所有 SinkPad 的能力声明应当一致
    + Demux / Mux 多路：属于开一路服务 hint_type 的具体流端口，新 pad 的 TemplateCaps 就是 `{hint_type}`，一个 pad 服务一种流身份。因为在 Demux/Mux 这种节点中，每个 SinkPad 对应容器里一路流，各 pad 天然可能会承载不同类型
9. requestPad 的 hint_type 仅用于 Build 前的能力校验和决定新 Pad 的 TemplateCaps，不代表 actual_type；不得直接设置 actual_type。
10. requestPad 会立即把新 Pad 加入节点，Graph::link 后续任一步失败都必须 release 本次 request 创建且尚未连接的 Pad；有附属状态的节点同步清理，例如 DemuxNode 的 pad_to_type_。


### 5.2 SourceNode

`SourceNode` 的当前 `capture() -> Buffer*` 默认骨架只能表达 `Buffer* → EOS`，早于 Running Caps 协议，不能独自表达 `Caps → Buffer* → Caps → Buffer*`。本地播放器的 `DemuxNode` 不使用该默认骨架，而是在自己的 worker 中先发布探测到的 encoded Caps。

通用采集源尚未落地；V4L2/AudioCapture 接入时必须先根据设备协商、重配和运行期格式变化语义，设计能够生产有序 `QueueItem` 的 Source 抽象。此项在“已知问题与后续优化”中单独保留，当前不得把默认 `capture()` 骨架用于连接要求 Running Caps 的新节点。

需要注意的是用户可能对同一路输出连接多个下游，比如采集到的画面可以一路直接本地预览，一路编码之后传输，甚至可以有别的路用来作别的格式的编码或者其他的处理，**所以 SourceNode 的 requestSrcPad 需要重写，需要支持分叉**

在 SourceNode 下，新 SrcPad 是已有 SrcPad 的同源多路拷贝，所以能力集合必须和已有 pad 的保持一致，hint_type 只用于校验"这次 link 想承载的类型是否在已有能力集合内"，不参与 TemplateCaps 的构造


### 5.3 SinkNode

SinkNode 在自己的 Route worker 内按序处理 Caps、Buffer、EOS：

```text
CapsEvent
→ applyCapsEvent()：校验 Pad MediaType → Sink::onCaps() → 成功后写 active Caps → ack

BufferRef
→ 必须已有 active Caps，且 Buffer.media_type 与其一致
→ consume(Buffer)
→ consume 成功且未停止后 ack

EOSEvent
→ ack（先释放上游 Route）
→ onDrain()
→ 未停止时 postMessage(EOS)
```

`consume()` 因而只会收到已被最近成功应用的 active Caps 完整解释的 Buffer。格式字段是否足够由该 Sink 的 `onCaps()` 判断；例如 VideoRender 要求 YUV420P/YUVJ420P 的 width/height/pix_fmt，AudioPlay 要求完整且受支持的 AUDIO_RAW 参数。Caps 应用失败或 Buffer 违反顺序/类型合同都不 ack 当前 Delivery，由 RAII 撤销 in-flight 状态，随后通过 ERROR/stop 统一取消。

`onDrain()` 默认为空；AudioPlay 重写它以等待 swr/SDL/设备尾音，VideoRender 保持默认实现。

Buffer 必须在处理完成后才 ack；停止或处理失败时不 ack 当前 Delivery，由 Delivery 的 RAII 析构撤销 in-flight 状态。EOS 在 ack 后才上报 Sink 完成，Route cancel 负责唤醒阻塞中的 `acquireBlocking()`

### 5.3.1 VideoRenderNode

`VideoRenderNode` 是 `SinkNode` 的具体 SDL3 视频渲染实现。它仍由 Pipeline 为其创建独立工作线程，但 SDL 视频资源具有线程亲和性，其完整生命周期必须在该工作线程内完成。其余进程级约束、Ready/Running 生命周期、SDL 事件范围和停止请求语义如下：

- **当前进程只支持一个 VideoRenderNode，且该节点必须是进程中首次真正初始化 SDL 视频子系统的拥有者**
- Pipeline 构造函数调用 `SDL_Init(0)` 建立 SDL 基础设施生命周期，但不带 VIDEO/AUDIO 等子系统 flag；这不会设置 SDL 视频主线程身份
- **同一进程同一时刻至多一个 Pipeline 存活**；Pipeline 直接一一管理 `SDL_Init(0)` / `SDL_Quit()`，不支持并存 Pipeline
- 不支持多个 Pipeline 在同一进程内各自拥有 VideoRenderNode
- SDL AUDIO / EVENTS 可以提前初始化，但不改变上述 SDL VIDEO 的线程归属约束
- 本方案依赖 SDL3 在非 Apple 平台将首次调用 SDL_InitSubSystem(SDL_INIT_VIDEO) 的线程视为 SDL 视频主线程，macOS 暂不适用；未来若支持 Apple 平台，需改用符合平台主线程约束的执行模型

生命周期：
- Ready 阶段不依赖上游 Caps，只保留空的格式状态；不初始化 SDL VIDEO，也不创建 Window、Renderer 或 Texture
- Running 中收到 VIDEO_RAW Caps 时，校验完整 width/height/pix_fmt，当前仅接受紧密 YUV420P/YUVJ420P；若已有 Texture 尺寸不匹配则销毁，下一帧按新 Caps 创建
- 进入 Running 后，工作线程按如下顺序完成资源管理与渲染（`SDL_InitSubSystem(SDL_INIT_VIDEO)`）：
  - SDL_Window / SDL_Renderer 创建
  - 消费 VIDEO_RAW Buffer，创建或更新 SDL_Texture
  - Update / Clear / Render / Present
  - 线程退出前销毁 Texture / Renderer / Window
  - SDL_QuitSubSystem(SDL_INIT_VIDEO)
  - SDL_CleanupTLS()（Pipeline 的 `std::thread` 调用过 SDL API 后清理其线程局部状态）

进程级 SDL 基础设施生命周期由 `Pipeline` 管理：构造时调用 `SDL_Init(0)`，但不初始化任何具体子系统；析构时先完成 `stop()` 的 worker join 与节点资源释放，再调用一次 `SDL_Quit()`。节点仍只负责各自的 `SDL_InitSubSystem` / `SDL_QuitSubSystem`，Pipeline 不触碰 VIDEO/AUDIO 等子系统 flag。

AudioPlay 保持当前资源生命周期：Ready 调用线程初始化 AUDIO/AudioStream，音频 worker 消费和 drain，join 后 `onStop()` 销毁 AudioStream 并退出 AUDIO 子系统；但该 worker 在线程退出前同样调用 `SDL_CleanupTLS()`。

线程退出前完成线程亲和资源和 SDL TLS 的清理，Pipeline 通过 join 等待其结束。

SDL 事件处理：
- VideoRenderNode 当前只消费并处理自己窗口的 `SDL_EVENT_WINDOW_CLOSE_REQUESTED`，通过窗口 ID 过滤其他窗口事件
- 键盘、鼠标、手柄、触摸、剪贴板、拖放、音频设备、相机、`SDL_EVENT_QUIT`、窗口大小变化和其他窗口事件暂不属于本节点功能范围
- 由于事件轮询只穿插在 `consume()` 中，当前只能在 VideoRenderNode 取得视频帧时检查窗口关闭；上游长时间无帧时的事件响应及时性属于后续待优化项
- 使用 VideoRenderNode 时，SDL 事件队列由其工作线程消费，应用层不应再调用
`SDL_PollEvent()`、`SDL_WaitEvent()` 或 `SDL_PumpEvents()`

停止请求：
- 捕获自身窗口关闭请求后，VideoRenderNode 调用 `postMessage(MessageType::STOP_REQUESTED, ...)`；`BaseNode::postMessage()` 统一设置本地 `stop_requested_`，结束当前消费循环
- 该消息不是 EOS，也不是后端 ERROR
- VideoRenderNode 仍在线程退出前清理 SDL 资源，Pipeline 的 MessageBus、`waitEOS()` 和 `stop()` 分别按各自章节的合同处理该停止请求

### 5.4 TransformNode

TransformNode 有一个输入 Pad 和动态 SrcPad。其 worker 在同一输入 Route 中串行处理三类 `QueueItem`：

```text
CapsEvent
→ applyCapsEvent(in, caps, outputs)
→ 子类 onCaps 可把“旧格式必须先发出的 delayed 输出”填入 outputs
→ 按顺序发布 outputs
→ ack 输入 Caps

BufferRef
→ 要求已有 active Caps 且 media_type 匹配
→ process(input, outputs)
→ 按顺序发布 outputs
→ ack 输入 Buffer

EOSEvent
→ onEOS(outputs) 只收集 delayed Caps/Buffer
→ 基类无条件在 outputs 末尾追加唯一 EOSEvent
→ 按顺序发布 outputs
→ ack 输入 EOS，退出循环
```

`outputs` 的类型是拥有型 `std::vector<QueueItem>`，因此普通处理、Caps 重配 drain、Decoder EOS flush 和唯一终结 EOS 都经过同一个 RAII 发布边界。新 Buffer 必须立刻置入 `BufferRef`；stop 或 cancel 令发布中断时，已发布项由 Route 管理，失败项和未遍历尾项由 outputs 自动释放。

默认 `TransformNode::onCaps()` 是格式保留：将收到的 Caps 原样加入 outputs。DecodeNode 覆盖它：输入 encoded Caps 到达时先 drain 旧 decoder、重建上下文；真实 `AVFrame` 到达时才在该帧之前生成完整 RAW Caps。子类 `onEOS()` 不得追加或转发 EOSEvent，避免遗漏或重复 EOS。

同源分叉仍通过 `requestSrcPad()` 创建共享 Route 的新 SrcPad；新 Pad 复制已有 TemplateCaps，hint_type 只做能力检查。

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

1. `requestSrcPad()` 只接受 `VIDEO_ENCODED` / `AUDIO_ENCODED`；同类型多个 Pad 表示同一路最佳 Track 的静态可靠分叉。
2. `onReady()` 调用 `openInput(url_)` 和 `probeStreams(&result)`，缓存一路最佳视频/音频的 encoded Caps；基类只验证 media_type 与 codec_id，并校验用户连接的每种 Pad 都有对应流。encoded 的 width/height、sample_rate、channel_layout 都是允许未知的提示字段。
3. worker 启动后，在每条逻辑 Route 的首个 Buffer 前发布缓存的完整 encoded Caps；随后依据发布后更新的 `actualType()` 分发 Buffer，正常 EOF 时广播 EOS。
4. `onStop()` 是唯一资源释放入口。`onReady()` 失败路径不自行调用 `closeInput()`，由 `Graph::ready()` 回滚统一进入 `onStop()`。

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
    virtual bool writePacket(const Buffer* buf, int stream_index) = 0;
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

`onReady()` 只分配与输入格式无关的输出 context、确认 Pad 已连接并注册 Route 通知；具体 stream、Header 和输出 CONTAINER Caps 全部在 Running 中由输入 Caps 驱动：

1. 每个输入 Pad 的首份 encoded Caps 到达时，Mux 先校验 codec_id、Pad 能力和当前 Buffer 类型合同，再调用具体后端 `addStream()`，记录 Pad → stream 映射；容器特有字段（例如是否必须有 width/height）由 `addStream()` 判断。
2. Mux 必须等全部已连接输入都提供首份 Caps，才依次发布 `CONTAINER` Caps、调用 `writeHeader()`、flush Header 字节；此前先到的有效 encoded Buffer 保留在各自 Route 中等待，不越过 Header。
3. Header 写入后，每个 Buffer 必须已有该 Pad 的 active Caps 且 media_type 匹配；基类按最小 DTS 选择输入，`writePacket()` 成功后 flush 本次容器字节。
4. Header 建立后的 encoded Caps 再次到达明确报错；当前 Mux 不支持运行期 encoded 格式重配。

Ready 任一步失败只返回 `false`，资源由 `Graph::ready()` 回滚后统一通过 `onStop()` / `closeContext()` 释放，不在失败分支重复 close。

#### 5.6.3 pending 输出与 Header 死锁规避

FFmpeg 自定义 AVIO callback 得到的是临时容器字节，未来 `AVMuxNode` 的 callback 只能调用：

```cpp
appendContainerBytes(data, size);
```

该 helper 立即复制字节到 `pending_output_`，但不直接在 Ready 阶段生成 Buffer。Header 仍由 Mux worker 在全部输入初始 Caps 到齐后发送；这样既保证 `CONTAINER Caps → Header bytes` 的有序边界，也避免启动前在有界 Route 上阻塞。

发送顺序为：

```text
Ready:   allocate output context、注册输入 Route 通知
Running: 所有输入 initial Caps
         → CONTAINER Caps → writeHeader() → flush Header bytes
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

Ready 按拓扑顺序只执行 `node->onReady()`，建立不依赖上游格式的资源；此阶段不从 Route acquire 或向 Route publish Caps/Buffer/EOS。若任一步失败，Graph 先 cancel 全部 Route，再按拓扑逆序调用已触及节点的 `onStop()`。

进入 Running 后，各节点 worker 才在已经 seal 的 Route 上有序处理 CapsEvent、Buffer 和 EOS。Route 初始容量为 8；每份成功发布的 CapsEvent 在进入对应格式区段前将该 Route resize 到实际 MediaType 的容量。若旧格式前缀尚占用超过新容量的条目，Caps 发布在可靠背压下等待消费者排空，不破坏配置边界顺序。

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

Pipeline 对外保留两种生命周期用法：`waitEOS()` 用于由框架等待自然结束或节点停止请求，
手动模式则由应用自行调用 `pipeline.stop()`。框架不提供通用应用输入监听

VideoRenderNode 对其自身窗口关闭请求的处理是具体节点职责，不等同于框架提供通用 SDL 输入系统

```cpp
if (!pipeline.play()) {
    fprintf(stderr, "pipeline play failed: %s\n", pipeline.lastError().c_str());
    return -1;
}
pipeline.waitEOS();   // 等待自然 EOS、ERROR 或节点 STOP_REQUESTED，内部自动调 stop()

// 或者
if (!pipeline.play()) {
    fprintf(stderr, "pipeline play failed: %s\n", pipeline.lastError().c_str());
    return -1;
}
// ... 用户自己的生命周期逻辑 ...
pipeline.stop();      // 用户主动停止
```

**waitEOS() 流程**：
1. 阻塞在 `eos_cv_` 上
2. 条件：`active_sink_count_ == 0`（所有 Sink 正常 EOS）、`error_occurred_`（有节点报错）、`stop_requested_by_node_`（有节点请求 Pipeline 停止），或者 `state_` 已经不是 `RUNNING`（说明已经被外部 `stop()` 打断）
3. 唤醒后调用 `stop()`（已经停止过的情况下，`stop()` 内部 CAS 直接返回，不会重复执行）

**stop() 流程**：
1. CAS：仅当 `state_ == RUNNING` 时才能切换到 `STOPPING`，否则直接返回（已经停止/正在停止）
2. 设置所有节点 `stop_requested_.store(true)`
3. cancel 所有 OutputRoute，唤醒阻塞中的 publish/acquire 和 Mux 外部等待
4. join 所有节点线程
5. 停止 MessageBus 监听线程（`bus_running_ = false`，notify，join）
6. 按拓扑逆序调用 `onStop()` 释放不要求节点工作线程亲和性的资源；具有线程亲和性的资源由所属节点在线程退出前释放
7. `state_ = STOPPED`，notify `eos_cv_`

这两种用法也可以同时使用——比如主线程调用 `waitEOS()` 阻塞等待，另一条监听路径（信号处理、另一个线程的事件循环等）检测到退出意图后调用 `stop()`。这两条路径谁先触发不确定，**`stop()`内部的 CAS 保护保证不管谁先到，只有一次真正的清理会被执行，另一次安全地什么都不做**

**节点侧责任分工**：
- `runLoop` 退出前：数据层面收尾（flush 编解码器缓冲区，把未处理完的数据推出去）；具有线程亲和性的资源由所属节点工作线程在退出前释放
- `onStop()`：释放不要求节点工作线程亲和性的资源（`avcodec_free_context`、文件句柄等）；VideoRenderNode 的 SDL 视频资源不在此处释放


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
    std::atomic<bool>                               stop_requested_by_node_{false};
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

    // Ready 阶段：只初始化格式无关资源；Caps/Buffer/EOS 从 worker 启动后才在 Route 中传递。
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

    // 5. 按拓扑逆序调用 onStop() 释放不要求工作线程亲和性的资源。
    //    VideoRenderNode 的 SDL 视频资源已在线程退出前由其工作线程释放。
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
            || stop_requested_by_node_.load()      // 有节点请求 Pipeline 停止
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

            case MessageType::STOP_REQUESTED:
                stop_requested_by_node_.store(true);
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

**为什么 bus 必须早于 `graph_.ready()`**：`onReady()` 可能执行真实设备/文件初始化（DemuxNode 打开 URL、AudioPlay 打开 SDL 音频设备、Mux 分配容器 context 等），失败时会 `postMessage(ERROR, "...")`。bus 若尚未启动，消息只落到 queue 无人消费，`last_error_` 永远为空，§7.3 承诺的 `pipeline.lastError()` 就不可用。

---

## 8. Caps 协商与运行期格式边界

### 8.1 静态能力协商（Build 时）

每个 Pad 声明 TemplateCaps（支持的 MediaType 集合）。`Graph::link()` 只检查两端集合是否有交集；它不判断 codec、尺寸、像素格式、采样率或声道布局，也不设置 Pad 的 actualType。

```text
DecodeNode     SinkPad { VIDEO_ENCODED, AUDIO_ENCODED }
               SrcPad  { VIDEO_RAW, AUDIO_RAW }
MuxNode        SinkPad { VIDEO_ENCODED, AUDIO_ENCODED }
               SrcPad  { CONTAINER }
DemuxNode      SrcPad  { VIDEO_ENCODED, AUDIO_ENCODED }
VideoRender    SinkPad { VIDEO_RAW }
AudioPlay      SinkPad { AUDIO_RAW }
```

`actual_type` 的唯一写入者是 BaseNode：生产侧首份 Caps 经 TemplateCaps 校验后，选定共享 Route 全部 SrcPad 的实际类型；消费侧首份 Caps 的 `onCaps()` 成功后，选定对应 SinkPad 的实际类型。它在 Ready 前、及 Running 中首份 Caps 到达前都可以为 `nullopt`；后续 Caps 的 media_type 必须等于该冻结值，运行期类型分发只能读取它，不能从 TemplateCaps 推测。

### 8.2 Running 阶段的有序动态 Caps

Build 后 Route 的订阅集合 seal，随后 Ready 只建立格式无关资源。节点 worker 启动后，每条逻辑 Route 的唯一数据协议为：

```text
CapsEvent → Buffer* → CapsEvent → Buffer* → EOSEvent
```

- 谁先获得完整流格式，谁必须在第一个受该格式解释的 Buffer 前发布 Caps。
- 每份 Caps 必须完整、准确地解释它之后、直到下一份 Caps 之前的全部 Buffer；Caps 可在同一 Route 出现多次。
- Buffer 在 active Caps 之前到达，或其 media_type 与 active Caps 不一致，是协议错误。
- Caps 的应用与 Delivery ack 是同一边界：消费者的 `onCaps()` 成功后才更新 active Caps 并 ack；失败时不 ack，由 ERROR/stop 的 Route cancel 统一收尾。
- `sendCapsEvent()` 只定位指定的已连接 SrcPad/Route，随后由 `publishOutputItem()` 对共享 Route 执行类型规则：首份 Caps 用全部共享 SrcPad 的 TemplateCaps 选定并固定 actualType，后续 Caps 只允许与该冻结类型相同。`applyCapsEvent()` 对 SinkPad 执行对应的首份选择/后续比对。字段是否足够由实际生产/消费节点检查。
- `hasSameFormat()` 只比较 Caps 声明值，不能作为全局“是否必须重配”的闸门；是否重建 decoder、texture、swr 或拒绝变化由节点自己定义。

字段责任按使用点划分：

| 使用者/生产者 | 本地要求 |
|---|---|
| Decode 输入 encoded Caps | `codec_id` 必须存在；VIDEO_ENCODED 的 width/height、AUDIO_ENCODED 的 sample_rate/channel_layout 是可选提示。已提供的合法提示写入 AVCodecContext；未知提示留给 FFmpeg 从 extradata/bitstream 确定。 |
| Decode 输出 RAW Caps | 从真实 AVFrame 生成；VIDEO_RAW 必须有 width/height/pix_fmt，AUDIO_RAW 必须有 sample_rate/sample_fmt/有效 channel_layout。 |
| VideoRender | 只接受完整 YUV420P/YUVJ420P VIDEO_RAW Caps。 |
| AudioPlay | 只接受完整、标准 native channel layout 的 AUDIO_RAW Caps，并重采样为固定 canonical SDL 格式。 |
| Mux 基类 | 初始 encoded Caps 至少需要 `codec_id`；具体容器 Header 所需的尺寸/采样率等由 `addStream()` 后端校验。 |

### 8.3 节点边界

**DemuxNode** 在 Ready 探测并缓存 encoded Caps；其 worker 对每条已连接逻辑 Route 先发布 Caps，再发布 Packet Buffer。对于同类型分叉，只发布一次，所有订阅者通过共享 Route 看到相同序列。

**DecodeNode** 在输入 encoded Caps 到达时：若已有旧上下文，先把 delayed AVFrame 加入本地有序 outputs，再释放旧上下文并配置新 decoder。它不以 `avcodec_open2()` 后的 context 字段声明 RAW 格式；许多视频 decoder 此时仍没有 pix_fmt。每取得一帧真实 AVFrame，Decode 先比较其真实 RAW 格式：首帧或格式变化帧先将完整 RAW Caps 加入 outputs，随后加入该帧 Buffer。EOS flush 使用同一 outputs 序列，基类最后追加唯一 EOSEvent。

**VideoRenderNode** 在 Running 应用 Caps，记录格式并在尺寸变化时销毁旧 Texture；SDL VIDEO/Window/Renderer/Texture 仍由 worker 持有完整生命周期。

**AudioPlayNode** 在 Ready 建立固定 canonical SDL 提交端：S16 packed、默认设备派生采样率、stereo FL/FR。Running 中每份 AudioRaw Caps 先排空旧 swr 的 canonical 尾部，再重建 `input → canonical` swr；不会清空 canonical SDL 队列，故背压、提交账本和 Clock 始终使用 canonical 帧量纲。

**MuxNode** 等全部输入初始 encoded Caps 后才建立 Header；先到的 Packet 保留在各自 Route。Header 后同一输入 Pad 的 encoded Caps 变化明确报错，当前不支持容器运行期重配。

### 8.4 传递链路示意

```text
Demux worker:
VIDEO_ENCODED Caps → Packet* → EOS
                    ↓
Decode worker:
VIDEO_RAW Caps (由首个真实 AVFrame 定案) → Frame* → EOS
                    ↓
VideoRender worker

AUDIO_ENCODED Caps → Packet* → EOS
                    ↓
AUDIO_RAW Caps (由真实 AVFrame 定案) → Frame* → EOS
                    ↓
AudioPlay worker
```

---

## 9. AV Sync（音视频同步）

### 9.1 设计原则

- 以音频消费进度为主时钟（Master Clock）的**唯一权威**；视频渲染以主时钟为参考，通过等待或丢帧保持同步；墙钟只作为两次采样之间的插值
- Clock 对象挂在 Pipeline 上，节点通过 `pipeline_->clock()` 访问，节点间不直接依赖
- **锚定**：首个真实媒体时间戳，有音频时由 AudioPlayNode 用首个有效音频 Buffer 的 PTS 锚定；无音频时由呈现方（VideoRenderNode）用首帧 PTS 一次性锚定
- **权威**：音频消费进度无条件重锚，不丢弃"落后样本"，正常播放时样本本身单调且与墙钟同速；欠载时样本如实停滞，时钟如实跟随
- **偏移**：音频路径含一段不可观测、有界恒定的硬件缓冲领先，已知不精确、不校准

### 9.2 Clock

```cpp
class Clock {
public:
    // 未锚定时 getPositionUs() 返回的哨兵（与 AV_NOPTS_VALUE 同值）
    static constexpr int64_t kUnanchored = std::numeric_limits<int64_t>::min();

    // AudioPlayNode 每次提交后调用：用真实消费进度（已含首帧 PTS 锚）重锚主时钟。
    // 音频样本是权威：无条件重锚（base_pts = pts、base_wall = now）。
    void setAudioPosition(int64_t pts_us) {
        base_pts_us_.store(pts_us);
        base_wall_us_.store(nowUs());
        anchored_.store(true);
    }

    // 无音频时由呈现方（VideoRenderNode）首帧调用：仅在未锚定时生效的一次性锚定。
    void anchorOnce(int64_t pts_us) {
        if (anchored_.load()) return;
        base_pts_us_.store(pts_us);
        base_wall_us_.store(nowUs());
        anchored_.store(true);
    }

    // 获取当前主时钟位置（微秒）
    // 未锚定：返回 kUnanchored；已锚定：base_pts + 距 base_wall 的墙钟差。
    int64_t getPositionUs() const {
        if (!anchored_.load()) return kUnanchored;
        return base_pts_us_.load() + (nowUs() - base_wall_us_.load());
    }

    // 策略位：是否存在音频主时钟来源（Ready 阶段由音频节点设置）。
    void setHasAudio(bool has) { has_audio_.store(has); }
    bool hasAudio() const { return has_audio_.load(); }

    // play 时调用：清除锚定与基准，等待新一轮 Running 重新锚定。
    void reset() {
        anchored_.store(false);
        base_pts_us_.store(0);
        base_wall_us_.store(0);
    }

private:
    std::atomic<bool>    anchored_{false};
    std::atomic<int64_t> base_pts_us_{0};
    std::atomic<int64_t> base_wall_us_{0};
    std::atomic<bool>    has_audio_{false};

    static int64_t nowUs() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
};
```

### 9.3 AudioPlayNode 更新时钟

AudioPlayNode 以 SDL3 设备真实消费进度更新主时钟，而不是写入量或 `SDL_GetAudioStreamAvailable`。Ready 阶段先建立固定 application-side canonical 提交格式：S16 packed、默认播放设备派生的采样率、stereo FL/FR。输入 AUDIO_RAW Caps 在 Running 中只重建 `input → canonical` 的 swr；设备周期、背压、SDL queued 查询和 Clock 账本始终使用 canonical 帧量纲。

音频输出维护完整提交账本和首个有效 PTS 的显式基线：

```text
submitted_frames_
    所有成功提交给 SDL 的 canonical PCM 总帧数
frames_before_anchor_
    首个有效 PTS 所在输入 Buffer 送入 swr 前，已进入 SDL 队列的前缀
    加上 swr 内仍属于 NOPTS 前缀的 canonical delay
anchor_pts_us_
    首个有效 PTS
```

`SDL_GetAudioStreamQueued` 返回输入队列中尚未被设备读路径取走的 canonical 字节数，因此：

```text
queued_frames = SDL_GetAudioStreamQueued(stream) / canonical_bytes_per_frame
consumed_from_anchor =
    submitted_frames - frames_before_anchor - queued_frames
```

- `consumed_from_anchor < 0`：设备仍在消费锚定帧之前的 NOPTS 前缀，Clock 保持 `kUnanchored`；VideoRender 按未锚定策略呈现。
- `consumed_from_anchor >= 0`：设备已经消费到锚定帧，才允许更新 Clock。
- 锚定后继续遇到 NOPTS Buffer 时照常转换、提交和计数，不改变锚点；后续有效 PTS 也不重新锚定。
- 如果整条音频流没有有效 PTS，Audio Clock 保持未锚定。

设备已经到达锚定帧后：

```text
audible = max(0, consumed_from_anchor - canonical_device_period_frames)
clock = anchor_pts_us + audible / canonical_rate
```

当 `consumed_from_anchor` 位于 `[0, canonical_device_period_frames]` 时，采样点上的 `audible` 被钳为 0，位置不早于 `anchor_pts_us`；两次采样之间仍按墙钟插值。

正常 `consume()`、运行期 Caps 重配前的旧 swr drain 和最终 EOS drain 都经过同一个 canonical 提交入口：

```text
SDL_PutAudioStreamData 成功
→ submitted_frames_ 增加 canonical 输出帧数
→ 根据锚定状态查询 queued 并刷新 Clock
```

重配前只排空旧 swr 的 canonical 尾部，不 flush 或清空 SDL canonical 队列，因此旧新两段 PCM 连续播放，账本和 Clock 不换量纲。在最终 `onDrain()` 等待 queued 清空期间，仍持续刷新 Clock，以覆盖 `consumed_from_anchor` 由负转非负的边界。

### 9.4 AudioPlayNode 提交背压

SDL AudioStream 内部缓冲对 App 无界；AudioPlayNode 在每次 canonical PCM `SDL_PutAudioStreamData` 前设置双阈值迟滞闸门，借“晚 ack”把背压沿 Route 传导到上游：

- 从打开后的物理设备取得周期，再一次性换算为 canonical 帧周期 `P`。
- `LOW = N_low × P`，并有 canonical 毫秒下限。
- `HIGH = LOW + N_band × P`，同样有 canonical 毫秒带宽下限。
- 若 canonical queued 帧数超过 HIGH，取消感知地轮询等待到 `queued ≤ LOW`；被 stop 打断则不 put、不更新时钟。

输入 AudioRaw Caps 改变只会重建 input→canonical swr，不改变水位、SDL 队列换算或 Clock 的量纲。

### 9.5 EOS Drain

SinkNode 收到输入 EOS 后，先 ack（释放上游 Route，不占背压窗口），再调用 `onDrain()`，最后才上报最终 EOS。`onDrain()` 默认空实现（VideoRenderNode 不受影响）。AudioPlayNode 的 `onDrain()`：

1. swr 尾部排空（最终 EOS，或新 AudioRaw Caps 到达前的旧输入格式）；
2. 最终 EOS 时调用 `SDL_FlushAudioStream` 把 SDL 内部残留转换出来；运行期 Caps 重配不 flush SDL canonical 队列；
3. 最终 EOS 时取消感知等待 `SDL_GetAudioStreamQueued()` 归零（返回值 -1 视为失败）；
4. 再等待 3 个设备周期，覆盖设备周期缓冲与后端硬件缓冲的尾音；
5. `onDrain()` 内出错或被打断（`stop_requested_` 置位）时，不再上报 EOS。

### 9.6 VideoRenderNode 同步逻辑

VideoRenderNode 统一从 `pipeline_->clock()` 读取主时钟，不再保留私有的 steady_clock 路径：

- 未锚定：立即呈现（不同步）；无音频时本帧调用 `anchorOnce(frame_pts)` 一次性锚定。
- 已锚定：`diff = frame_pts − clock_pos`；超前则取消感知等待，落后则立即追帧。
- 丢帧策略本轮不实现（待接音频接线阶段单独定阈值）。

```cpp
bool VideoRenderNode::waitForPresentationTime(int64_t pts_us) {
    Clock* clock = pipeline_->clock();
    int64_t pos = clock->getPositionUs();
    if (pos == Clock::kUnanchored) {
        if (!clock->hasAudio()) clock->anchorOnce(pts_us);
        return true;
    }
    while (pts_us - pos > 0 && !stop_requested_.load()) {
        // 取消感知等待，10ms 或剩余较小者
        pos = clock->getPositionUs();
    }
    return !stop_requested_.load();
}
```


---

## 10. MessageBus

所有节点通过统一的 `postMessage()` 接口上报消息，Pipeline 内部运行独立的 MessageBus 监听线程处理所有消息类型

### 10.1 MessageBus 类

```cpp
enum class MessageType { EOS, ERROR, STOP_REQUESTED, WARNING, INFO };

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
    if (type == MessageType::ERROR || type == MessageType::STOP_REQUESTED)
        stop_requested_.store(true);
}
```

节点调用示例：

```cpp
postMessage(MessageType::EOS, "");
postMessage(MessageType::ERROR, "avcodec_open2 failed", -1);
postMessage(MessageType::STOP_REQUESTED, "window close requested");
postMessage(MessageType::WARNING, "queue full, dropping frame");
postMessage(MessageType::INFO, "decoder initialized");
```

### 10.3 Pipeline 侧处理逻辑

Pipeline 内部的 `messageBusLoop()` 统一处理所有消息类型：

| 消息类型 | 处理逻辑 |
|---------|---------|
| EOS | `active_sink_count_--`，为 0 则 `eos_cv_.notify_all()` |
| ERROR | `error_occurred_ = true`，记录 `last_error_`，`eos_cv_.notify_all()` |
| STOP_REQUESTED | `stop_requested_by_node_ = true`，`eos_cv_.notify_all()`；不在 MessageBus 线程中调用 `Pipeline::stop()` |
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
  → 子类 onEOS() 仅收集 delayed Caps/Buffer（例如 Decode flush 帧）
  → 基类在输出序列末尾追加唯一 EOSEvent
  → 依序发布后 ack 输入 EOS

诸如 VideoRenderNode 和 AudioPlayNode 这些 SinkNode 收到 EOS，`SinkNode::runLoop` 会按以下顺序处理：

1. **ack(EOS)**：先释放上游 Route 容量，让 drain 等待期间不占用背压窗口。
2. **onDrain()**：输出侧 drain。VideoRenderNode 为空实现；AudioPlayNode 在此等 swr 排空、`SDL_FlushAudioStream`、设备队列吃空，再覆盖几个设备周期尾音。
3. **postMessage(EOS)**：drain 成功或被打断/出错后，才上报最终 EOS。

`onDrain()` 内出错或 `stop_requested_` 置位时，不再上报 EOS；这保证"自然 EOS"只代表"输入耗尽且输出真正播完"。

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
4. **Route 通知回调限制**：当前只在 Mux Ready 阶段注册，用于唤醒多输入调度；若未来需要运行期变更，必须定义通知列表的线程安全边界。
5. **link/build 错误报告**：核心库当前只返回 bool，不直接 fprintf，也不适合走运行期 MessageBus；详细错误报告机制仍需独立设计。
6. **Caps 运行期边界的剩余限制**：Decoder 首帧真实格式定案、同一 Route 多份 Caps、VideoRender 尺寸变化和 AudioPlay 输入重配已支持；尚不支持 PTS discontinuity、Caps generation、非 YUV420P swscale。
7. **采集 Source 的 Caps 生产模型**：默认 `SourceNode::capture() -> Buffer*` 不能表达 `Caps → Buffer* → Caps → Buffer*`。V4L2/AudioCapture 落地前必须先结合设备 open、协商、重配及运行期格式变化，设计有序 `QueueItem` 的 Source 生产接口；当前不得用该旧默认骨架接入要求 Running Caps 的新节点。
8. 音视频同步丢帧阈值（落后多少丢帧）待定。
9. **媒体兼容性**：Packet side data、`best_effort_timestamp`、send/receive EAGAIN、非 YUV420P swscale、色彩空间/HDR 等仍需按具体节点补全；`pkt_timebase` 已设置为框架微秒时间基，完整 channel layout 已以 Caps 的 `ChannelLayout` 值类型传递，但 AudioPlay 当前只承诺标准 native layout 到 stereo 的转换。
10. **VideoRender 事件轮询**：当前只在有视频帧进入 `consume()` 时检查自身窗口关闭请求；上游无帧期间的窗口事件响应及时性仍待优化。
11. **Demux/Mux 边界**：同类型多 Track 暂不支持，每种媒体只选一路最佳流；Mux 当前固定 `out_0`，虽 Route 已支持可靠多订阅者输出，但动态 Mux 输出 Pad、最终交织策略、阻塞网络 I/O interrupt callback 和具体 AVMuxNode 仍待实现。
12. **传统 MP4**：项目中 `MuxFormat::MP4` 固定表示 fragmented MP4；需要 seek 回文件头的传统 MP4 应使用专用节点，而不是通用 MuxNode。
13. **启动时序对齐（栅栏，暂缓）**：当前多呈现 Sink 启动时首次输出延迟不对称（如音频设备在 Ready 阶段即出声，而视频窗口创建需数秒），造成 A/V 起跑不齐。完整的启动栅栏设计需让共享主时钟的呈现型 Sink 在全部就绪后才同时出首份输出；已讨论但暂缓，待后续实现。
14. 当前 Clock 是多字段原子快照：base_pts_us_/base_wall_us_/anchored_ 是三个独立的 memory_order_relaxed 原子,setAudioPosition 写三次、getPositionUs 读两次,中间没有任何东西保证这五次操作在其他线程眼里是一个原子整体。C++ 内存模型允许读者看到"新 pts + 旧 wall"撕裂组合。当前不会崩溃、不会破坏不变量,下一次 getPositionUs 就自我修正。真正需要收紧内存序的场景是"未来出现多写者"或"要给撕裂上硬性正确性保证"。
15. **第三方 GUI LeakSanitizer 基线**：当前 Linux/X11 环境的 SDL3 2D software renderer 在 window surface 呈现时会内部尝试 GPU texture framebuffer，加载 Mesa/GLX；即使独立最小程序完整销毁 Texture、Renderer、Window，退出 VIDEO 并调用 `SDL_Quit()`，LeakSanitizer 仍报告 Mesa/GLX 约 1464B/16 allocations。强制直接 X11 framebuffer 可避免 Mesa 报告，但会出现约 33066B/572 allocations 的 X11/XKB 报告。两者均可由独立 SDL 最小程序复现，不属于 Pipeline、Buffer、Route 或节点资源泄漏；不为消除报告而改 renderer/backend。player 的 LeakSanitizer 验证应将该 Mesa/GLX 基线与项目自身泄漏区分，框架单测仍无 suppression 严格运行。
16. SDL_GetAudioDeviceFormat 查询已打开设备偶发返回空错误失败,而当前它被当硬 ERROR 直接毙掉整个 Ready。将来或可对这个查询加一次重试/容忍,但现在按硬失败处理也说得过去,先不动。

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
