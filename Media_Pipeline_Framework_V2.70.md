# Media Pipeline Framework V2 设计文档

## 1. 项目概述

### 1.1 目标

构建一个基于有向无环图（DAG）的音视频全链路处理框架，运行于 Linux / 嵌入式 Linux 平台。用户只需声明节点与连接关系，框架自动完成拓扑管理、Caps 协商、线程调度、数据流转，从而实现采集、编码、解码、解复用、复用、渲染、播放、推流、本地录制等任意组合的音视频处理链路。

### 1.2 核心设计理念

- **DAG 驱动**：Pipeline 内部维护显式有向图，拓扑结构一等公民，Build 阶段完成全图校验
- **节点自由组合**：用户随意连接节点，框架负责协商、调度、数据流转，用户不感知内部细节
- **Pad 一对一，分叉靠多 Pad**：每个 Pad 严格连接一个对端 Pad，分叉通过节点动态创建多个 SrcPad 实现
- **边即队列**：每条连接边（Edge）携带一个独立的有界队列，节点间天然解耦，速率差异由队列吸收
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
│   └── Edge（连接两个 Pad，携带一个 Queue）
├── Clock（主时钟，AV Sync）
├── MessageBus（节点向 Pipeline 上报消息）
└── 线程表（每个 Node 对应一个 std::thread）
```

### 2.2 数据流模型

数据流方向：上游节点 → Edge Queue → 下游节点。

- 上游节点的 runLoop 将处理结果 push 到 SrcPad 对应的 Edge Queue
- 下游节点的 runLoop 从 SinkPad 对应的 Edge Queue pop 数据
- **单路连接**（SrcPad 只有一个下游）：push 使用阻塞 API，队列满时上游自然阻塞，背压传导
- **多路连接**（SrcPad 连接了多个下游，即分叉）：push 使用非阻塞 API，队列满时直接丢弃，各路互不阻塞
  - 阶段性设计妥协：当前以能跑通为目标，多路分叉 `tryPush` 满则丢弃是明确策略，不作为当前风险项。

```
[单路]
SourceNode ──push(阻塞)──→ [Edge Queue] ──pop(阻塞)──→ TransformNode

[分叉]
EncodeNode ──tryPush(非阻塞)──→ [Edge Queue A] ──→ MuxNode(推流)
           ──tryPush(非阻塞)──→ [Edge Queue B] ──→ MuxNode(录制)
```

### 2.3 节点五分类

| 类型 | SinkPad 数量 | SrcPad 数量 | 驱动方式 |
|------|-------------|-------------|---------|
| **SourceNode** | 0 | 动态（≥1） | 独立线程，阻塞采集 |
| **SinkNode** | 动态（≥1） | 0 | 独立线程，阻塞消费 |
| **TransformNode** | 1 | 动态（≥1） | 独立线程，从 SinkPad Queue pop 驱动 |
| **DemuxNode** | 0 或 1 | 动态（≥1，运行时创建） | 独立线程，读文件/URL 驱动 |
| **MuxNode** | 动态（≥1） | 1 | 独立线程，多路 Queue 多路复用监听 |

**Pad 数量说明**：说"动态"是指 Build 阶段根据用户 link 调用动态创建，每次 link 在对应节点上增加一个 Pad。

---

## 3. 核心数据结构

### 3.1 Buffer

Buffer 是框架内所有数据的载体，拥有独立的引用计数体系，与 FFmpeg 的 AVFrame / AVPacket 解耦。节点从 FFmpeg 结构中拷贝数据填入 Buffer 后，立即释放原始 FFmpeg 结构。

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

// 阶段性设计妥协：BufferMeta 与 CapsEvent 存在部分重复，EncodedMeta/extradata
// 当前不要求逐包完整填充；下游初始化以 Ready 阶段 CapsEvent 为准。
// Buffer 层只忠实承载 FFmpeg 时间戳：pts/dts 无效时保留 AV_NOPTS_VALUE，
// duration 无效时为 0，不在此处推算时间。stream_index/pos/side_data 当前不进入
// Buffer：流身份由 SrcPad/Edge 表达，seek/HDR/rotation/SEI 等后续单独设计。
// AudioRaw Buffer 保持 FFmpeg sample_fmt 对应的原始布局；planar 数据按 plane
// 顺序拼接，下游通过 sample_fmt/channels/nb_samples 解释。

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
    std::atomic<int> ref_count{1};
    void ref()   { ref_count.fetch_add(1); }
    void unref() { if (ref_count.fetch_sub(1) == 1) delete this; }

    // 分叉时的深拷贝（第一阶段，后续优化为引用计数零拷贝）
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
    explicit BufferRef(Buffer* buf) : buf_(buf) {}
    ~BufferRef() { if (buf_) buf_->unref(); }
    Buffer* get() const { return buf_; }
    Buffer* operator->() const { return buf_; }
    BufferRef clone() const { return BufferRef(buf_->clone()); }
private:
    Buffer* buf_;
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

### 3.3 Event（事件）

事件与数据分开传递，在 Edge Queue 内与 Buffer 共用同一个队列（使用 variant），保证事件和数据的相对顺序。

```cpp
struct EOSEvent {};

using Event = std::variant<CapsEvent, EOSEvent>;

// Queue 中存放的元素：Buffer 或 Event
using QueueItem = std::variant<BufferRef, Event>;
```

### 3.4 Edge 与 Queue

Edge 是图的一等公民，Graph 显式维护所有 Edge。每条 Edge 携带一个 BoundedQueue，队列容量根据数据类型在创建时确定。

```cpp
// 默认队列容量（按 MediaType）
// Raw Video Frame（1080p YUV420 约 3MB/帧）：4 帧，约 12MB
// Raw Audio PCM：50 帧
// Encoded Packet（几十 KB/帧）：128 帧
// Container（Mux 输出）：32 帧

constexpr size_t DEFAULT_QUEUE_CAPACITY_VIDEO_RAW     = 4;
constexpr size_t DEFAULT_QUEUE_CAPACITY_AUDIO_RAW     = 50;
constexpr size_t DEFAULT_QUEUE_CAPACITY_ENCODED       = 128;  // 设大以降低 DemuxNode tryPush 丢包概率
constexpr size_t DEFAULT_QUEUE_CAPACITY_CONTAINER     = 32;

class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {}

    // 阻塞 push：队列满时等待，直到有空间或被 flush 唤醒
    // push 成功后触发外部 notify 回调（如果已注册）
    void pushBlocking(QueueItem item);

    // 非阻塞 push：队列满时直接返回 false，不阻塞
    // push 成功后同样触发外部 notify 回调
    bool tryPush(QueueItem item);

    // 阻塞 pop：队列空时等待，直到有数据或被 flush 唤醒
    std::optional<QueueItem> popBlocking();

    // 非阻塞 pop：队列空时返回 nullopt
    std::optional<QueueItem> tryPop();

    // 查看队首但不取出（MuxNode 选最小 DTS 时使用）
    std::optional<QueueItem> peek() const;

    // flush：唤醒所有阻塞的 push/pop，用于停止流程
    void flush();

    // 调整队列容量，只在 onStreamInfo() 阶段调用
    // 此时队列里只有 CapsEvent，不会有数据溢出问题
    void resize(size_t new_capacity);

    size_t size() const;
    bool   empty() const;
    bool   full() const;

    // 注册外部 notify 回调
    // 每次 push 成功后，在释放内部锁之后调用此回调
    // MuxNode 用此机制将自身的 mux_cv_ 与多个 Queue 联动：
    //   任意一个 SinkPad 的 Queue 收到数据，mux_cv_ 就被 notify，MuxNode 被唤醒
    // 注意：回调在 push 线程中执行，必须轻量，只做 notify，不做阻塞操作
    using NotifyCallback = std::function<void()>;
    void setExternalNotify(NotifyCallback cb) {
        std::lock_guard lock(mutex_);
        external_notify_ = std::move(cb);
    }

private:
    // push 成功后的通知逻辑（内部复用）
    // 调用时必须已释放 mutex_，避免回调中再次加锁导致死锁
    void notifyAfterPush() {
        not_empty_.notify_one();
        if (external_notify_) external_notify_();
    }

    std::queue<QueueItem>    queue_;
    std::mutex               mutex_;
    std::condition_variable  not_empty_;
    std::condition_variable  not_full_;
    size_t                   capacity_;
    bool                     flushing_        = false;
    NotifyCallback           external_notify_ = nullptr;
};

struct Edge {
    // 连接关系
    BaseNode*        src_node;
    std::string      src_pad_name;
    BaseNode*        dst_node;
    std::string      dst_pad_name;

    // 每条边独立的队列
    std::unique_ptr<BoundedQueue> queue;

    // 工厂：创建 Edge（不含 Queue，Queue 在 createQueuesForNode() 中创建）
    static std::unique_ptr<Edge> create(
        BaseNode* src, const std::string& src_pad,
        BaseNode* dst, const std::string& dst_pad);
};

// 根据实际 MediaType 选择队列容量。自由函数，不属于任何类——
// Graph::createQueuesForNode() 用它创建初始 Queue（实际场景里固定传 8，
// 见下方说明），节点的 onStreamInfo() 用它对自己 SrcPad 的 Queue 做 resize。
// 节点没有 Graph 实例也没有访问权限，所以这个函数不能挂在 Graph 上。
size_t selectQueueCapacity(MediaType type);
```

---

## 4. Pad 设计

### 4.1 Pad 基类

```cpp
enum class PadDir { SRC, SINK };

class Pad {
public:
    const std::string& name() const { return name_; }
    PadDir             dir()  const { return dir_; }
    BaseNode*          node() const { return node_; }
    TemplateCaps       templateCaps() const { return template_caps_; }

    bool isConnected() const { return edge_ != nullptr; }
    Edge* edge() const { return edge_; }

protected:
    std::string    name_;
    PadDir         dir_;
    BaseNode*      node_;
    TemplateCaps   template_caps_;
    Edge*          edge_ = nullptr;   // 连接到的 Edge（一对一）

    friend class Graph;  // Graph 负责建立连接，设置 edge_
};
```

### 4.2 SrcPad

SrcPad 负责向 Edge Queue push 数据。push 行为（阻塞/非阻塞）由节点当前的 SrcPad 总数决定：只有一个 SrcPad 时阻塞，多个 SrcPad 时非阻塞。

```cpp
class SrcPad : public Pad {
public:
    SrcPad(const std::string& name, BaseNode* node, TemplateCaps caps);

    // 由 BaseNode::pushToDownstream 调用，不直接暴露给子类
    void pushBlocking(QueueItem item);
    bool tryPush(QueueItem item);
};
```

### 4.3 SinkPad

SinkPad 负责从 Edge Queue pop 数据。pop 行为（阻塞/非阻塞）由节点类型决定，通常使用阻塞 pop。

```cpp
class SinkPad : public Pad {
public:
    SinkPad(const std::string& name, BaseNode* node, TemplateCaps caps);

    // 阻塞 pop，队列空时等待
    std::optional<QueueItem> popBlocking();

    // 非阻塞 pop，MuxNode 轮询时使用
    std::optional<QueueItem> tryPop();

    // 查看队首但不取出（MuxNode 选最小 DTS 时使用）
    std::optional<QueueItem> peek();
};
```

---

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
    // 所有具体节点子类必须在初始化列表里调用这个构造函数，将 name 传给 name_。
    // BaseNode 没有默认构造函数，子类忘记调用会直接编译报错，
    // 不会出现某个节点忘记设置名字导致 name_ 是空字符串的情况。
    // 例：DemuxNode(const std::string& name, const std::string& url)
    //         : BaseNode(name), url_(url) {}
    explicit BaseNode(const std::string& name) : name_(name) {}

    // === 子类实现的生命周期回调 ===

    // Ready 阶段第一步：初始化自身资源
    // Source/DemuxNode：打开设备/文件，探测流信息，但不发送 CapsEvent
    // Transform/SinkNode：此时尚未收到 CapsEvent，只做基础初始化
    // 返回 true 表示成功，false 表示失败（失败时应先往 MessageBus 发 ERROR 消息）
    virtual bool onReady() = 0;

    // Ready 阶段第三步：发送 CapsEvent 给下游（Graph 建完 Queue 后调用）
    // Source/DemuxNode：构造并发送 CapsEvent
    // Transform/SinkNode：收到上游 CapsEvent 后初始化处理器，再发出自己的输出 CapsEvent
    // 默认实现返回 true（Sink 节点无需发送）
    // 返回 true 表示成功，false 表示失败
    virtual bool onStreamInfo() { return true; }

    // 节点停止或 Ready 回滚时释放资源；必须支持部分初始化状态
    virtual void onStop() = 0;

    // === 子类的运行循环（由 Pipeline 创建的线程调用）===
    virtual void runLoop() = 0;

    // === 基类统一的数据分发（子类调用，不感知下游数量）===
    // 阻塞策略取决于节点总 SrcPad 数量：
    //   1 个 SrcPad → pushBlocking（背压传导）
    //   多个 SrcPad → tryPush（满则丢弃，各路互不阻塞）
    // src_pad_name 为空时推给所有 SrcPad（SourceNode / TransformNode 场景）
    // src_pad_name 非空时只推给指定 SrcPad（DemuxNode 按流类型分发场景）
    //
    // 所有权约定：调用者交出 buf 的所有权（buf 的 ref_count 应为 1 且未被其他
    // BufferRef 持有）。函数入口即用 BufferRef 接管，内部 RAII 全程负责 unref：
    //   - 单路：将 primary move 进 QueueItem，成功入队后由队列持有
    //   - 多路广播：primary 全程持有原 buf，各路各自 primary.clone() 深拷贝分发
    //     tryPush 失败时 BoundedQueue::tryPush 的形参 QueueItem 析构自动释放该路副本，
    //     调用者不需要（也不能）再手动 unref
    void pushToDownstream(Buffer* buf, const std::string& src_pad_name = "");

    // 向所有已连接 SrcPad 广播 EOS（阻塞 push，不允许丢失）
    // EOS 是运行期流结束事件，所有下游都必须收到。
    void sendEOSDownstream();

    // 将 CapsEvent 发送给指定 SrcPad 的下游（阻塞 push，不允许丢失）
    // CapsEvent 是 Ready / onStreamInfo 阶段的流级格式协商事件；每个 SrcPad
    // 的 CapsEvent 可以不同，因此必须显式指定 src_pad_name。
    // 只负责发送，不存储。接收方在 onStreamInfo() 里自行存入 negotiated_caps_
    void sendCapsEvent(const std::string& src_pad_name, const CapsEvent& caps);

    // sendEOSDownstream 实现示意：
    // void BaseNode::sendEOSDownstream() {
    //     for (auto& pad : src_pads_) {
    //         if (pad->isConnected())
    //             pad->edge()->queue->pushBlocking(QueueItem{Event{EOSEvent{}}});
    //     }
    // }

    // 动态创建 Pad（节点构造时声明固定 Pad 时调用，子类构造函数里直接调用）
    SrcPad*  addSrcPad(const std::string& name, TemplateCaps caps);
    SinkPad* addSinkPad(const std::string& name, TemplateCaps caps);

    // ===== 动态请求 Pad（Graph::link 在目标 Pad 不存在时调用）=====
    //
    // 节点构造时已声明的固定 Pad（比如 TransformNode 的 "in"）不走这条路，
    // Graph::link 会优先查找已存在的 Pad。只有当 Pad 不存在时，才调用这两个
    // 方法，由节点自己决定是否允许动态创建、以及创建出来的 Pad 应该是什么类型。
    //
    // hint_type 是用户在 link() 调用时传入的类型提示，节点据此判断：
    //   - 类型合法 → 创建并返回新 Pad
    //   - 类型不合法（不支持的 MediaType，或与节点已有输出类型冲突）→ 返回 nullptr
    //
    // 默认实现返回 nullptr，表示该节点不支持动态创建 Pad。
    // 需要支持分叉的节点（Source/Transform 的多路输出）和多路输入的节点
    // （DemuxNode 的多路输出、MuxNode 的多路输入）需要重写对应的方法。
    virtual SrcPad*  requestSrcPad(const std::string& name, MediaType hint_type) { return nullptr; }
    virtual SinkPad* requestSinkPad(const std::string& name, MediaType hint_type) { return nullptr; }

    // 成员
    std::string                              name_;
    Pipeline*                                pipeline_ = nullptr;
    std::vector<std::unique_ptr<SrcPad>>     src_pads_;
    std::vector<std::unique_ptr<SinkPad>>    sink_pads_;
    // 收到的 CapsEvent（key: SinkPad 名字，value: 从上游收到的 CapsEvent）
    // 源节点（DemuxNode/SourceNode）没有 SinkPad，此 map 为空
    // 非源节点在 onStreamInfo() 里从 SinkPad Queue 取出后自行存入
    std::unordered_map<std::string, CapsEvent> negotiated_caps_;
    std::atomic<bool>                     stop_requested_{false};

    friend class Pipeline;
    friend class Graph;
};
```

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

    // 支持分叉：用户对同一路输出再连一个下游时（如采集画面一路预览一路编码），
    // Graph::link 发现目标 SrcPad 不存在，会调用这里创建。
    // 新 Pad 的类型必须与已有 SrcPad 一致（同一份采集数据的多路拷贝），
    // 类型不一致则拒绝创建。
    SrcPad* requestSrcPad(const std::string& name, MediaType hint_type) override {
        if (!src_pads_.empty() &&
            src_pads_[0]->templateCaps().supported_types[0] != hint_type) {
            return nullptr;
        }
        return addSrcPad(name, TemplateCaps{{hint_type}});
    }
};
```

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

    // 支持分叉：比如 EncodeNode 编码后一路推流一路本地存储。
    // 新 Pad 的类型必须与已有 SrcPad 一致（同一份处理结果的多路拷贝），
    // 类型不一致则拒绝创建。
    SrcPad* requestSrcPad(const std::string& name, MediaType hint_type) override {
        if (!src_pads_.empty() &&
            src_pads_[0]->templateCaps().supported_types[0] != hint_type) {
            return nullptr;
        }
        return addSrcPad(name, TemplateCaps{{hint_type}});
    }
};
```

### 5.5 DemuxNode

> **说明**：`DemuxNode` 在 `include/pipeline/core/BaseNode.h` 中是**抽象基类**，只包含格式无关的共享骨架（`requestSrcPad`、`pad_to_type_`、流校验、按 `media_type` 分发、`EOS` 传播）。
> 
> 本节代码示例展示的是**基于 FFmpeg 的具体实现 `AVDemuxNode`** 的参考写法，继承 `DemuxNode` 并实现 `openInput` / `closeInput` / `probeStreams` / `readFrame` 四个钩子即可。

DemuxNode 在 `link()` 时立刻创建 SrcPad（类型由 `hint_type` 决定），和其他节点的处理方式一致。`onReady()` 阶段打开文件后，对照已经创建好的 Pad 校验文件里是否存在对应的流，找不到则直接报错退出。

```cpp
class DemuxNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::DEMUX; }

    // 参数：url（本地文件路径或网络 URL）
    void setUrl(const std::string& url) { url_ = url; }

protected:
    // 用户对每一路视频/音频输出（包括分叉）调用 link 时，目标 SrcPad 不存在，
    // Graph::link 会调用这里创建。hint_type 必须是 VIDEO_ENCODED 或 AUDIO_ENCODED，
    // 其他类型直接拒绝。
    SrcPad* requestSrcPad(const std::string& name, MediaType hint_type) override {
        if (hint_type != MediaType::VIDEO_ENCODED && hint_type != MediaType::AUDIO_ENCODED) {
            return nullptr;
        }
        auto* pad = addSrcPad(name, TemplateCaps{{hint_type}});
        pad_to_type_[name] = hint_type;
        return pad;
    }

    bool onReady() override {
        // 1. 打开文件
        if (avformat_open_input(&fmt_ctx_, url_.c_str(), nullptr, nullptr) < 0) {
            pipeline_->bus()->post(Message{MessageType::ERROR, this, "DemuxNode: avformat_open_input failed"});
            return false;
        }
        avformat_find_stream_info(fmt_ctx_, nullptr);

        // 2. 用 av_find_best_stream 选流，每种类型只取一路最优流
        int video_idx = av_find_best_stream(fmt_ctx_, AVMEDIA_TYPE_VIDEO, -1, -1, nullptr, 0);
        int audio_idx = av_find_best_stream(fmt_ctx_, AVMEDIA_TYPE_AUDIO, -1, -1, nullptr, 0);

        // 3. 校验：已创建的 Pad 对应的类型，文件里是否真的存在
        //    用户连了视频 Pad 但文件没有视频流 → 报错
        //    用户连了音频 Pad 但文件没有音频流 → 报错
        //    同一类型可以有多个下游（分叉时各自建的 Pad），对应同一路流分发多次
        bool user_wants_video = false;
        bool user_wants_audio = false;
        for (auto& [pad_name, type] : pad_to_type_) {
            if (type == MediaType::VIDEO_ENCODED) user_wants_video = true;
            if (type == MediaType::AUDIO_ENCODED) user_wants_audio = true;
        }
        if (user_wants_video && video_idx < 0) {
            pipeline_->bus()->post(Message{MessageType::ERROR, this,
                "DemuxNode: file has no video stream, but a video pad was linked"});
            return false;
        }
        if (user_wants_audio && audio_idx < 0) {
            pipeline_->bus()->post(Message{MessageType::ERROR, this,
                "DemuxNode: file has no audio stream, but an audio pad was linked"});
            return false;
        }

        // 4. 记录 stream index 供 runLoop 使用
        if (user_wants_video) video_stream_idx_ = video_idx;
        if (user_wants_audio) audio_stream_idx_ = audio_idx;

        // 5. 构造 CapsEvent，在 onStreamInfo() 中发送
        if (user_wants_video) {
            AVStream* st = fmt_ctx_->streams[video_idx];
            video_caps_.media_type = MediaType::VIDEO_ENCODED;
            video_caps_.codec_id   = st->codecpar->codec_id;
            video_caps_.width      = st->codecpar->width;
            video_caps_.height     = st->codecpar->height;
            video_caps_.extradata  = {st->codecpar->extradata,
                                      st->codecpar->extradata + st->codecpar->extradata_size};
        }
        if (user_wants_audio) {
            AVStream* st = fmt_ctx_->streams[audio_idx];
            audio_caps_.media_type = MediaType::AUDIO_ENCODED;
            audio_caps_.codec_id   = st->codecpar->codec_id;
            audio_caps_.sample_rate = st->codecpar->sample_rate;
            audio_caps_.channels   = st->codecpar->ch_layout.nb_channels;
            audio_caps_.extradata  = {st->codecpar->extradata,
                                      st->codecpar->extradata + st->codecpar->extradata_size};
        }
        return true;
    }

    // Graph::ready() 在建完 Queue 后调用，此时 Queue 已存在可以写入
    bool onStreamInfo() override {
        // 向每个已创建的 SrcPad 发送对应类型的 CapsEvent（同一路流，多路下游各发一份）
        // 先 resize 到正确容量，再发 CapsEvent
        for (auto& [pad_name, type] : pad_to_type_) {
            SrcPad* pad = getSrcPad(pad_name);
            pad->edge()->queue->resize(selectQueueCapacity(type));
            sendCapsEvent(pad_name, type == MediaType::VIDEO_ENCODED ? video_caps_ : audio_caps_);
        }
        return true;
    }

    void runLoop() override {
        AVPacket* pkt = av_packet_alloc();
        while (!stop_requested_.load()) {
            int ret = av_read_frame(fmt_ctx_, pkt);
            if (ret < 0) {
                sendEOSDownstream();   // EOS 强制阻塞，不丢失
                break;
            }
            // 只处理用户连接的流，忽略文件中其他流
            MediaType media_type;
            if (pkt->stream_index == video_stream_idx_) {
                media_type = MediaType::VIDEO_ENCODED;
            } else if (pkt->stream_index == audio_stream_idx_) {
                media_type = MediaType::AUDIO_ENCODED;
            } else {
                av_packet_unref(pkt);
                continue;
            }
            // 转换时间戳到微秒
            AVStream* st = fmt_ctx_->streams[pkt->stream_index];
            Buffer* buf = Buffer::fromAVPacket(pkt, media_type,
                                               st->time_base,
                                               st->codecpar->codec_id);

            // 往所有对应类型的 SrcPad 分发（分叉时一路流对应多个 Pad）
            // 每个 pad 各自 clone 一份，tryPush，满了丢弃（第一阶段简易策略）
            bool first = true;
            for (auto& [pad_name, type] : pad_to_type_) {
                if (type == media_type) {
                    Buffer* to_push = first ? buf : buf->clone();
                    first = false;
                    SrcPad* pad = getSrcPad(pad_name);
                    if (!pad->tryPush(QueueItem(BufferRef(to_push))))
                        to_push->unref();
                }
            }
            // 如果 first 仍为 true，说明没有匹配的 pad，buf 未被使用
            if (first) buf->unref();

            av_packet_unref(pkt);
        }
        av_packet_free(&pkt);
    }

    void onStop() override {
        if (fmt_ctx_) avformat_close_input(&fmt_ctx_);
    }

private:
    std::string         url_;
    AVFormatContext*    fmt_ctx_          = nullptr;
    int                 video_stream_idx_ = -1;
    int                 audio_stream_idx_ = -1;
    CapsEvent           video_caps_;
    CapsEvent           audio_caps_;
    // pad_name → 该 Pad 的类型，link() 时由 requestSrcPad 记录
    std::unordered_map<std::string, MediaType> pad_to_type_;
};
```

### 5.6 MuxNode

> **说明**：`MuxNode` 在 `include/pipeline/core/BaseNode.h` 中是**抽象基类**，只包含格式无关的共享骨架（`requestSinkPad`、CapsEvent 收集、`waitAnyPadReady`、`selectMinDtsPad`、`eos_pads_` 汇合、`EOS` 传播）。
> 
> 本节代码示例展示的是**基于 FFmpeg 的具体实现 `AVMuxNode`** 的参考写法，继承 `MuxNode` 并实现 `allocateContext` / `addStream` / `writeHeader` / `writePacket` / `writeTrailer` / `closeContext` 六个钩子即可。

MuxNode 有多个 SinkPad，runLoop 使用多路复用监听：任意一路有数据就取，全部为空才阻塞。

```cpp
class MuxNode : public BaseNode {
public:
    NodeType nodeType() const override { return NodeType::MUX; }

protected:
    // 用户为每一路输入连接 link 时（比如 "video_in"、"audio_in"，命名完全由用户决定），
    // 目标 SinkPad 不存在，Graph::link 会调用这里创建。hint_type 必须是
    // VIDEO_ENCODED 或 AUDIO_ENCODED，其他类型直接拒绝。
    SinkPad* requestSinkPad(const std::string& name, MediaType hint_type) override {
        if (hint_type != MediaType::VIDEO_ENCODED && hint_type != MediaType::AUDIO_ENCODED) {
            return nullptr;
        }
        return addSinkPad(name, TemplateCaps{{hint_type}});
    }

    bool onReady() override {
        // 此阶段只做基础初始化
        // CapsEvent 由上游在 onStreamInfo() 阶段推入 SinkPad Queue
        // 输出容器的初始化在 onStreamInfo() 中完成（需要先拿到 CapsEvent）
        return true;
    }

    bool onStreamInfo() override {
        // 从每个 SinkPad 的 Queue 中取出 CapsEvent，初始化输出容器
        // 此时所有 SinkPad 的 Queue 已由 Graph 创建，可以读取

        if (avformat_alloc_output_context2(&fmt_ctx_, nullptr, format_.c_str(), nullptr) < 0) {
            pipeline_->bus()->post(Message{MessageType::ERROR, this, "MuxNode: avformat_alloc_output_context2 failed"});
            return false;
        }

        for (auto& pad : sink_pads_) {
            // 从 Queue 中取出 CapsEvent（上游的 onStreamInfo 已推入）
            auto item = pad->popBlocking();
            if (!item || !std::holds_alternative<Event>(*item)) continue;
            auto& event = std::get<Event>(*item);
            if (!std::holds_alternative<CapsEvent>(event)) continue;
            const CapsEvent& caps = std::get<CapsEvent>(event);

            // 存储收到的 CapsEvent
            negotiated_caps_[pad->name()] = caps;

            // 用 CapsEvent 初始化输出流
            AVStream* st = avformat_new_stream(fmt_ctx_, nullptr);
            st->codecpar->codec_id   = caps.codec_id;
            st->codecpar->codec_type = (caps.media_type == MediaType::VIDEO_ENCODED)
                                       ? AVMEDIA_TYPE_VIDEO : AVMEDIA_TYPE_AUDIO;
            st->codecpar->width      = caps.width;
            st->codecpar->height     = caps.height;
            st->codecpar->sample_rate = caps.sample_rate;
            if (!caps.extradata.empty()) {
                st->codecpar->extradata = (uint8_t*)av_malloc(caps.extradata.size());
                memcpy(st->codecpar->extradata, caps.extradata.data(), caps.extradata.size());
                st->codecpar->extradata_size = caps.extradata.size();
            }
            pad_to_stream_[pad->name()] = st->index;

            // 向此 SinkPad 的 Queue 注册外部 notify 回调
            // 任意一路有数据 push 进来，mux_cv_ 就被唤醒
            pad->edge()->queue->setExternalNotify([this]() {
                mux_cv_.notify_one();
            });
        }

        // 创建自定义 AVIOContext，write callback 将输出数据 push 到 SrcPad
        fmt_ctx_->pb = createCustomAVIOContext();

        // resize SrcPad 的 Edge Queue 到 CONTAINER 容量
        src_pads_[0]->edge()->queue->resize(selectQueueCapacity(MediaType::CONTAINER));

        if (avformat_write_header(fmt_ctx_, nullptr) < 0) {
            pipeline_->bus()->post(Message{MessageType::ERROR, this, "MuxNode: avformat_write_header failed"});
            return false;
        }
        return true;
    }

    void runLoop() override {
        while (!stop_requested_.load()) {
            // 多路复用监听：等待任意一个 SinkPad 的 Queue 有数据
            SinkPad* ready_pad = waitAnyPadReady();
            if (!ready_pad) break;

            auto item = ready_pad->tryPop();
            if (!item) continue;

            if (std::holds_alternative<BufferRef>(*item)) {
                Buffer* buf       = std::get<BufferRef>(*item).get();
                int     stream_idx = pad_to_stream_[ready_pad->name()];
                AVPacket* pkt     = toAVPacket(buf, stream_idx);
                av_interleaved_write_frame(fmt_ctx_, pkt);
                // write callback 触发，封装后的数据流入 SrcPad Queue → 下游
                av_packet_free(&pkt);

            } else if (std::holds_alternative<Event>(*item)) {
                const Event& event = std::get<Event>(*item);
                if (std::holds_alternative<EOSEvent>(event)) {
                    eos_pads_.insert(ready_pad->name());
                    // 所有 SinkPad 都收到 EOS 后，写 trailer 并向下游传递 EOS
                    if (eos_pads_.size() == sink_pads_.size()) {
                        av_write_trailer(fmt_ctx_);
                        sendEOSDownstream();
                        break;
                    }
                }
            }
        }
    }

    // 多路复用监听：mux_cv_ 由各 SinkPad Queue 的外部 notify 回调驱动
    // 任意一路 Queue 有数据 push 进来 → 回调触发 mux_cv_.notify_one() → 此处被唤醒
    // 全部为空才继续阻塞
    SinkPad* waitAnyPadReady() {
        std::unique_lock<std::mutex> lock(mux_mutex_);
        mux_cv_.wait(lock, [this] {
            if (stop_requested_.load()) return true;
            for (auto& pad : sink_pads_)
                if (!pad->edge()->queue->empty()) return true;
            return false;
        });
        if (stop_requested_.load()) return nullptr;
        // 从非空的队列中选 DTS 最小的那路，保证交织顺序正确
        return selectMinDtsPad();
    }

    // 从所有非空 SinkPad 中选 DTS 最小的
    SinkPad* selectMinDtsPad() {
        SinkPad*  min_pad = nullptr;
        int64_t   min_dts = INT64_MAX;
        for (auto& pad : sink_pads_) {
            auto top = pad->peek();
            if (!top) continue;
            if (std::holds_alternative<BufferRef>(*top)) {
                int64_t dts = std::get<BufferRef>(*top)->dts;
                if (dts < min_dts) { min_dts = dts; min_pad = pad.get(); }
            } else {
                // Event（EOS 等）优先处理，不参与 DTS 比较
                if (!min_pad) min_pad = pad.get();
            }
        }
        return min_pad;
    }

    // 自定义 AVIOContext：FFmpeg 写出数据时，通过 write_packet 回调
    // 将数据封装成 Buffer 推入 SrcPad Queue，流向下游（FileSinkNode / RTSPPushNode）
    AVIOContext* createCustomAVIOContext() {
        // write_packet 回调：FFmpeg 每次调用时传入一块封装好的数据
        auto write_packet = [](void* opaque, const uint8_t* buf, int buf_size) -> int {
            auto* self   = static_cast<MuxNode*>(opaque);
            Buffer* out  = new Buffer();
            out->data    = new uint8_t[buf_size];
            out->size    = buf_size;
            out->media_type = MediaType::CONTAINER;
            memcpy(out->data, buf, buf_size);
            self->pushToDownstream(out);
            return buf_size;
        };
        uint8_t* avio_buf = (uint8_t*)av_malloc(4096);
        return avio_alloc_context(avio_buf, 4096, 1, this, nullptr, write_packet, nullptr);
    }

    void onStop() override {
        if (fmt_ctx_) avformat_free_context(fmt_ctx_);
    }

private:
    std::string         format_;
    AVFormatContext*    fmt_ctx_ = nullptr;
    std::unordered_map<std::string, int>        pad_to_stream_;
    std::unordered_set<std::string>             eos_pads_;

    std::mutex              mux_mutex_;
    std::condition_variable mux_cv_;
    // mux_cv_ 由各 SinkPad Queue 注册的外部 notify 回调驱动
    // 注册时机：onStreamInfo() 中，Queue 创建完毕后立即注册
};
```

---

## 6. Graph（有向图）

### 6.1 Graph 职责

- 维护所有节点和边的显式邻接表
- `link()` 阶段：静态 Caps 兼容性检查
- `build()` 阶段：拓扑排序、环路检测、孤立节点检测
- `ready()` 阶段：按拓扑顺序依次执行 onReady() → 建 Queue → onStreamInfo()，三步穿插进行

### 6.2 Graph 数据结构

```cpp
class Graph {
public:
    // 添加节点
    void addNode(std::unique_ptr<BaseNode> node);

    // 声明连接（Build 阶段）
    // 优先查找 src/dst 上已存在的 Pad；不存在时调用节点的 requestSrcPad/
    // requestSinkPad，由节点自己决定是否允许创建（hint_type 作为类型提示）。
    // 失败原因：目标 Pad 已被占用、节点拒绝创建（requestPad 返回 nullptr）、
    // 或两端 TemplateCaps 无交集，三种情况均返回 false。
    // TODO：Build/Link 阶段的详细错误报告机制后续单独设计；核心库不直接 fprintf(stderr)。
    bool link(BaseNode* src, const std::string& src_pad_name,
             BaseNode* dst, const std::string& dst_pad_name,
             MediaType hint_type = MediaType::CONTAINER);

    // Build 阶段完整校验
    // 1. 拓扑排序（Kahn 算法），结果存入 topo_order_
    // 2. 环路检测：topo_order_.size() != nodes_.size() 说明图中存在环，报错
    // 3. 孤立节点检测：与环路检测无关，需要单独遍历——
    //    检查每个节点是否既不是任何 Edge 的 src_node，也不是任何 Edge 的 dst_node，
    //    这种节点入度、出度均为 0，会被 Kahn 算法正常排进 topo_order_，
    //    不会被环路检测捕获，必须单独检查并返回 false
    //
    // 注：TemplateCaps 兼容性检查已在 link() 阶段逐条完成，build() 不再重复。
    bool build();

    // Ready 阶段：按拓扑顺序对每个节点依次执行三步
    // 步骤1：node->onReady()          初始化资源（DemuxNode 在此校验流是否存在）
    // 步骤2：createQueuesForNode()     为此节点所有 SrcPad 的 Edge 创建 BoundedQueue
    // 步骤3：node->onStreamInfo()      发送 CapsEvent，此时 Queue 已存在可以写入
    // 三步穿插进行，保证每个节点发 CapsEvent 时下游的 Queue 已就绪
    // ready() 具备事务语义：失败时按拓扑逆序调用已 touched 节点的 onStop()，
    // 并 reset 已创建的 Edge Queue；返回 false 时 Pipeline::play() 不进入 RUNNING。
    bool ready();

    // 访问拓扑排序结果
    const std::vector<BaseNode*>& topoOrder() const { return topo_order_; }

    // flush 所有 Edge Queue（Pipeline::stop() 使用）
    void flushAllQueues();

private:
    std::vector<std::unique_ptr<BaseNode>>  nodes_;
    std::vector<std::unique_ptr<Edge>>      edges_;
    std::vector<BaseNode*>                  topo_order_;

    // 静态 Caps 检查：检查两端 TemplateCaps 是否有交集
    bool checkCapsCompatibility(const TemplateCaps& src, const TemplateCaps& dst);

    // 为此节点所有 SrcPad 对应的 Edge 创建 BoundedQueue（固定容量 8）
    // 注：调整到实际正确容量是各节点在 onStreamInfo() 里调用全局的
    // selectQueueCapacity()（见 3.4 节 Edge）自行 resize 完成的，不在这里做
    void createQueuesForNode(BaseNode* node);
};
```

### 6.3 link 的执行流程

```
Graph::link(src_node, "out_0", dst_node, "in", hint_type)

① 查找 src_node 上名为 "out_0" 的 SrcPad
   存在 → 检查是否已被占用（isConnected()），已占用则返回 false
   不存在 → 调用 src_node->requestSrcPad("out_0", hint_type)
           节点自己决定是否允许创建（类型不合法或与已有输出冲突则返回 nullptr）
           返回 nullptr → link 失败，返回 false

② 对 dst_node 上的 SinkPad "in" 执行同样的查找/请求流程

③ 静态 Caps 检查：src TemplateCaps 与 dst TemplateCaps 有无交集，无交集返回 false

④ 创建 Edge（此时 Queue 尚未创建，Ready 阶段创建）
⑤ 设置 Pad::edge_ 指针（双向）
⑥ 返回 true
```

节点构造时已经声明好的固定 Pad（比如 TransformNode 唯一的 SinkPad "in"）在第一次 `link()` 时就会被直接找到，不会触发 `request*Pad`；只有分叉场景（同一节点多次作为 src 被连接）或 DemuxNode/MuxNode 的多路 Pad，才会真正调用到 `request*Pad`。

**孤立节点检测的具体实现**（独立于环路检测，必须单独遍历）：

```cpp
bool Graph::build() {
    // ... 拓扑排序（Kahn 算法）...

    // 环路检测
    if (topo_order_.size() != nodes_.size()) {
        return false;
    }

    // 孤立节点检测：单独遍历，不能依赖上面的环路检测结果
    // 孤立节点入度=0、出度=0，会被 Kahn 算法正常排入 topo_order_，
    // 不会触发 topo_order_.size() != nodes_.size()
    std::unordered_set<BaseNode*> connected;
    for (auto& edge : edges_) {
        connected.insert(edge->src_node);
        connected.insert(edge->dst_node);
    }
    for (auto& node : nodes_) {
        if (!connected.count(node.get())) {
            return false;
        }
    }
    return true;
}
```

### 6.4 Ready 阶段流程

```
Graph::ready()

对每个节点按拓扑顺序，依次执行：

步骤1：node->onReady()
   → Source / DemuxNode：打开设备/文件，探测流信息，记录 CapsEvent 内容
     DemuxNode 在此对照 link() 阶段已创建的 Pad，校验文件里是否存在对应的流
   → TransformNode / SinkNode：基础初始化，等待后续 CapsEvent
   → 返回 false 时触发 ready() 回滚：按拓扑逆序调用已 touched 节点的 onStop()，
     并 reset 已创建的 Edge Queue，然后 ready() 返回 false

步骤2：createQueuesForNode(node)
   → 为此节点所有 SrcPad 对应的 Edge 创建 BoundedQueue（固定容量 8）
   → 容量 8 足够传递 CapsEvent，此时数据流尚未开始，不会有真实数据

步骤3：node->onStreamInfo()
   → Source / DemuxNode：先 resize SrcPad 的 Edge Queue 到正确容量，再发 CapsEvent
     （DemuxNode 需对每个 SrcPad 分别 resize，video 和 audio 容量不同）
   → TransformNode：收到 CapsEvent（从 SinkPad Queue 中取出），初始化处理器
     （DecodeNode 根据 CapsEvent 初始化 AVCodecContext）
     （VideoRenderNode 根据 CapsEvent 初始化 SDL 窗口和纹理）
     初始化完成后，resize 自己 SrcPad 的 Edge Queue，再发输出 CapsEvent
     然后构造自己的输出 CapsEvent，推入下游 Queue
   → 返回 false 时触发 ready() 回滚：按拓扑逆序调用已 touched 节点的 onStop()，
     并 reset 已创建的 Edge Queue，然后 ready() 返回 false

任一步骤失败，ready() 立即返回 false，Pipeline::play() 检测到后停止
```

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

这两种用法也可以同时使用——比如主线程调用 `waitEOS()` 阻塞等待，另一条监听路径（信号处理、
另一个线程的事件循环等）检测到退出意图后调用 `stop()`。这两条路径谁先触发不确定，`stop()`
内部的 CAS 保护保证不管谁先到，只有一次真正的清理会被执行，另一次安全地什么都不做。

**waitEOS() 流程**：
1. 阻塞在 `eos_cv_` 上
2. 条件：`active_sink_count_ == 0`（所有 Sink 正常 EOS）、`error_occurred_`（有节点报错），
   或者 `state_` 已经不是 `RUNNING`（说明已经被外部 `stop()` 打断）
3. 唤醒后调用 `stop()`（已经停止过的情况下，`stop()` 内部 CAS 直接返回，不会重复执行）

**stop() 流程**：
1. CAS：仅当 `state_ == RUNNING` 时才能切换到 `STOPPING`，否则直接返回（已经停止/正在停止）
2. 设置所有节点 `stop_requested_.store(true)`
3. flush 所有 Edge Queue，唤醒阻塞中的节点线程
4. join 所有节点线程
5. 停止 MessageBus 监听线程（`bus_running_ = false`，notify，join）
6. 按拓扑逆序调用 `onStop()` 释放资源
7. `state_ = STOPPED`，notify `eos_cv_`

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

    // 2. flush 所有 Edge Queue，唤醒阻塞中的节点线程
    graph_.flushAllQueues();

    // 3. join 所有节点线程（节点线程退出前做数据层面收尾）
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
2. flush 所有 Edge Queue
3. join 所有节点线程（按启动顺序依次退出）
4. 停止 MessageBus 监听线程
5. 按拓扑逆序调用 onStop()

MessageBus 监听线程最后停止，保证节点退出过程中 postMessage 的消息（如 EOS、WARNING）仍能被处理。

**为什么 bus 必须早于 `graph_.ready()`**：`onReady()` / `onStreamInfo()` 可能执行真实设备/文件初始化（DemuxNode 打开 URL、DecodeNode 调 `avcodec_open2` 等），失败时会 `postMessage(ERROR, "...")`。bus 若尚未启动，消息只落到 queue 无人消费，`last_error_` 永远为空，§7.3 承诺的 `pipeline.lastError()` 就不可用。

---

## 8. Caps 两阶段协商

### 8.1 第一阶段：静态协商（Build 时）

每个 Pad 声明 TemplateCaps（支持的 MediaType 集合）。`link()` 时检查两端 TemplateCaps 是否有交集，无交集则 `link()` 返回 false。

各节点 TemplateCaps 声明：

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

### 8.2 第二阶段：动态协商（Ready 时）

Ready 阶段按拓扑顺序初始化节点，Source / DemuxNode open 之后发出 CapsEvent，顺流传递：

```
[步骤1] DemuxNode::onReady()
  → avformat_open_input，获取真实参数
  → 将 CapsEvent 内容记录到成员变量，暂不发送

[步骤2] Graph 为 DemuxNode 所有 SrcPad 的 Edge 创建 Queue（容量 8）

[步骤3] DemuxNode::onStreamInfo()
  → resize video_0 的 Edge Queue 到 selectQueueCapacity(VIDEO_ENCODED)
  → sendCapsEvent("video_0", { VIDEO_ENCODED, codec_id=H264, width=1920, height=1080, extradata=... })
    → CapsEvent 推入 DecodeNode 的 SinkPad Queue

[步骤1] DecodeNode::onReady()
  → 无特殊操作（等待 CapsEvent）

[步骤2] Graph 为 DecodeNode 所有 SrcPad 的 Edge 创建 Queue（容量 8）

[步骤3] DecodeNode::onStreamInfo()
  → 从 SinkPad Queue 取出 CapsEvent
  → 用 codec_id 查找解码器：avcodec_find_decoder(caps.codec_id)
  → 分配上下文：avcodec_alloc_context3(codec)
  → 填充 extradata（SPS/PPS）到 ctx_->extradata
  → avcodec_open2(ctx_, codec, nullptr)
  → ⚠️ 必须等 avcodec_open2() 完成后，才能从 ctx_ 中读取输出参数
     输出的 pix_fmt、width、height、sample_fmt 由解码器决定，不能直接透传输入 CapsEvent 的值
     例如：输入 H264 1920x1080 → 解码器可能输出 YUV420P 1920x1080（以 ctx_ 实际值为准）
  → 从 ctx_ 读取实际输出参数，构造输出 CapsEvent
     视频：{ VIDEO_RAW, pix_fmt=ctx_->pix_fmt, width=ctx_->width, height=ctx_->height }
     音频：{ AUDIO_RAW, sample_fmt=ctx_->sample_fmt, sample_rate=ctx_->sample_rate, channels=ctx_->ch_layout.nb_channels }
  → resize out_0 的 Edge Queue 到 selectQueueCapacity(out_caps.media_type)
  → sendCapsEvent("out_0", out_caps)

[步骤3] VideoRenderNode::onStreamInfo()
  → 从 SinkPad Queue 取出 CapsEvent
  → 用 width / height / pix_fmt 初始化 SDL 窗口和纹理
```

DecodeNode::onStreamInfo() 参考实现：

```cpp
bool DecodeNode::onStreamInfo() override {
    // 1. 从 SinkPad Queue 取出上游的 CapsEvent
    auto item = sink_pads_[0]->popBlocking();
    const CapsEvent& in_caps = ...;

    // 存储收到的 CapsEvent
    negotiated_caps_["in"] = in_caps;

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
        pipeline_->bus()->post(Message{MessageType::ERROR, this, "DecodeNode: failed to open decoder"});
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

    // 4. resize SrcPad 的 Edge Queue 到正确容量，再推给下游
    src_pads_[0]->edge()->queue->resize(selectQueueCapacity(out_caps.media_type));
    sendCapsEvent("out_0", out_caps);
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

auto* demux   = pipeline.addNode<DemuxNode>("demux");
auto* vdecode = pipeline.addNode<DecodeNode>("vdecode");
auto* adecode = pipeline.addNode<DecodeNode>("adecode");
auto* vrender = pipeline.addNode<VideoRenderNode>("vrender");
auto* aplay   = pipeline.addNode<AudioPlayNode>("aplay");

demux->setUrl("input.mp4");

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
auto* mux_r   = pipeline.addNode<MuxNode>("mux_rtsp");
auto* mux_f   = pipeline.addNode<MuxNode>("mux_file");
auto* rtsp    = pipeline.addNode<RTSPPushNode>("rtsp");
auto* file    = pipeline.addNode<FileSinkNode>("file");

vcap->setDevice("/dev/video0");
acap->setDevice("hw:0,0");
venc->setCodec("libx264");
aenc->setCodec("aac");
mux_r->setFormat("flv");
mux_f->setFormat("mp4");
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

auto* demux = pipeline.addNode<DemuxNode>("demux");
auto* mux   = pipeline.addNode<MuxNode>("mux");
auto* rtsp  = pipeline.addNode<RTSPPushNode>("rtsp");

demux->setUrl("input.mp4");
mux->setFormat("flv");
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

EOS 从 Source / DemuxNode 发出，顺流传递到所有 Sink，Pipeline 在所有 Sink 收到 EOS 后停止。

```
DemuxNode 读到文件末尾
  → sendEOSDownstream()
  → EOS 进入 video_0 SrcPad 对应的 Edge Queue
  → EOS 进入 audio_0 SrcPad 对应的 Edge Queue

DecodeNode 的 runLoop 收到 EOS
  → onEvent(EOSEvent{}) → sendEOSDownstream() → 继续往下传

VideoRenderNode 收到 EOS
  → postMessage(MessageType::EOS, "")（SinkNode 固定只有一个 SinkPad，收到即上报）

AudioPlayNode 收到 EOS
  → postMessage(MessageType::EOS, "")（SinkNode 固定只有一个 SinkPad，收到即上报）

Pipeline::messageBusLoop 收到 EOS
  → active_sink_count_--
  → 如果所有 Sink 都已 EOS：notify eos_cv_

Pipeline::waitEOS
  → 等待 eos_cv_
  → 调用 stop()
```

---

## 13. 已知问题与后续优化

### 第一阶段已知限制

| 问题 | 描述 | 计划 |
|------|------|------|
| Buffer 全量拷贝 | 分叉时每路 clone 一份，内存压力大 | 后续引入引用计数零拷贝 |
| 多路非阻塞丢帧无反馈 | tryPush 失败时静默丢帧 | 后续加入丢帧计数上报 MessageBus |
| MuxNode 多路等待 | 所有 SinkPad 的 Queue 共享 mux_cv_，BoundedQueue 支持注册外部 notify 回调 | 已设计，实现时注意 Queue 注册回调的线程安全 |
| 孤立节点 | 与环路检测是两个独立的检查：遍历所有节点，找出既没有任何输入边也没有任何输出边的节点（这种节点会被 Kahn 算法正常排入 topo_order_，不会被环路检测捕获） | Build 阶段单独遍历报错，明确说明哪个节点未连接 |
| AV Sync 无自适应 | 丢帧阈值固定 50ms | 后续根据实际延迟动态调整 |
| 队列所有权约定隐式 | `BoundedQueue::tryPush(QueueItem)` / `pushBlocking(QueueItem)` 的"进来即归我"约定靠注释表达，caller 违约会 double-unref。二阶段收尾轮已在 `pushToDownstream` 中修好一处、在 `test_bounded_queue_try_push_full` 中修好一处 | 后续接口改为 `QueueItem&&` 让类型系统承载所有权，同时把 `TransformNode::process` 的 `std::vector<Buffer*>&` 出参一并改为 `std::vector<BufferRef>&` |

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
│   │   ├── Edge.h               # Edge，持有 BoundedQueue
│   │   ├── BoundedQueue.h       # 有界队列，阻塞/非阻塞 API
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
