
---

## OutputRoute / 静态可靠背压重构

### 为什么替换每 Edge 独立 BoundedQueue

旧模型由上游逐 SrcPad push：单路使用 `pushBlocking()`，分叉使用 `tryPush()`。它存在两个不可接受的结果：可靠编码 Packet 会在慢分支满时静默丢失；若全部顺序阻塞，某个慢分支又会立即锁住同一生产线程负责的其他输出。

正式模型改为：**每条逻辑流一个静态、有界、多订阅者 OutputRoute；上游 publish 一次，每条 Edge 持有独立 RouteSubscription 游标。**

### 核心合同

- 拓扑和全部 Subscription 在 `link/build` 阶段一次性建立；`build()` 成功后 Route seal，Running 期间不允许增删节点、Pad、Edge 或订阅者。
- Source/Transform 同源分叉 Pad 共享同一 Route；Demux VIDEO/AUDIO 是不同 Route，同类型多个 Pad 是同一路最佳 Track 的静态订阅分叉。
- Route Entry 只保存一份 `QueueItem`；Buffer 分叉复制 `BufferRef` 句柄、共享只读 payload，不再调用 `clone()`。
- 订阅者 acquire 得到不可拷贝的 RAII `RouteDelivery`；节点完成 `consume/process/writePacket` 以及相应输出 publish 后才显式 ack。
- Delivery 未 ack 析构只撤销 in-flight 状态，不推进游标；同一订阅者可重新 acquire 同一项。
- 所有可靠订阅者都 ack 后，Route 才回收 Entry；最慢游标决定 retained 水位。
- 达到硬条目容量时 `publishBlocking()` 等待；最慢订阅者 ack 释放空间后唤醒 publisher，背压沿节点“不再读取输入”逐级向上游传导。
- Caps、Buffer、EOS 共用 Route 有序日志。每个 Sink 处理并 ack EOS 后上报完成；Pipeline 等全部 Sink 完成后才 stop/cancel，保证其他静态订阅者不会被提前截断。
- stop/error 使用 `cancelAllRoutes()` 清空未完成日志并唤醒 publisher/subscriber/Mux 等待；cancel 是强制停止，不代替自然 EOS。

### Buffer 只读和生命周期

`BufferRef` 现在持有 `const Buffer*`，消费接口收紧为：

```cpp
consume(const Buffer*);
process(const Buffer*, ...);
writePacket(const Buffer*, ...);
```

生产者仍在发布前构造可写 Buffer；发布后只能通过 const 输入访问。Route Entry 回收与 payload 释放相互独立：游标/ack 决定日志 Entry 何时移除，最后一个 BufferRef 的原子 unref 决定底层 Buffer 何时销毁。

这实现的是框架分叉传输零拷贝；FFmpeg AVPacket/AVFrame 与 Buffer 之间仍可能复制。

### Graph / Pad / Edge 迁移

- SrcPad 绑定一个 `shared_ptr<OutputRoute>`；分叉 Pad 共享已有 Pad 的 Route。
- Edge 不再拥有 BoundedQueue，只拥有 `RouteSubscription`。
- SinkPad 暴露 `acquireBlocking/tryAcquire/peek`；Mux readiness 和最小 DTS 选择都相对于自身 Subscription 游标。
- Graph::link 在 Route 上创建 Subscription 后放入 Edge；build 完成拓扑校验后 seal 全部 Route。
- Graph::ready 不再创建/销毁 Edge Queue；Ready 失败先 cancel Route，再逆拓扑 onStop。
- Pipeline::stop 将 `flushAllQueues()` 替换为 `cancelAllRoutes()`。

### 容量与后续边界

当前容量仍按 MediaType 使用条目数：VIDEO_RAW=4、AUDIO_RAW=50、ENCODED=128、CONTAINER=32。后续可以在不改变 Route/cursor/ack 模型的前提下增加字节硬上限、节点级内存预算和只用于监控的高低水位。

本轮不支持 drop policy、动态拓扑或 lock-free 实现。所有订阅者均为静态可靠订阅者。

---

## SDL VideoRender 线程亲和与窗口停止请求

### 决策背景

SDL 视频资源具有线程亲和性。VideoRender 的 SDL 视频资源由工作线程独占完整生命周期：在该线程内完成初始化、使用、销毁

### SDL 视频资源生命周期合同

+ onStreamInfo()：只接收并保存 Caps，不初始化 SDL VIDEO，不创建 Window / Renderer / Texture
+ Running：工作线程初始化 SDL_INIT_VIDEO，创建并使用 Window、Renderer、Texture
+ 退出前：销毁 Window / Renderer / Texture，调用 SDL_QuitSubSystem(SDL_INIT_VIDEO)

### SDL 事件范围

VideoRender 工作线程消费 SDL 事件队列，但当前只处理属于自身窗口的`SDL_EVENT_WINDOW_CLOSE_REQUESTED`

### STOP_REQUESTED 合同

VideoRenderNode 检测到自身窗口关闭后调用 `postMessage(MessageType::STOP_REQUESTED, ...)`；
`postMessage()` 统一设置该节点的 `stop_requested_`，结束消费循环。Pipeline MessageBus 线程收到消息后只设置 `stop_requested_by_node_` 并唤醒 `eos_cv_`

---

## 音频三件事：时钟、背压、EOS drain

### 设计锚点

音频问题的核心不是"找一个更好的 SDL API"，而是定义清楚"音频播放进度"的语义：

- **权威**：SDL 设备真实消费进度是主时钟的唯一来源，不是写入量、不是 `SDL_GetAudioStreamAvailable`、也不是墙钟。
- **锚定**：主时钟锚定首个有效音频 Buffer 的 PTS；无音频时退化为视频首帧一次性锚定墙钟。
- **偏移**：音频路径含一段不可观测、有界恒定的硬件缓冲领先；已知不精确，接受不校准。
- **速率**：两次采样之间用墙钟插值；背压阻塞期间插值继续走，因为设备仍在按消费速率前进。

### 音频 Clock：用 `SDL_GetAudioStreamQueued` 而非 `Available`

`SDL_GetAudioStreamAvailable` 是输出侧（设备格式）可读字节数，与提交账本量纲不一致；一旦 SDL 内部重采样，相减就失真。`SDL_GetAudioStreamQueued` 返回输入侧尚未被设备读取的字节数。

音频播放保留完整的输出提交账本，并显式保存首个有效 PTS 的提交基线：

```text
submitted_frames_
    所有成功提交给 SDL 的输出 PCM 总帧数
frames_before_anchor_
    首个有效 PTS 所在 Buffer 提交前，已经提交的输出帧数
anchor_pts_us_
    首个有效 PTS
```

锚定后统一按以下公式计算：

```text
queued_frames = SDL_GetAudioStreamQueued() / bytes_per_sample
consumed_from_anchor =
    submitted_frames - frames_before_anchor - queued_frames
audible = max(0, consumed_from_anchor - 一个设备周期)
clock = anchor_pts_us + audible / sample_rate
```

- 锚定前的 NOPTS Buffer 照常提交并计入 `submitted_frames_`，但不更新 Clock；
- `consumed_from_anchor < 0` 表示设备尚未消费到锚定帧，Clock 保持未锚定，前缀窗口内允许短暂不同步；
- `consumed_from_anchor >= 0` 后才允许更新 Clock；
- 锚定后的 NOPTS 继续计数，后续有效 PTS 不重新锚定；
- 整条音频流没有有效 PTS 时，Audio Clock 保持未锚定；
- `onDrain()` 等待 `queued_frames` 清空期间也持续刷新 Clock，覆盖负值转为非负值的边界；
- 正常 `consume()` 与 swr drain 尾部统一经过同一个 SDL 提交/记账入口，避免账本分叉。

当 `consumed_from_anchor` 位于 `[0, device_period]` 时，采样点上的 `audible` 被钳为 0，位置不早于 `anchor_pts_us`；两次采样之间仍按墙钟插值，这是设备周期补偿造成的正常启动行为，不是卡顿。

该公式假设 swr 不改变采样率，输出帧与媒体 PTS 的映射为 1:1；真正重采样时必须重新定义帧数映射。

### 音频 Clock：无条件重锚

Clock 的 `setAudioPosition` 无条件重锚（`base_pts = pts; base_wall = now; anchored = true`），不靠"拒绝落后样本"换取单调。因为：

- 正常播放时 `consumed` 样本本身单调且与墙钟同速；
- 欠载时设备真实停播，样本如实停滞；若用"只向前"门槛，会把墙钟外推当权威，反而在欠载时让时钟虚构前进。

`anchorOnce` 仅用于无音频场景，由 VideoRenderNode 首帧调用一次。

### 提交背压：双阈值迟滞闸门

SDL AudioStream 内部缓冲对 App 无界；AudioPlayNode 在每次 `SDL_PutAudioStreamData` 之前检查 `SDL_GetAudioStreamQueued`，若超过高水位则取消感知地轮询到低水位再放行。阈值以"设备周期 P"为单位推导：

```
P    = sample_frames / device_freq (SDL_GetAudioDeviceFormat 查询，失败按 10ms 兜底)
LOW  = N_low  × P  (N_low=3，小周期设备加 ms 下限兜底)
HIGH = LOW + N_band × P (N_band=8)
```

这样常量变成自适配的，不再是脱离硬件的魔法毫秒。迟滞带 N_band 保证每次开闸成批提交，避免"等-put-等-put"的逐块抖动。

背压闸门位于 `consume()` 内、ack 之前，因此"晚 ack"把背压沿 OutputRoute 逐级传导到上游。被 `stop_requested_` 打断时直接返回，不 put、不更新时钟。

### EOS drain：SinkNode 生命周期钩子

普通 `SinkNode::runLoop` 收到 EOS 后直接 `postMessage(EOS)`，把"输入耗尽"当作"完成"。这对 AudioPlayNode 不够：输入 EOS 只说明没有新 Buffer，不说明设备已播完此前提交的 PCM。

新流程：

```
ack(EOS) → onDrain() → postMessage(EOS)
```

- `ack` 先释放上游 Route，drain 等待期间不占用背压窗口；
- `onDrain()` 默认空实现，VideoRenderNode 不受影响；
- AudioPlayNode 的 `onDrain()` 依次：
  1. swr 尾部排空（按 `swr_get_delay` + `av_rescale_rnd` 算输出容量）；
  2. `SDL_FlushAudioStream` 把 SDL 内部残留转换出来；
  3. 取消感知等待 `SDL_GetAudioStreamQueued() == 0`；
  4. 再等 3 个设备周期覆盖后端缓冲尾音（(c) 不可观测，按上界等待）。
- `onDrain()` 内任一 SDL/swr API 失败或 `stop_requested_` 置位，立即返回，且不再上报 EOS。

### 视频消费侧切换

VideoRenderNode 不再保留私有 steady_clock 路径，统一读 `pipeline_->clock()->getPositionUs()`：

- 未锚定：立即呈现；无音频时 `anchorOnce(frame_pts)` 锚定。
- 已锚定：超前等待、落后立即追帧。丢帧策略本轮不实现。

这样纯视频路径的首帧 PTS 锚定被收进了 Clock，视频节点不再持有独立计时器。

### 启动时序不对称（暂缓）

当前两个呈现 Sink 的首次输出延迟不对称：音频设备在 Ready 阶段即 resume，视频窗口创建需要数秒。这导致 A/V 文件起播时音频先响、视频追帧。完整修复需要启动栅栏（start barrier）：所有共享主时钟的呈现型 Sink 都备好首份输出后再同时释放。该设计已讨论但暂缓，待后续实现；吞吐型 Sink（FileSink、RTSPPush 等）不参与栅栏，因为它们不锚定主时钟。

---

## Transform 输出所有权类型化

### 问题背景

`TransformNode::process` 原先通过 `std::vector<Buffer*>` 返回 0 到 N 个新输出。裸指针容器只靠调用约定表示“这些 Buffer 尚未发布且由 Transform 持有”，无法自动覆盖提前退出：

- `process()` 产出后若 `stop_requested_` 已置位，runLoop 直接退出，全部输出尚未进入发布入口；
- 多输出发布到一半时若 Route 被 cancel，当前发布失败后 runLoop 退出，尚未遍历的尾部输出仍留在裸指针容器中。

这两条停止/取消路径都会泄漏未发布的输出 Buffer；输入侧 `RouteDelivery` 与已经进入 `pushToDownstream()` 的输出本身没有问题。

### 正式所有权合同

`TransformNode::process` 输出改为：

```cpp
process(const Buffer* input, std::vector<BufferRef>& outputs);
```

- 子类创建新 Buffer 后立即放入 `BufferRef`，不再把拥有型裸指针跨控制流保存在 outputs 中；
- `outputs` 持有全部尚未发布的输出，process 后 stop/error 时由 vector 析构统一释放；
- 正常发布后对应元素被 move 为空，复用 vector 容量不保留 payload 所有权。

唯一 Buffer 发布入口改为：

```cpp
pushToDownstream(BufferRef&& buffer);
```

- 调用方必须显式 `std::move`，表示无条件交出当前发布引用；
- 入口立即移动接管引用，不保留 `Buffer*` 重载；
- publish 成功时引用进入 Route Entry；Route 缺失、输出歧义、无订阅或 cancel 时也由入口内 RAII 释放；
- 部分发布后 cancel 时，已发布项由 Route cancel 释放，当前失败项由发布入口释放，未遍历尾部仍由 outputs 释放。

Source、Mux 和 Decode EOS flush 等其他 Buffer 生产路径同步改为先用 `BufferRef` 接住新 Buffer，再显式移动到同一个发布入口。

---

## SDL 外部线程 TLS 与进程级生命周期 owner

### 问题背景

Pipeline 通过 `std::thread` 创建 VideoRender 和 AudioPlay worker。SDL3 要求非 SDL 创建的线程在调用 SDL API 后、线程退出前调用 `SDL_CleanupTLS()`；此前两个 worker 都没有执行该清理。与此同时，节点仅调用 `SDL_QuitSubSystem()`，但 SDL3 明确要求应用在整个进程生命周期末尾仍调用一次 `SDL_Quit()` 清理基础设施和进程级状态。

### 线程 TLS 合同

- VideoRender worker 在所有 SDL VIDEO 资源销毁、`SDL_QuitSubSystem(SDL_INIT_VIDEO)` 后调用 `SDL_CleanupTLS()`；初始化失败、EOS、ERROR、窗口关闭和 stop 都经由同一 runLoop 尾部。
- AudioPlay 保持既有资源生命周期不变：Ready 调用线程初始化 AUDIO/AudioStream，worker 负责消费和 drain，join 后 `onStop()` 销毁 AudioStream 并退出 AUDIO 子系统。仅在音频 worker 的 `SinkNode::runLoop()` 返回后调用 `SDL_CleanupTLS()`。
- 不把 Audio 的 init/use/destroy 线程归属重构混入本轮。

### 进程级 owner 合同

`Pipeline` 构造时调用：

```cpp
SDL_Init(0);
```

它只建立 SDL 基础设施生命周期，不带 VIDEO/AUDIO 等子系统 flag，不持有或管理任何具体子系统。`Pipeline` 析构时先调用 `stop()`，完成 worker join 与节点 `onStop()`，随后调用一次 `SDL_Quit()`：

```text
Pipeline stop → worker join → node onStop → SDL_Quit()
```

因此节点仍独占各自的 `SDL_InitSubSystem` / `SDL_QuitSubSystem`：VideoRender worker 首次调用 `SDL_InitSubSystem(SDL_INIT_VIDEO)`，该线程仍被 SDL 认定为视频主线程；AudioPlay 继续管理 AUDIO 子系统。Pipeline 永不触碰 VIDEO/AUDIO flag。

### 验证与第三方基线

真实 ASAN player 在 `SDL_Init(0)` 后，VideoRender worker 仍报告 `SDL_IsMainThread() == true`。SIGINT 中断后项目侧 Buffer/Route/Transform/SDL TLS 泄漏均消失。

Linux/X11 下 SDL3 software renderer 的 window surface 会内部启用 GL texture framebuffer，最小独立程序完整销毁 SDL 资源并调用 `SDL_Quit()` 后仍报告 Mesa/GLX 1464B/16 allocations；强制直接 X11 framebuffer 则产生 X11/XKB 33066B/572 allocations。两者是第三方 GUI 基线，不为压低 LSAN 数字而更换 renderer/backend。框架单测保持无 suppression 严格运行，player 报告将这组基线与项目自身泄漏区分。

### 单进程单存活 Pipeline 约束

`Pipeline` 是一条实例级媒体管线，持有自己的 Graph、节点工作线程、Clock 和 MessageBus；但当前它的构造/析构直接管理 SDL 全局基础设施的 `SDL_Init(0)` / `SDL_Quit()`。`SDL_Quit()` 是强制全局拆除，若允许两个 Pipeline 并存，先析构的一方会在另一方仍可能使用 SDL 时拆除全局运行时。

因此当前正式限制为：**同一进程同一时刻至多允许一个存活的 Pipeline 实例**。顺序创建、销毁多个 Pipeline 是允许的；两个 Pipeline 重叠存活不受支持。当前不为未实现的多 Pipeline 模型引入进程级引用计数、lease 或 Pipeline manager。

---

## Running 有序动态 Caps 与 Decoder 首帧格式定案

### 决策背景

旧模型要求 Ready 阶段沿 Route 一次性传递 Caps，随后 Running 只传 Buffer。它无法表达 FFmpeg Decoder 的实际行为：H.264 等 decoder 在 `avcodec_open2()` 后可能仍无 pix_fmt，必须看到真实 AVFrame 才能确定 RAW 输出格式；同一流也可能在运行中出现新的格式边界。

用 Ready preroll 或“未知时猜 YUV420P”都不能建立可靠合同。因此 Caps 不再是启动期旁路消息，而是流内有序配置边界。

### 正式合同

- Build 阶段只用 TemplateCaps 做 MediaType 交集检查；Ready 只建立不依赖上游格式的资源，不收发 Caps。
- Running 阶段每条逻辑 Route 的序列为：

  ```text
  CapsEvent → Buffer* → CapsEvent → Buffer* → EOSEvent
  ```

  每份 Caps 必须完整、准确地解释其后至下一份 Caps 前的全部 Buffer；Buffer 在 active Caps 前到达或其 media_type 不一致都是协议错误。
- `QueueItem = variant<BufferRef, Event>` 同时是 Route 传输项与 Transform 的本地拥有型待发布序列。Caps、Buffer、EOS、Decoder delayed 输出和 EOS flush 都走同一个顺序/RAII 发布边界。
- 取消全局 `CapsEvent::isComplete()`。传输层只检查 Route 和 TemplateCaps 的 MediaType；格式字段是否充分由实际使用点判断：Decode 至少需 codec_id，VideoRender 需 RAW 视频 width/height/pix_fmt，AudioPlay 需完整受支持 RAW 音频格式，Mux 的容器字段由具体 `addStream()` 后端检查。
- `hasSameFormat()` 只比较 Caps 声明值，不能充当全局重配闸门。VIDEO_ENCODED 比较 codec/width/height/extradata；AUDIO_ENCODED 比较 codec/sample_rate/channel_layout/extradata。
- `ChannelLayout` 是可复制框架值类型，避免把含 FFmpeg 堆指针的 `AVChannelLayout` 直接嵌入 Caps。AUDIO_ENCODED 的 sample_rate/channel_layout 是可选提示：Decode 有效时写入 AVCodecContext，未知时留给 FFmpeg 从 extradata/bitstream 确定；Decode 从真实 AVFrame 生成的 AUDIO_RAW Caps 则必须完整。

### 节点边界

- Demux 在 Ready 探测并缓存 encoded Caps；worker 启动后先为每条 Route 发布 Caps，再发布 Packet。
- Decode 收到新的 encoded Caps 时，先把旧 decoder 的 delayed 输出放入同一有序 outputs，再替换 decoder context。Decode 不从 `avcodec_open2()` 后的 context 猜 RAW Caps；每个真实 AVFrame 在首帧或格式变化时先输出 RAW Caps、后输出 Frame Buffer。Transform 基类在 `onEOS()` 返回后统一追加唯一 EOSEvent。
- VideoRender 在 Running `onCaps()` 应用格式边界，仅支持紧密 YUV420P/YUVJ420P；尺寸变化时销毁旧 Texture，下一帧重建。
- AudioPlay 在 Ready 建固定 canonical SDL 提交端（S16 packed、设备派生 rate、stereo）。Running AudioRaw Caps 重配前先 drain 旧 swr 到 canonical 队列，再重建 input→canonical swr；不清空 SDL canonical 队列，水位、账本和 Clock 始终使用 canonical 帧量纲。
- Mux 只在全部输入初始 encoded Caps 到齐后发布 CONTAINER Caps、写 Header；Header 后 encoded Caps 重配明确报错。每个写入 Buffer 都必须已有同 Pad active Caps 且 media_type 匹配。

### 当前明确边界

该决策解决 Decoder 首帧格式未知、同 Route 有序 Caps、Video 尺寸变化和 Audio 输入重配；不包含 PTS discontinuity、Caps generation、非 YUV420P swscale，也不提前设计通用采集 Source 的 Caps 生产接口。Mux Header 后的 encoded Caps 拒绝是已确定的 Header 冻结合同，不属于待实现的运行期重配。默认 `SourceNode::capture() -> Buffer*` 无法表达有序 Caps，留待 V4L2/AudioCapture 的设备协商模型确定后处理。

---

## framerate 的 timing hint 语义更正

上一节把 framerate 列入 `hasSameFormat()` 的 VIDEO_ENCODED 声明比较，现予以更正：framerate 不改变 payload 布局，不要求重建 Texture、重建 swr、重新解释 Buffer 或 drain/reopen Decoder，因此不属于本框架的格式边界。

- `CapsEvent::framerate` 仅保留给 VIDEO_ENCODED，作为 Demux 提供给 Decode 的 nominal timing hint；Decode 用它推导输出 VIDEO_RAW Buffer 的 `duration`。
- VIDEO_RAW Caps 不携带 framerate；后续帧的实际时序由 Buffer 自己的 `pts` 与 `duration` 表达。
- `hasSameFormat()` 的 VIDEO_RAW 只比较 width/height/pix_fmt，VIDEO_ENCODED 只比较 codec_id/width/height/extradata；framerate 单独变化不产生 Caps 配置边界。
- 将来若某个消费者确实需要感知 nominal framerate 更新，应设计独立 timing property update 语义，不能复用 payload 格式 Caps 或令渲染资源重配。

---

## 启动 rendezvous 与动态视频丢帧

### 决策背景

音频和视频首个外部输出的准备延迟不对称：AudioPlay 可先获得 canonical PCM，而 VideoRender 仍可能在创建窗口、Texture 或上传首帧。若音频先 `SDL_PutAudioStreamData()`，Audio Clock 已经推进后视频才首次 Present，会造成视频天生落后。栅栏只应解决共同起跑，不能混入 NOPTS、Clock 锚定或释放后的 A/V 同步状态机。

### 启动 rendezvous 合同

- 当前一个 Pipeline 的唯一 `Clock` 即唯一同步域；本轮不提前建设多 Clock、多窗口或多音频 master 模型。
- 参与者在 Ready 成功时向 Clock 登记；Pipeline 在 `graph_.ready()` 成功后、启动任一 worker 前封闭人数。状态机为 `REGISTERING → WAITING → RELEASED`，任意阶段可被 `CANCELLED` 覆盖。
- AudioPlay 在首段 canonical PCM、首次 `SDL_PutAudioStreamData()` 前到达；VideoRender 在首帧完成 Texture 创建及上传、首次 `SDL_RenderPresent()` 前到达。栅栏不预填 SDL、不等待有效 PTS，也不改变既有 NOPTS 账本。
- 最后到达者释放全部等待者；单参与者到达即释放；无参与者在封闭时直接释放。首帧前自然 EOS 的已登记参与者在 `onDrain()` withdraw，自身不再成为其他参与者的启动前提。AudioPlay withdraw 后仍按既有 swr/SDL drain 收尾；若该尾部首次产生 canonical PCM，它面对已释放栅栏提交，当前不把该罕见尾部路径提升为第二轮 rendezvous。
- `stop()`、ERROR、STOP_REQUESTED 和 Ready/封闭失败均取消栅栏并唤醒等待者。取消是等待者的门闩，不是对已从 arrival 调用返回的 worker 与紧随其后的 SDL 外部调用的事务性撤销。
- 需要回到自身事件循环的参与者使用限时 arrival：`TIMEOUT` 只是一次等待结果，Clock 持久状态仍为 `WAITING`；调用者保留 `bool& arrived`，重试绝不重复计数。当前 VideoRender 每 10ms 轮询一次自身窗口关闭请求。

### 动态晚帧丢弃合同

VideoRender 统一以 Pipeline Clock 评价已锚定视频帧。`waitForPresentationTime(pts, duration)` 保持 bool 返回：`false` 在 `consume()` 中统一跳过渲染；SinkNode 通过 `stop_requested_` 决定是停止时不 ack，还是正常丢帧后 ack 并继续。

```text
remaining = frame_pts - clock_pos
threshold = max(Buffer.duration, 40ms)

remaining < -threshold
    → 当前帧过期，跳过 Texture 上传和 Present，dropped_frames++
remaining >= -threshold
    → 立即追帧呈现
```

- 无 PTS 或 Clock 未锚定时不丢帧；无音频时首帧仍 `anchorOnce()`，随后按同一 Clock 规则运行。
- `Buffer.duration == 0` 时阈值为 40ms；当前 duration 来自 encoded nominal framerate hint，尚不等于完整逐帧 VFR 时长模型。
- 不复刻 ffplay 的“确认后面还有下一帧才丢当前帧” lookahead，因为当前 Subscription 在持有 in-flight Delivery 时不能窥视后项；改变 Route in-flight/peek 合同不应混入本轮。
- 记录 rendered/dropped 计数但不逐帧输出日志；真实 MV 自然 EOS 验证为 `5703 rendered + 3 dropped = 5706` 解码帧。


---

## Decoder 时间戳、send/receive EAGAIN 与 VideoRender swscale 收尾

### 决策背景

第三阶段剩余的三个媒体兼容性缺口都已有明确边界，不需要再引入新的 Route、Caps 或 Buffer 所有权模型：

- Decode 输出 Buffer 以前只读取 `frame->pts`，遗漏 FFmpeg 在帧重排后提供的展示时间；
- `avcodec_send_packet()` 返回 `EAGAIN` 时以前直接报 ERROR，未遵守“先 receive drain、再重发同一 Packet”的 API 合同；
- Decoder 已经在 Running Caps 诚实发布真实 `pix_fmt`，但 VideoRender 只接受 YUV420P/YUVJ420P，其他格式虽不再被误读却不能播放。

### 时间戳与 EAGAIN 合同

- `Buffer::fromAVFrame()` 对视频和音频统一选择展示时间：优先 `frame->best_effort_timestamp`，其无效时保留 `frame->pts`，两者都无效才传递 `AV_NOPTS_VALUE`。时间基换算仍由调用方传入的 frame time base 完成；Buffer 不继续猜测时间戳。
- `DecodeNode` 把 `avcodec_send_packet()` 和紧随其后的 `receive_frame()` 收敛为同一 helper。首次 send 返回 `EAGAIN` 时，先 drain 现有可输出帧到当前本地 `outputs` 序列，再以**同一个仍存活的 AVPacket**重发一次；重发仍失败才上报 ERROR。
- 普通输入 Packet、输入 Caps 重配前对旧 decoder 的 null-packet flush、EOS null-packet flush全部复用该 helper，避免任一路径重新把 `EAGAIN` 误判为致命错误。Packet 仅在 helper 完成后释放。

### VideoRender 非 YUV420P 转换合同

- Decode 输出 Caps 始终忠实保留真实 `width`、`height`、`pix_fmt`；不把上游格式伪装成 YUV420P，也不在 Buffer 层做面向特定 Sink 的转换。
- VideoRender 在 `onCaps()` 根据真实输入格式选择本地消费路径：紧密 YUV420P/YUVJ420P 直传 SDL IYUV；其他 CPU 可访问格式建立新的 `SwsContext` 和紧密 YUV420P 输出缓冲。所有 replacement 构造成功后才替换旧转换资源。
- 每帧 `consume()` 依据 active Caps 用 FFmpeg image 工具从紧密 Buffer 重建 source plane/linesize；非直传格式在此执行 `sws_scale()`，随后与直传格式共用 SDL IYUV 上传、Texture 和 Present 路径。晚帧先完成 Clock 判断，过期帧不浪费转换成本。
- 像素格式重配只替换 swscale context/缓冲，尺寸变化才销毁并重建 SDL Texture。转换资源和 SDL 视频资源同属 VideoRender worker，在 worker 统一退出尾部释放。
- 本轮目标是让 NV12、YUV422P、YUV444P、常规 10-bit、RGB 等 CPU 可访问格式可转换播放；Caps 当前不承载 color range、matrix、primaries、transfer 或 HDR metadata，因此不宣称完成严格色彩管理或 HDR 呈现。

---

## SourceNode 有序生产模型与构造期输出能力声明

### 决策背景

旧 `SourceNode::capture() -> Buffer*` 只能把一份裸 Buffer 作为单一输出表达，无法表达采集设备在首帧前发布真实格式，或未来同类格式边界的 `Caps → Buffer → Caps → Buffer`。采集设备是持续、无界的实时输入，不具备文件读取到末尾式的自然 EOS：其停止只能来自外部 stop/cancel，设备失败则是 ERROR。与此同时，普通 Source/Transform 的 `requestSrcPad()` 曾在不存在首个 SrcPad 时用 `Graph::link()` 传入的 `hint_type` 临时构造 `TemplateCaps`；这让下游连接请求反向定义上游节点能力，破坏了 TemplateCaps 的静态声明语义。

### Source 有序生产合同

- `SourceNode` 改用与 `TransformNode` 同构的钩子：`produce(std::vector<QueueItem>& outputs)`。具体 Source 每轮只向 `outputs` 放入有序 `CapsEvent` / `BufferRef`；实际采集实现应在钩子内部阻塞至获得新项目、观察到外部 stop/cancel 或检测到 ERROR。
- 采集 Source 不生产 `EOSEvent`，基类也不追加 EOS：实时设备没有自然 EOF。窗口关闭、SIGINT、应用调用 `Pipeline::stop()` 均走 stop/cancel；设备拔出、DQBUF/协商等后端失败必须先 `postMessage(ERROR)`，不得伪装为 EOS。
- 基类逐项使用 `publishOutputItem()` 发布 `outputs`，所以 Caps、Buffer 与 Transform 输出一样经过同一个共享 Route、可靠背压和 RAII 所有权边界。发布中断后已进入 Route 的项由 Route 管理，当前项与未遍历尾项由 `outputs` 自动释放。
- Source 没有输入 Route 的 active Caps，故基类维护最近一次**成功发布**的输出 Caps：Buffer 在首份 Caps 前产生，或其 media_type 与该 Caps 不一致，立即 ERROR，非法 Buffer 不得进入 Route。后续同类型 Caps 可在对应首 Buffer 前发布；跨 MediaType 仍由 OutputRoute 的 actualType 冻结合同拒绝。
- `produce()` 允许本轮输出为空；非阻塞后端若使用该语义，必须避免忙等并及时观察 stop。第四阶段 V4L2 预期使用阻塞 DQBUF，在 `produce()` 内等待到有项目、ERROR 或外部取消。

### 构造期静态能力合同

- 普通 SourceNode / TransformNode 的具体类必须在构造函数中显式 `addSrcPad()` 创建唯一首个固定 SrcPad，并以完整 `TemplateCaps` 声明输出能力；不再允许 `requestSrcPad()` 用 link hint 创造首个能力集合。
- `requestSrcPad()` 仅服务同源分叉：没有构造期首 Pad 时直接拒绝；已有首 Pad 时只验证 hint_type 落在其 TemplateCaps 内，再完整复制该 TemplateCaps 并共享其 OutputRoute。
- 因而 `hint_type` 始终只是本次 link 的静态能力校验，绝不写入 actualType、也绝不定义节点能力。DecodeNode 已在构造期显式声明 `{VIDEO_RAW, AUDIO_RAW}` 输出；未来 V4L2CaptureNode 必须同样在构造期声明 `{VIDEO_RAW}`。
- Demux 的按媒体流动态 Route 和 Mux 的固定 `out_0` 是既有特殊模型，不改变。

### 当前边界

该决策只完成 Source 抽象与其单元/ASAN 验证，不实施 V4L2 ioctl、mmap、设备协商或阻塞 DQBUF 取消。第四阶段 V4L2 首版仍只承诺固定协商格式：首帧前产出真实 VIDEO_RAW Caps，之后产出深拷贝 BufferRef；设备运行期重配、PTS discontinuity 和 Caps generation 留待后续独立设计。

---

## V4L2 采集取消、时钟域与 Encode configuration 边界

### 决策背景

`SourceNode` 有序生产模型落地后，首个真实设备后端必须补齐三个不能由抽象基类替代的边界：V4L2 阻塞等待如何响应 `Pipeline::stop()`、设备时间戳能否进入以 `steady_clock` 建立的纯视频 Clock，以及 DQBUF 错误帧如何影响实时管线。与此同时，EncodeNode 虽然方向上是 Decode 的逆过程，但输出 encoded Caps 的完整时机不能照搬 Decode：编码器在 `avcodec_open2()` 后未必已经有稳定的 codec configuration，且实时 Source 不允许为等待未来信息无界缓存 Packet。

### V4L2 可取消等待与驱动 buffer 合同

- V4L2CaptureNode 使用 `O_NONBLOCK` 打开设备，并在 `produce()` 内用有限超时 `poll()` 等待可读事件；每次 timeout 或 EINTR 返回 Source 基类，由下一轮自然观察 `stop_requested_`。不另设 eventfd、自管唤醒线程或在 stop 时跨线程 close fd。
- 否决“等 `onStop()` 关闭 fd 来打断阻塞 DQBUF”的方案：Pipeline 的 stop 顺序是先置节点 stop 标志、cancel Route、join worker，之后才按逆拓扑调用 `onStop()`。若 worker 正在不可中断 DQBUF，它等待 `onStop()`；而 `onStop()` 又等待 worker join，形成生命周期环。短超时 poll 已足以将停止延迟约束在 `poll_timeout_ms` 内，且不扩张核心取消接口。
- Ready 固定执行 `S_FMT → G_PARM →（若 TIMEPERFRAME 支持）S_PARM → G_PARM → mmap/QBUF/STREAMON`。请求 fps 只是配置意图；第二次 G_PARM 返回的 `timeperframe` 才是驱动最终接受的 nominal interval，写入 `Buffer.duration` 供视频消费侧选择晚帧阈值。它不进入 VIDEO_RAW Caps，也不等同于通过逐帧 PTS 测得的实际交付帧率。
- 每个成功 DQBUF 的驱动 mmap buffer 都必须在离开本轮处理前重新 QBUF：先深拷贝入框架紧密 Buffer，再 QBUF；拷贝、布局检查或后续 Route 发布失败也不得永久占住驱动 buffer。未压缩固定单平面 capture 的完整布局以 `S_FMT` 返回的 bytesperline/sizeimage 和 QUERYBUF mmap length 为权威，不能仅因部分 UVC 驱动的 `bytesused` 报告而把可访问 buffer 误判为半帧。
- `V4L2_BUF_FLAG_ERROR` 表示当前 DQBUF buffer 可能损坏，但仍是可恢复 streaming error：丢弃当前帧、QBUF、继续采集；不把单个坏帧上报为 Pipeline ERROR。只有 QBUF/DQBUF/协商等真正无法继续的后端操作失败才走 ERROR。错误帧计数、限频 WARNING 和观测接口留待需要时设计。

### V4L2 时间戳时钟域合同

- 框架纯视频 Clock 的墙钟基准是 `std::chrono::steady_clock`，在当前 Linux 目标上对应单调时钟域。因此 Capture Buffer 的 PTS 只接受明确带 `V4L2_BUF_FLAG_TIMESTAMP_MONOTONIC` 的 timeval，并换算为微秒。
- 否决“timestamp type 只要不是 UNKNOWN 就接纳”的宽松判断：`V4L2_BUF_FLAG_TIMESTAMP_COPY` 只说明 timestamp 从对应 output buffer 复制，其时钟域来自外部提供者，COPY 本身不证明与 CLOCK_MONOTONIC 同域；UNKNOWN 也可能来自 realtime。两者均保留 `AV_NOPTS_VALUE`，不与 `steady_clock` 混用。
- `V4L2_BUF_FLAG_TSTAMP_SRC_SOE/EOF` 仅表示帧开始/结束的采样位置，不改变其时钟域；MONOTONIC timestamp 的 SOE 与 EOF 都可被接受。

### Encode configuration 就绪合同

- EncodeNode 的 encoder 名称和 nominal framerate 是构造期 `EncodeConfig`，不是从 VIDEO_RAW Caps 推断。Ready 只查找、验证 encoder；`avcodec_open2()` 必须等待 Running RAW Caps 提供真实 width、height、input pix_fmt 后再执行。
- 否决“在 `avcodec_open2()` 后立即发布 VIDEO_ENCODED Caps”的方案：encoder 可能直到第一个输出 Packet 才写出 codec configuration；提前发布的空/不完整 extradata 不能完整解释后续 Packet，违反 Running Caps 先于所辖 Buffer 的合同。
- 当前请求 `AV_CODEC_FLAG_GLOBAL_HEADER`，并只在第一个实际 Packet 前观察到稳定、非空 `ctx_->extradata` 时发布 `VIDEO_ENCODED Caps → 第一个 Packet`。这将 out-of-band codec configuration 固定为 Caps 的可复制字段，避免猜测关键帧是否恰好携带完整 in-band 参数集。
- 否决“等待第一个未来关键帧或 EOS flush 才决定 configuration、期间暂存所有 Packet”的方案：实时 V4L2 Source 无界，若 configuration 只在未来才出现，该策略会形成无界内存积压并失去流式输出。无法满足 global-header 首 Packet configuration 的 encoder 配置明确报错；未来若要支持 in-band configuration 或运行期 configuration 变化，必须先独立设计其 Caps/event 语义。
- 每个编码输入都复制或转换到 encoder 自有的 `av_frame_get_buffer()` 存储；不让 encoder 延迟引用 Route delivery 中的 Buffer payload。`send_frame(EAGAIN)` 一律先 receive drain、再以同一个仍存活 AVFrame 重发，Caps 重配 flush 与 EOS flush复用该状态机。

---

## AVMuxNode MPEG-TS 首版与框架侧 DTS 调度

### 决策背景

`MuxNode` 原本只在当前已有队首的输入之间选择最小 DTS。若某一路暂时为空，其他路的 Packet 会先写出，随后到达的更小 DTS 已无法插回，因而不能诚实地宣称框架完成了跨输入排序。与此同时，`av_interleaved_write_frame()` 会建立 libavformat 自己的通用 interleave queue，与 Route 的可靠有界背压和框架侧排序 owner 重叠。

### 正式合同

- `MuxNode::writeHeader()` 改为 `writeHeader(MuxFormat format)`：基类持有唯一不可变的 `format_`，在调用钩子时显式传入；具体后端不读取 protected/private 格式成员，也不重复保存 format
- Header 后，所有尚未 EOS 的输入都必须先拥有一个 Buffer 队首，才在完整候选集中选择框架微秒量纲的全局最小 DTS。Caps 和 EOS 等控制 Event 先在全部活跃输入中扫描、优先于 Packet 处理；ack 后的 EOS 输入从下一轮候选集合移除。Header 前任一路 EOS 都证明本轮初始 Caps 无法齐备，必须立即报配置错误
- 活跃输入缺少候选 Buffer 时，其他输入在 Route 上可靠背压，不以“先写当前可见 Packet”、FFmpeg 隐藏 interleave queue 或无界缓存越过未知未来 DTS
- 当前 `MuxNode` 因而要求每个 Header 后待写 encoded Buffer 有有效 DTS；无 DTS 不参与猜测排序，而是作为协议错误拒绝
- `AVMuxNode` 具体后端使用 `av_write_frame()`，不调用 `av_interleaved_write_frame()`；跨输入排序的唯一 owner 是 `MuxNode`
- `AVPacket` 使用 `av_new_packet()` 建立 FFmpeg 自有且带 padding 的 payload 后深拷贝框架 Buffer 数据。不得借用 Buffer 的 `new[]` 存储，也不得把它交给 `av_packet_from_data()`
- `EncodedMeta::flags` 必须恢复到 `AVPacket::flags`，保持 keyframe 信息供 MPEG-TS、后续 FLV 和 fragmented MP4 使用
- custom AVIO callback 只调用 `appendContainerBytes()` 立即复制临时容器字节。Header、每个 Packet 和 Trailer 结束前调用 `avio_flush()` 并检查 AVIO error，随后仍由 `MuxNode` 发布有序 CONTAINER Buffer

### 首版范围与后续边界

- `AVMuxNode` 首版只接受一条 `VIDEO_ENCODED` H.264 或 HEVC 输入，并只实现 `MuxFormat::MPEGTS`。音频、多视频、FLV 和 fragmented MP4 在 link 或后端边界明确拒绝/留待后续，不伪装为已支持
- 首份 video Caps 必须有 codec_id、正 width/height 和非空 extradata；后端将 extradata 以 `av_mallocz(size + AV_INPUT_BUFFER_PADDING_SIZE)` 深拷贝给 `AVCodecParameters`
- AVStream 先以框架微秒 `{1, 1000000}` 建立 time_base；`avformat_write_header()` 后写 Packet 必须使用 muxer最终确认的 stream time_base 重标定 pts/dts/duration
- `closeContext()` 必须支持部分初始化，释放顺序为当前 `AVIOContext::buffer`、`avio_context_free()`、`avformat_free_context()`
- 本轮不新增临时 CONTAINER Sink 或 test_pipeline 测试场景。普通/ASAN 既有回归只证明集成、生命周期和既有 Mux 合同未退化；真实 MPEG-TS 字节的 ffprobe/解码验收等待实际网络 Sink 连接后进行

---

## AVMux FLV、Mux session 时间轴与正式 FileSink

### FLV 后端范围

- `AVMuxNode` 仍是唯一 FFmpeg 容器具体节点，以构造期不可变 `MuxFormat` 选择后端；不拆出独立 FLVMuxNode
- `MuxFormat::MPEGTS` 继续支持单视频 H.264/HEVC；`MuxFormat::FLV` 首版只声明单视频 H.264，暂不把 FFmpeg 特定版本的 HEVC-in-FLV 能力提升为框架兼容性合同
- custom AVIO、Packet 自有 payload、flags、Header/Packet/Trailer flush 与 `av_write_frame()` 路径由两种格式共用；fragmented MP4 保持后续独立步骤

### Mux session 相对时间轴

- 框架 Buffer 的 V4L2 PTS/DTS 可来自系统 monotonic absolute 时间，不能直接写入文件容器，否则 FLV duration 会被错误解释为系统启动秒数
- 单视频 Mux session 以首个有效 DTS 为唯一 origin，后续写入 `relative_dts = dts - origin`、`relative_pts = pts - origin`，保留 PTS−DTS 的 B-frame 重排偏移、duration 和帧间间隔
- Mux 仍要求有效 DTS，并在写入前拒绝 DTS 回退；首包归一化 DTS 必须为零。该归一化只属于容器 session，不修改框架 Buffer、Clock 或上游时间戳
- FLV 实测首 DTS 为零、首 PTS 为 +0.3 秒，ffprobe 的 start_time=0.3 秒是 B-frame 合法重排偏移；中断录制 duration 从错误的 58059 秒修正为约 11.833 秒

### FileSinkNode 正式合同

- `FileSinkNode` 是正式顺序 CONTAINER Sink，不解析容器格式，也不是测试夹具
- 构造期配置固定 path 与 overwrite；默认 `O_EXCL` 拒绝已有路径，显式覆盖仍使用 `O_NOFOLLOW` 并拒绝既有非普通文件，防止无意覆盖符号链接或设备节点
- Ready 打开并 `fstat()`；consume 对每个 CONTAINER Buffer 循环处理 partial write 和 EINTR，只有完整写入后 SinkNode 才 ack Delivery
- 自然 EOS 时 `onDrain()` 先 fsync，成功后基类才上报 Sink EOS；实时 Source 的 stop/cancel 不伪装为 EOS，不执行 Mux Trailer 或 FileSink自然 drain
- `onStop()` 是 Ready 回滚、错误、自然 EOS 后统一 fd 关闭入口；Linux close 被 EINTR 打断时不盲目重试，避免误关复用 fd

### 两类文件验收边界

- `v4l2_record_flv` 验证真实 V4L2→libx264→FLV→FileSink 的流式数据面和中断回收；SIGINT 文件不承诺 Encoder EOS flush 或 Mux Trailer，但应能被 ffprobe 识别并连续解码已写 Packet
- `transcode_to_flv` 使用有限文件输入自然 EOF，覆盖 Decode/Encode flush、Mux Trailer、FileSink fsync 和 Pipeline自然 EOS
- 当前实测两类 FLV 均为 H.264 640×480、约 11.833 秒且 ffmpeg 解码退出 0；自然 EOS 输入/输出均为 168 Frame/168 Packet，无尾帧截断；普通与 ASAN 两类路径均通过

---

## Mux 等齐期有界 staging 与多路 FLV 死锁修复

### 决策背景

多路 `transcode_to_flv`（视频 Decode→libx264 重编码 + AAC encoded 旁路 → 同一 FLV Mux）在自然 EOS 路径上输出 0 字节并永久挂起。实测证实：AAC Caps/Packet 先到，Header 前旧调度不消费已配置路的 Buffer；`AUDIO_ENCODED` Route 容量 32 被填满后，单线程 Demux 的 `publishBlocking` 卡在第 33 个音频包，视频只推进到 Encode 约 26 帧，默认 x264 约 47 帧才出首 Packet/video Caps，Header 永远无法建立。

这不是单点 API 写错，而是三个各自合理语义首次同时触发：

```text
单线程 Demux 可靠阻塞发布
+ 等齐 Header / DTS 候选时不能越过写容器
+ Route 有限容量 + 视频侧编码迟滞
= 快路堵死慢路供给
```

问题比“Header 前”更宽：Header 后等齐 DTS 候选时，若已到齐的那路仍长期停在上游 Route，同样可堵死 Demux。扩大 Route、或只依赖 zerolatency 缩短窗口，都不是结构性解。

### 正式合同

- 跨输入排序与等齐期数据持有的唯一 owner 是 `MuxNode`；上游 Route 继续只做短 handoff（`ENCODED=32` 不变）
- 每个已连接输入 Pad 维护有界 `InputStaging`：只存已通过合同校验的 `BufferRef`；Caps/EOS 不进 staging
- 默认容量：每路 256 个 packet、全节点总计 512 个；按默认 x264 迟滞量级估算，不按 zerolatency 估算（算式见下节）
- 调度改为 `pull → emit → wait`：
  - pull 对全部输入只使用 `tryAcquire()`，禁止在多路扫描中 `acquireBlocking()`
  - Buffer 的 media_type、active Caps、有效 DTS、stream mapping 校验必须在 ack 前、进 staging 前完成；失败不 ack、不进 staging，直接 ERROR
  - 已配置 Pad 的 Buffer 进入 staging 后立即 ack，释放上游 Route
  - Header 前禁止 `writePacket`；全部 initial Caps 齐套后才 `CONTAINER Caps → writeHeader → flush`
  - Header 后在全部未 EOS 且未排空路的 staging 队首上选全局最小 DTS 写出
- Header 后 EOS：若该路 staging 非空，EOS 留在 Route 队首，排空后再 ack；排空并 ack 后该路退出候选集
- 只有“该路尚未 initial Caps 就 EOS”才在 Header 齐套前 ERROR；已配置路在 Header 前因 eager pull 露出的 EOS 合法，不按旧模型一律 Header 前 EOS 失败
- staging 满且无法写 Header/Packet：明确 ERROR，禁止永久挂死
- stop/cancel：清空全部 staging 与 pending 容器字节，不伪装成功 Header/Trailer

### 容量估算（`kStagingPerPadLimit=256` / `kStagingTotalLimit=512`）

这两个数字不是拍脑袋，也不是按 zerolatency 估；它们按**默认 libx264 quality 迟滞**下“快路在等齐期间最多会先到多少 encoded packet”做量级核算，并显式留安全余量。以后若引入 Encode 私参把首包延迟压到 1～2 帧，所需窗口只会更短，容量方向上只会更宽松，不会反噬本估算前提。

#### 输入量

| 量 | 取值 | 依据 |
|---|---|---|
| 默认 x264 内部固定缓存 | 约 **47 帧** | 实测 `rc_lookahead=40 + bframes=3`；首个 encoded Packet 前 Encode 约收 48 帧，稳态 `input_frames - output_packets ≈ 47` |
| 名义视频帧间隔 | **1/30 s** | demo/`EncodeConfig` 常用 nominal 30fps；这是配置意图，不是 VMware 实测交付 fps |
| 名义等齐时间窗口 | `47 / 30 ≈ 1.57 s` | 仅由 encoder 内部缓存决定的下界窗口；若再叠加 Decode 重排或更慢消费者，窗口只会更长 |
| AAC 包间隔 | `1024 / 44100 ≈ 23.22 ms` | 标准 AAC-LC 帧长 1024 sample @ 44.1 kHz |
| AAC 包率 | `44100 / 1024 ≈ 43.07 packet/s` | 与上式互逆 |
| 实测死锁触发对照 | audio 填满 Route **32** 时 video 仅约 **26** 帧 | 旧模型在默认 x264 远未出首包前就已堵死；说明“默认迟滞窗口内快路包数 ≫ 32” |

VMware 上还曾测到实际采集约 14.7fps、默认 quality 端到端 Present age 约 3.2s。那是环境 + encoder 叠加后的墙钟延迟，**不拿来当容量下界公式的唯一输入**；容量按更干净的“默认 encoder 帧缓存 × 名义帧率 × 快路包率”估算，再用余量覆盖 demux 抖动、Decode 重排和偶发更高音频包率。

#### 单路下限

等齐期内，已配置快路（典型为 AAC 旁路）在 Mux 本地需要暂存的 packet 数：

```text
N_fast ≈ T_wait × R_audio
       ≈ (N_encoder_delay_frames / F_video_nominal) × (sample_rate / samples_per_aac_frame)
       ≈ (47 / 30) × (44100 / 1024)
       ≈ 1.567 × 43.07
       ≈ 67.5 packet
```

对照与余量：

```text
旧 Route handoff 上限                         32     ← 已证实不够
默认 x264@30fps 名义窗口                      ≈ 68
按 3s 墙钟窗口粗算（1.5× 名义窗口量级）       3 × 43 ≈ 129
按诊断用 1.5s × ~50 packet/s 量级             ≈ 75
demux 抖动 / Decode 重排 / 更高音频包率余量   取整到 256
```

因此：

```text
kStagingPerPadLimit = 256
≈ 默认名义需求 68 的约 3.8 倍
≈ 3s@43pps 需求 129 的约 2.0 倍
```

256 覆盖“默认 encoder 迟滞 + 常见 AAC 包率 + 一截安全余量”，不是“刚好 47”。触顶仍 ERROR，极端 skew 不靠无界内存硬扛。

#### 全节点总顶

```text
kStagingTotalLimit = 512
= 2 × kStagingPerPadLimit
```

含义：

- 当前主验收拓扑是两路（video encoded + audio encoded）；总顶允许最坏时约两路同时接近单路上限，同时防止三路以上或异常双涨时内存按路数线性失控
- 总顶与单路顶同时生效：任一先到即停止再 pull 该方向的新 Buffer；若此时仍无法写 Header/Packet，则 ERROR
- 总顶**不是**“永远够用的产品 SLA”，而是有界失败边界；真要服务高码率多路/更长产品窗口，应另开字节预算或可配置 staging，而不是默默加大常数

#### 与 Encode 私参的关系

```text
容量合同按默认 quality 迟滞成立
私参/zerolatency 只会缩短 T_wait → 降低常规水位
因此“以后上私参只会更宽松”以本节算式为前提，而不是口号
```

容量合同**不**依赖 encoder 一定 1～2 帧出首包；默认 medium 仍必须能过多路验收。

### 明确否决

- Demux 侧 multiqueue 作为 P0 唯一解（可作后续抖动优化，但不能替代 Mux 作为同步屏障 owner）
- 无界临时队列
- 仅扩大 Route 容量掩盖死锁
- 把 Encode zerolatency / 私参接口当作唯一修复
- 为提前齐套 Header 而发布残缺 Caps

### 验收边界

- 既有单测 `test_mux_waits_for_all_initial_caps` 与 `test_mux_orders_by_global_dts_when_inputs_arrive_out_of_order` 继续通过
- 真实素材 `那天下雨了原版MV.mp4` 经 `transcode_to_flv libx264 30` 自然 EOS 完成；ffprobe 识别 AAC + H.264 1280x720，duration 约 3:10，ffplay 可播放
- Encode 私参接口本轮不动，继续作为编码回环高延迟后续项；容量合同不依赖 encoder 一定 1～2 帧出首包

---

## TcpSinkNode 与 MPEG-TS/TCP 首闭环

### 决策

- 首个网络 Sink 采用与 `FileSinkNode` 同构的顺序 `CONTAINER` 字节写出模型，不解析容器，不实现 RTMP/RTSP 会话
- 具体节点名 `TcpSinkNode`；首版只支持主动 Connect，对端 listen（`ffmpeg`/`ffplay` 的 `tcp://host:port?listen`）
- 首闭环固定容器为 `MuxFormat::MPEGTS`：`Encode/Demux → AVMux(MPEGTS) → TcpSink`
- 因为 `Pipeline::stop()` 先 join worker 再调 `onStop()`，socket 必须非阻塞；consume 用 `poll(POLLOUT)` 切片观察 `stop_requested_`，禁止阻塞 `send` 卡死 stop
- 自然 EOS 的 `onDrain()` 只 `shutdown(SHUT_WR)` 让接收端 EOF；实时 Source 的 stop/cancel 不进入 drain，不承诺 Mux Trailer
- `send` 使用 `MSG_NOSIGNAL`，对端关闭/写失败明确 ERROR，不静默截断已获得的容器字节

### 明确否决

- 把已 mux 的 FLV/TS 字节裸写到 RTMP 端口
- 为贴文档旧名把 TCP 字节 Sink 叫 `RTSPPushNode`
- 首闭环引入 RTSP Server、RTMP URL 输出或 TCP Listen 模式
- 在 Network Sink 内二次解析容器或重建交织

### 验收

- `transcode_to_mpegts_tcp` 自然 EOS：本机 listen 的 ffmpeg 收到可识别 MPEG-TS/H.264 并完整结束
- `v4l2_push_mpegts_tcp` 实时推流可被 ffplay/ffmpeg 解码；SIGINT 有限时间回收
- 普通 + ASAN `test_pipeline` 不退化

---

## AVMuxNode fragmented MP4 与未提交 AVIO tail

### 决策

- `MuxFormat::MP4` 固定表示 fragmented MP4，不新建节点；不做传统 final-moov seek-back MP4
- 固定 `movflags=frag_keyframe+empty_moov+default_base_moof`
- 视频 H.264/HEVC，音频 AAC；多路复用既有 Mux staging + 全局 DTS 调度
- MPEG-TS / FLV 保持严格顺序 AVIO：`seekable=0`，write 立刻 `appendContainerBytes`
- fMP4 启用节点自有未提交 AVIO tail + 有限 `seekCallback`：
  - 逻辑字节流分为已 commit 前缀与 `[committed, end)` 的 tail
  - write 只落入 tail；seek 仅允许在 tail 内（含 `AVSEEK_SIZE`）
  - 禁止 seek/write 进入已 commit 前缀；禁止在逻辑 EOF 后制造稀疏空洞
  - 仅当写指针 `pos == end` 时才把整段 tail 顺序 `appendContainerBytes` 并推进 commit
  - Header / Packet / Trailer 在 `avio_flush` 后按上述条件尝试 commit
- 设计动机：FFmpeg mp4 muxer 写 fragment 时先把 moof/traf size 写成 0，再 seek 回填；大 moof 超过 AVIO 内部小缓冲后，若 size=0 头部已被顺序提交，则无法回填，ISO 下 size=0 表示 box 延伸到 EOF

### 明确否决

- 仅靠无限增大 `kAvioBufferSize` 掩盖大 moof（高码率/长 GOP 仍可能超过任意固定缓冲）
- 把 tail seek 推广为传统整文件 moov 回写
- 允许改写已进入 pending / Route / FileSink 的前缀
- 在 AVIO callback 内 publish Route 或发送 EOS
- 以短素材单小 fragment“碰巧可解码”单独作为 fMP4 完成判据