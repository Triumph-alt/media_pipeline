
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
