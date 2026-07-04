# 重构过程决策记录

实现 V2 过程中遇到的问题和调整，按时间顺序记录。

---

## 基础类型阶段

### Event 类型精简

最初按 V2 文档定义了 5 种事件：CapsEvent、EOSEvent、FlushStartEvent、FlushStopEvent、ErrorEvent。

讨论后发现：
- FlushStart/FlushStop 服务于 seek，但项目场景不需要 seek，直接删除
- ErrorEvent 走队列有问题——节点出错时 runLoop 可能已退出，没有机会再 push ErrorEvent；就算 push 进去了，下游收到也只能停，不如 Pipeline 统一 stop()
- 出错节点只需做两件事：通过 MessageBus 上报 ERROR + 退出 runLoop

最终 Event 精简为两种：`CapsEvent`（Ready 协商）和 `EOSEvent`（流结束）。

### MessageBus 统一上报

最初设计混合使用——EOS 走直接回调 `onSinkEOS()`，ERROR 走直接回调 `onNodeError()`，WARNING/INFO 走 MessageBus。节点作者需要记住什么情况用哪种方式，心智负担重。

改为统一走 MessageBus。节点只需 `postMessage(type, text, code)`，Pipeline 内部运行独立监听线程分发处理。删除了 `onSinkEOS()` 和 `onNodeError()` 两个直接回调。

代价是 Pipeline 多一个监听线程，但 Pipeline 本来就管理 N 个节点线程，多一个开销可忽略。

### notifyObserver 死锁约束

`MessageBus::notifyObserver()` 持有 mutex_ 时调用 observer_，如果用户在回调里调 `post()` 会死锁。决定不额外处理，在注释里说明约束。死锁只会在用户违反"回调轻量"约束时发生，是使用错误不是设计缺陷。

---

## Buffer 阶段

### 工厂方法签名调整

`fromAVPacket` 和 `fromAVFrame` 最初只接受 `(ptr, MediaType)`，时间戳转换由调用方负责。但 Buffer 的契约是"时间戳单位微秒"，如果工厂方法存原始值就是违反契约，调用方忘了转就出 bug。

改为工厂方法内转：加 `time_base` 参数，内部做 `av_rescale_q`。`fromAVPacket` 额外加 `codec_id` 参数填入 EncodedMeta。`fromAVFrame` 额外加 `framerate` 参数计算视频帧 duration。

### 视频平面数计算

最初手动用 `desc->nb_components` 计算平面数，在 NV12 等半平面格式下出错（3 个 component 但只有 2 个 plane）。

改为用 FFmpeg 的 `av_image_copy_to_buffer()`，正确处理所有像素格式。

### VideoRawMeta 去掉 framerate

framerate 是流级属性，不是帧级属性。CapsEvent 已经携带了，每帧 Buffer 里存是冗余的。从 VideoRawMeta 里去掉，`fromAVFrame` 的 framerate 参数只用于计算 duration。

### BufferMeta 保留

BufferMeta 和 CapsEvent 信息重复，但保留并注释说明：为后续支持运行时格式变化（STREAM_INFO_CHANGED）预留。

### duration 计算

`fromAVFrame` 里 duration 最初写死 0。但 AudioPlayNode 更新 Clock 时需要音频帧 duration。改为：
- 音频帧：`av_rescale_q(nb_samples, {1, sample_rate}, {1, 1000000})`
- 视频帧：`av_rescale_q(1, {framerate.den, framerate.num}, {1, 1000000})`，framerate 由调用方传入

---

## Edge / Queue 阶段

### Edge::create 去掉 caps 参数

`Edge::create()` 传了 TemplateCaps 参数但完全没用到——Queue 在 `createQueuesForNode()` 里创建，不在 `create()` 里。是死参数，直接删除。

### createQueuesForNode 的鸡生蛋问题

`createQueuesForNode()` 在 `onStreamInfo()` 之前执行，但此时不知道每条边实际传的 MediaType，无法精确选队列容量。而 Queue 必须在 `onStreamInfo()` 之前存在，因为 CapsEvent 要通过 Queue 传递。

分两步走：
1. `createQueuesForNode()` 先用固定容量 8 创建 Queue，足够传 CapsEvent
2. 上游节点在 `onStreamInfo()` 里，知道了实际 MediaType，先 `resize` 对应 SrcPad 的 Edge Queue 到正确容量，再发 CapsEvent

顺序必须是先 resize 再发 CapsEvent。BoundedQueue 增加 `resize()` 方法。`selectQueueCapacity()` 改为接受单个 MediaType。

### selectQueueCapacity 签名变更

原来接受 `TemplateCaps`，取所有支持类型中最大容量。现在改为接受 `MediaType`，因为在 `onStreamInfo()` 里 resize 时已经知道实际类型了，可以精确匹配。

---

## Graph / Pad 阶段

### 懒连接机制替换为 requestPad 机制

实现 Graph::link() 时发现，原设计中 DemuxNode 的懒连接（`pending_links_` + `friend class Graph`）导致 Graph 必须知道 DemuxNode 的内部实现，产生硬耦合。如果以后有其他需要动态创建 Pad 的节点，Graph 又得改。

引入统一的 requestPad 机制：
- BaseNode 新增 `virtual SrcPad* requestSrcPad(name, hint_type)` 和 `virtual SinkPad* requestSinkPad(name, hint_type)`，默认返回 nullptr
- Graph::link() 查找已有 Pad，找不到时调 requestPad，由节点自己决定能否创建
- SourceNode/TransformNode 重写支持分叉（验证 hint_type 与已有 SrcPad 类型一致）
- DemuxNode 重写 requestSrcPad 创建多路输出，用 `pad_to_type_`（string → MediaType）替代 `pending_links_`
- MuxNode 重写 requestSinkPad 创建多路输入

好处：
- Graph 不依赖任何具体节点类型
- 懒连接逻辑从 Graph 移入节点自身
- 新增动态 Pad 节点只需重写 requestPad，Graph 无需改动

### link() 签名变更

`void link(...)` 改为 `bool link(...)`，新增 `MediaType hint_type` 参数。三种失败情况返回 false：
1. 目标 Pad 已被占用（isConnected()）
2. 节点拒绝创建（requestPad 返回 nullptr）
3. TemplateCaps 不兼容

用户调用时传入 hint_type 指示预期的数据类型：
```cpp
pipeline.link(demux, "video_0", vdecode, "in", MediaType::VIDEO_ENCODED);
pipeline.link(demux, "audio_0", adecode, "in", MediaType::AUDIO_ENCODED);
```

### build() 步骤顺序修正

原设计 build() 步骤顺序有误（环路检测在拓扑排序之前）。修正为：
1. TemplateCaps 兼容性检查（已在 link() 阶段完成）
2. 拓扑排序（Kahn 算法）
3. 环路检测：`topo_order_.size() != nodes_.size()` 即有环
4. 孤立节点检测：单独遍历，找出入度出度均为 0 的节点

孤立节点会被 Kahn 算法正常排入 topo_order_，不会被环路检测捕获，必须单独检查。

---

## 节点 / Pipeline 阶段

### BaseNode 构造函数

BaseNode 原来没有构造函数，`name_` 是裸露的 protected 成员，没有任何机制强制子类初始化它。`addNode<T>(name, ...)` 把 name 作为第一个参数传给 T 的构造函数，但这个约定从未在 BaseNode 里声明过。

改为 BaseNode 提供 `explicit BaseNode(const std::string& name)` 构造函数，不提供默认构造。子类必须在初始化列表里调用 `BaseNode(name)`，否则编译失败。

### Pipeline stop() 线程安全：CAS + STOPPING 中间状态

原设计 `stop()` 没有并发保护。如果用户从两个线程分别调 `waitEOS()` 和 `stop()`，或重复调 `stop()`，会导致双重 join 崩溃。

方案：
- `state_` 改为 `std::atomic<PipelineState>`
- 新增 `STOPPING` 中间状态
- `stop()` 用 `compare_exchange_strong(RUNNING, STOPPING)` 原子抢占，只有第一个到达的线程执行清理，其他线程直接返回
- `waitEOS()` 的 wait 条件增加 `state_ != RUNNING`，防止外部 stop() 后 waitEOS 死等
- `waitEOS()` 末尾调 `stop()`，CAS 自动处理"已经停过"的情况

### Pipeline last_error_ 线程安全

`last_error_` 由 messageBusLoop 线程写，主线程读。加 `error_mutex_` 保护。公开 `lastError()` 方法返回拷贝。

### Pipeline 析构函数

`~Pipeline() { stop(); }`，CAS 自动处理所有状态，不需要额外判断。

### Event 发送 API 收敛

Event 精简后只剩 `CapsEvent` 和 `EOSEvent`，原来的 `sendEventDownstream(const Event&)` 同时承担“事件广播”和“关键/非关键事件策略”两层语义，已经不符合当前模型：
- `CapsEvent` 是 Ready/onStreamInfo 阶段的流级格式协商事件，每个 SrcPad 的 Caps 可以不同，必须指定 SrcPad 阻塞发送
- `EOSEvent` 是运行期流结束事件，需要广播到所有已连接 SrcPad，阻塞发送且不允许丢失

因此删除 `sendEventDownstream()` 和 `isCriticalEvent()`，改为：
- `sendCapsEvent(src_pad_name, caps)`：指定 SrcPad 发送 CapsEvent
- `sendEOSDownstream()`：向所有已连接 SrcPad 广播 EOS

`TransformNode::onEvent()` 默认只透传 EOS；如果 CapsEvent 进入 runLoop，视为节点没有在 onStreamInfo 阶段正确消费 CapsEvent，通过 MessageBus 上报 ERROR。`SinkNode::onEvent()` 同理只接受 EOS。

### stop_requested_ 改为原子变量

`stop_requested_` 由 `Pipeline::stop()` / `postMessage(ERROR)` 写入，由各节点 runLoop 读取。普通 `bool` 跨线程读写存在 data race，可能导致 stop 偶发不可见或未定义行为。

改为 `std::atomic<bool> stop_requested_{false}`：
- 写入使用 `stop_requested_.store(true)`
- 节点循环读取使用 `stop_requested_.load()`

### Graph::ready() 失败回滚

Ready 阶段发生在线程启动前，但节点已经可能在 `onReady()` / `onStreamInfo()` 中分配 FFmpeg/SDL/设备等资源。原实现中一旦某个节点返回 false，`Graph::ready()` 直接失败返回，已初始化节点不会执行 `onStop()`，已创建的 Edge Queue 也会残留。

将 `Graph::ready()` 改为事务语义：
- 使用索引遍历 `topo_order_`
- 任一节点的 `onReady()` 或 `onStreamInfo()` 失败时，从当前节点索引开始按拓扑逆序调用已 touched 节点的 `onStop()`
- reset 所有已创建的 Edge Queue，释放 Ready 阶段写入的 CapsEvent 和临时队列
- `Pipeline::play()` 只负责在 `graph_.ready()` 返回 false 后设置 `ERROR` 并返回 false，不进入 RUNNING，不启动任何线程

节点的 `onStop()` 因此必须支持部分初始化状态：既可能在正常 stop 后调用，也可能在 Ready 失败回滚时调用。

### V2.68 文档同步

`Media_Pipeline_Framework_V2.68.md` 用于记录当前代码对应的设计版本。本轮同步了已实现代码与文档的差异：
- `stop_requested_` 原子化及示例中的 `.load()` / `.store(true)`
- `Graph::ready()` Ready 失败回滚语义
- `onStop()` 支持 Ready 回滚和部分初始化状态
- 使用示例检查 `pipeline.play()` 返回值，失败时不继续 `waitEOS()`
- encoded queue 容量注释从 64 修正为 128

`Graph::link()` / `Graph::build()` 阶段的详细错误报告机制后续单独设计。核心库当前不直接 `fprintf(stderr)`，文档中已去掉历史 stderr 描述；用户暂时只能根据返回值判断失败。

### Buffer FFmpeg 边界增强

`Buffer::fromAVPacket()` / `Buffer::fromAVFrame()` 是 FFmpeg 数据进入框架的边界，必须避免返回半初始化 Buffer。

本轮调整：
- `pts` / `dts` 遇到 `AV_NOPTS_VALUE` 或无效 `time_base` 时保持 `AV_NOPTS_VALUE`，Buffer 层不推算时间戳
- `duration` 无效时为 0
- `fromAVPacket()` 只接受 `VIDEO_ENCODED` / `AUDIO_ENCODED`，并保存 `pkt->flags` 到 `EncodedMeta::flags`
- `fromAVFrame()` 只接受 `VIDEO_RAW` / `AUDIO_RAW`，加强视频宽高、格式、data 指针和 `av_image_copy_to_buffer()` 返回值检查
- 音频路径检查 `sample_rate`、`channels`、`nb_samples`、`bytes_per_sample`、data 指针和 size 乘法溢出
- 音频读取优先使用 `frame->extended_data`，兼容超过 `AV_NUM_DATA_POINTERS` 的 planar 多声道音频
- `AudioRawMeta` 增加 `nb_samples`，便于下游解释 planar/packed audio buffer

暂不保存 `stream_index` / `pos` / `side_data`：流身份由 SrcPad/Edge 表达；seek、HDR、rotation、SEI 等 metadata 后续按实际需求单独设计。`fromAVFrame()` 不做 sample_fmt/layout 转换，保持 FFmpeg 原始布局，下游按自身需求转换。

---

## 二阶段收尾轮

进入第三阶段之前对 V2.68 已交付代码做的一轮深度审查，修掉 4 个真实缺陷。

### Pipeline::play() 未调 Clock::reset() —— 无音频模式全帧被丢

`Clock::wall_start_us_` 默认成员初始化为 0，`play()` 里从未调用 `Clock::reset()`。无音频回退模式下 `getPositionUs()` 返回 `nowUs() - 0`（即 `steady_clock` 起点到现在的微秒），第三阶段 `VideoRenderNode` 一算 `diff_us = frame_pts - clock_us` 得到巨大负值，触发所有帧被"落后判定"丢帧。

`play()` 开头补 `clock_.reset()`。仅重置墙钟基准，暂不重置 `audio_base_pts_us_` / `has_audio_` —— 当前 Pipeline 用完即扔的语义下够用；若未来支持 Pipeline 复用（二次 play）需完善为全量 reset。

### Graph::build() 孤立节点检测 O(V×E) → O(V+E)

原实现三重嵌套：外层遍历所有 node，内层遍历所有 edge 找有无关联边。文档 §6.3 明确给了 `unordered_set` 一次性收集 + O(V) 查表的方案，代码没跟上。

改为按文档实现：先一次遍历 `edges_` 将两端 `BaseNode*` 塞进 `connected` set，再一次遍历 `nodes_` 检查是否在 set 中，总体 O(V+E)。同时也让代码与设计文档字面一致，降低 review 成本。

### MessageBus 监听线程提前启动 —— 修复 Ready 失败时 lastError() 为空

原 `play()` 里 `bus_thread_` 在 `graph_.ready()` 之后才启动，Ready 阶段节点 `postMessage(ERROR)` 只落到 bus queue 但**没有消费者**，`messageBusLoop` 从未被触发，`last_error_` 从未被写入。文档 §7.3 承诺的：

```cpp
if (!pipeline.play()) {
    fprintf(stderr, "pipeline play failed: %s\n", pipeline.lastError().c_str());
}
```

在原代码下永远打印空字符串。第三阶段 DemuxNode/DecodeNode 大量在 Ready 阶段做真实设备/文件初始化，是必然踩到的场景。

方案调整：
- `bus_thread_` 挪到 `graph_.ready()` 之前启动（早于任何可能 postMessage 的位置）
- Ready 失败时先 `bus_running_ = false → bus_.notify() → bus_thread_.join()`，再置 `state_ = ERROR` 返回
- 依赖 `MessageBus::waitMessage` 的 `!queue_.empty() || !running` 语义，`join` 保证所有 Ready 期消息被 drain 完才退出，`last_error_` 完全可见

副作用是 Ready 期间的 WARNING/INFO 也自动走 observer 回调 —— Ready 和 Running 两阶段的消息路径完全一致，是一致性红利。

Ready 期间 postMessage(EOS) 属于节点作者违约（Sink 线程还没启动，谁能发？），当前不做框架防御。`waitEOS()` 中的 `state_ != RUNNING` 条件在 Ready 失败后仍能让用户不合规调用 `waitEOS()` 安全返回，无需额外保护。

### pushToDownstream 分叉路径 double-unref → UAF

原多路广播分支：

```cpp
Buffer* to_push = first ? buf : buf->clone();
first = false;
if (!pad->tryPush(QueueItem{BufferRef{to_push}}))
    to_push->unref();      // ← double-unref
```

`tryPush` 值传参 + 内部 `std::move` 到 `BoundedQueue::tryPush`，caller 交出所有权后已经无法再管理。失败路径下 `BoundedQueue::tryPush` 的形参 `item` 析构时 BufferRef 已经 unref 过一次，caller 再 `to_push->unref()` 就是对已 `delete` 对象执行原子 RMW，UAF。当前测试全走单路所以从未触发，多路分叉场景（第三阶段 DemuxNode 立即用到）下队列偶尔满一次就命中。

修复：入口即 `BufferRef primary(buf)` RAII 接管，路径分四类统一处理：
- 单 SrcPad 阻塞路径：`pushBlocking(QueueItem{std::move(primary)})`
- 指定 pad 多 SrcPad 路径：`tryPush(QueueItem{std::move(primary)})`
- 未连接 / 空 pad 集：直接 return，`primary` 出作用域自动 unref
- **多路广播**：不 move primary，每路 `primary.clone()` 各自新建 BufferRef 分发，循环结束 primary 析构 unref 原 buf

放弃原代码的"第一路直传 + 后续 clone"尾优化，换取代码不含生命周期状态机。多的那一次 clone 在 V2.68 §13 承诺的引用计数零拷贝上线后（clone → ref+1）即消失。

顺带清理：`tests/test_pipeline.cpp::test_bounded_queue_try_push_full` 中同款 double-unref 模式（历史遗留，普通编译从未抓到，ASAN 一跑就现形），一并修掉。

新增 4 个测试覆盖本轮修复：
- `test_pipeline_forked_broadcast` — 分叉广播成功路径
- `test_pipeline_forked_backpressure_no_uaf` — 一路慢 Sink 制造背压稳定触发 tryPush 失败分支，ASAN 下能精确抓 UAF
- `test_pipeline_ready_failure_reports_error` — 覆盖 lastError 修复的核心承诺
- `test_pipeline_ready_failure_rollback` — 补齐 Ready 事务回滚（原承诺过但无测试）

### 遗留 P2：所有权约定类型化

`BoundedQueue::tryPush(QueueItem)` / `BoundedQueue::pushBlocking(QueueItem)` 的"进来的就归我"所有权约定目前靠注释表达，caller 违约会立即 UAF。本轮的 `pushToDownstream` bug 和 `test_bounded_queue_try_push_full` bug 都是同一款违约的镜像。

未来可将签名改为 `tryPush(QueueItem&&)` / `pushBlocking(QueueItem&&)` 让类型系统承载所有权（caller 侧被迫 `std::move`，moved-from 后编译器/lint 帮着盯用错）。改动会穿透到 `SrcPad`、所有 caller 及部分测试代码，不在本轮做，作为 P2 清理项进入 implementation_plan.md 的"后续优化方向"表。

同类记账：进入第三阶段时 `TransformNode::process(Buffer* input, std::vector<Buffer*>& outputs)` 的 `outputs` 也建议改成 `std::vector<BufferRef>&`，把所有权约定通过类型显式化。
