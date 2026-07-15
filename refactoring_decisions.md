
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
