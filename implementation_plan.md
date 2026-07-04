# Media Pipeline Framework — 实施计划

---

## 第一阶段：环境搭建与工程骨架

**目标**：编译环境、依赖库、工程结构全部就绪，写任何业务代码之前确保工具链正确。

### 任务

#### 1.1 Git 仓库

```bash
git init media-pipeline
```

`.gitignore` 需包含：`build/`、`third_party/ffmpeg/*/lib/*.a`、`compile_commands.json`、`.cache/`。

#### 1.2 安装交叉编译工具链（Ubuntu）

```bash
sudo apt install gcc-aarch64-linux-gnu g++-aarch64-linux-gnu
sudo apt install gcc-riscv64-linux-gnu g++-riscv64-linux-gnu   # RISC-V 占位
```

#### 1.3 交叉编译 FFmpeg 静态库

脚本放在 `scripts/build_ffmpeg.sh`，每个目标架构单独编译，输出到 `third_party/ffmpeg/<arch>/`。

**第一阶段配置（仅解码）**：

```bash
./configure \
  --prefix=$(pwd)/output \
  --enable-static \
  --disable-shared \
  --disable-programs \
  --disable-doc \
  --disable-avdevice \
  --disable-encoders \
  --disable-muxers \
  --disable-filters \
  --enable-decoder=h264 \
  --enable-decoder=hevc \
  --enable-decoder=aac \
  --enable-decoder=mp3 \
  --enable-decoder=pcm_s16le \
  --enable-demuxer=mov \
  --enable-demuxer=matroska \
  --enable-demuxer=flv \
  --enable-demuxer=mpegts \
  --enable-demuxer=aac \
  --enable-demuxer=mp3 \
  --enable-demuxer=wav \
  --enable-protocol=file \
  --enable-parser=h264 \
  --enable-parser=hevc \
  --enable-parser=aac
```

交叉编译时加：

```bash
--cross-prefix=aarch64-linux-gnu- \
--arch=aarch64 \
--target-os=linux
```

**第四阶段需额外开启**：

```bash
--enable-encoder=libx264 \
--enable-encoder=libx265 \
--enable-encoder=aac \
--enable-muxer=mp4 \
--enable-muxer=flv \
--enable-muxer=mpegts \
--enable-demuxer=v4l2 \
--enable-indev=v4l2 \
--enable-protocol=rtmp \
--enable-protocol=tcp
```

目录结构：

```
third_party/ffmpeg/
  x86_64/
    include/
    lib/          ← libavcodec.a libavformat.a libavutil.a libswscale.a libswresample.a
  aarch64/
    include/
    lib/
  riscv64/        ← 占位
    include/
    lib/
```

#### 1.4 交叉编译 SDL3

SDL3 使用 CMake 构建，同样需要为每个架构交叉编译。静态链接 SDL3 时需要一并链接 `libpthread`、`libdl`、`libm`。

脚本放在 `scripts/build_sdl3.sh`。

#### 1.5 CMake 工程骨架

**工具链文件** `cmake/toolchains/aarch64.cmake`：

```cmake
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
set(CMAKE_C_COMPILER aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER aarch64-linux-gnu-g++)

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
```

**工具链文件** `cmake/toolchains/riscv64.cmake`：

```cmake
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR riscv64)
set(CMAKE_C_COMPILER riscv64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER riscv64-linux-gnu-g++)
```

顶层 `CMakeLists.txt` 需要完成：

1. 检测目标架构（`CMAKE_SYSTEM_PROCESSOR`）
2. 根据架构选择 `third_party/ffmpeg/<arch>/` 下的库
3. 设置 `-fno-exceptions -fno-rtti`
4. 引入工具链文件
5. 添加 `src/`、`tests/`、`demo/` 子目录

#### 1.6 Hello World

在 `src/main.cpp` 写一个调用 FFmpeg API 的最小程序（打印 FFmpeg 版本号），能成功编译运行。

### 验收标准

- [x] `cmake -B build/x86_64 && cmake --build build/x86_64` 编译通过
- [x] `cmake -B build/aarch64 -DCMAKE_TOOLCHAIN_FILE=cmake/toolchains/aarch64.cmake && cmake --build build/aarch64` 编译通过
- [x] x86_64 二进制本机运行，输出 FFmpeg 版本号
- [ ] aarch64 二进制在目标板运行，输出 FFmpeg 版本号（如有板子）
- [x] Git 仓库初始化完成，`.gitignore` 配置正确

---

## 第二阶段：核心框架实现

**目标**：按照 V2 设计文档，实现完整的框架骨架。此阶段完成后框架可接入具体节点。

**设计依据**：`Media_Pipeline_Framework_V2.md`

### 任务

| 模块 | 关键内容 |
|------|---------|
| 基础类型 | MediaType 5 枚举、TemplateCaps、CapsEvent、Event variant、NodeType、NodeState、PipelineState |
| Buffer + BufferRef | 原子引用计数、RAII 包装、fromAVPacket/fromAVFrame、clone 深拷贝 |
| BoundedQueue | 阻塞/非阻塞 push/pop、flush、外部 notify 回调 |
| Edge | 每条边持有独立 BoundedQueue，根据 TemplateCaps 选择容量 |
| Pad | 纯接口，通过 Edge 间接访问 Queue |
| BaseNode 体系 | BaseNode + SourceNode + TransformNode + SinkNode + DemuxNode + MuxNode 基类 |
| Graph | 邻接表、link（含 DemuxNode 懒连接）、build（静态检查 + 拓扑排序）、ready（三步穿插） |
| Pipeline | 持有 Graph/Clock/MessageBus，build → play → stop → waitEOS，统管线程 |
| MessageBus | post + waitMessage |
| Clock | setAudioPosition 绝对位置 + 墙钟插值 |

### 清理旧代码

- 删除 `StreamInfo.h`（CapsEvent 替代）
- 删除 `Command.h`（`stop_requested_` 替代）
- 删除旧 MemoryBlock / MemoryPool / 旧 Buffer 实现
- 删除旧 INode / Pad / Pipeline / Event 实现
- 删除旧 `Types.h` 中的 `MemoryTier` 枚举

### 验收标准

- [x] TemplateCaps 兼容性检查测试通过
- [x] Buffer + BufferRef 生命周期测试通过（创建/拷贝/移动/释放/clone）
- [x] BoundedQueue 阻塞/非阻塞/flush/外部 notify 测试通过
- [x] Pad 通过 Edge Queue 正确传递数据（通过 Pipeline 集成测试间接覆盖）
- [x] BaseNode pushToDownstream 单路阻塞、多路 tryPush 测试通过（通过 Pipeline 集成测试间接覆盖）
- [x] Graph::build 检出不兼容连接、环路、孤立节点并报错
- [x] Graph::ready 三步穿插执行，CapsEvent 顺流传递
- [x] Pipeline build → play → stop 完整生命周期测试
- [x] 线程按拓扑逆序启动，stop 后全部退出无泄漏（并发 stop + waitEOS+stop 测试通过）
- [x] 编译时 `-fno-exceptions -fno-rtti`
- [x] Ready 阶段 `postMessage(ERROR)` 后 `lastError()` 可查询到错误文本（bus 提前启动 + 失败路径 join drain）
- [x] 分叉路径（多 SrcPad 广播）在下游背压 tryPush 失败时不发生 UAF / double-unref（ASAN 覆盖）
- [x] Ready 失败时事务性回滚，前置节点 `onStop()` 按拓扑逆序被调用

---

## 第三阶段：Demo 跑通 — 本地播放器

**目标**：实现 DemuxNode、DecodeNode、VideoRenderNode、AudioPlayNode，组装出完整播放器管线。

### 任务

| 节点 | 关键内容 |
|------|---------|
| DemuxNode | 懒连接、av_read_frame、时间戳转微秒、多路分发、EOF 发 EOS |
| DecodeNode | onStreamInfo 打开解码器（输出参数从 ctx 读取，不透传输入）、send/receive、EOS flush |
| VideoRenderNode | onStreamInfo 初始化 SDL、A/V 同步（超前 100ms sleep / 落后 50ms 丢帧）、sws_scale |
| AudioPlayNode | onStreamInfo 初始化 SDL 音频、swr_convert、推进 Clock |
| Demo | player.cpp：Demux → Decode×2 → VideoRender + AudioPlay |

### 验收标准

- [ ] 播放 H.264/AAC 的 mp4 文件正常（画面 + 声音）
- [ ] 音视频同步误差 ±40ms 以内
- [ ] 纯音频 / 纯视频文件不崩溃
- [ ] EOS 后正常退出，无线程泄漏
- [ ] Ctrl+C 中断后正常退出
- [ ] x86_64 通过

---

## 第四阶段：编码、复用与文件输出

**前提**：第三阶段验收全部通过。

### 任务

| 节点 | 关键内容 |
|------|---------|
| EncodeNode | avcodec_find_encoder + sws/swr 转换 + send_frame/receive_packet |
| MuxNode | 多 SinkPad + 外部 notify + selectMinDtsPad + 自定义 AVIOContext |
| FileSinkNode | fopen/fwrite/fclose |
| Demo | 采集编码录制、文件推流（不转码） |

FFmpeg 需额外开启 encoder 和 muxer（见第一阶段 1.3 配置）。

### 验收标准

- [ ] EncodeNode 编码 H264/AAC
- [ ] MuxNode 多流交织正确
- [ ] FileSinkNode 写出可播放的 mp4
- [ ] 端到端 Demo 通过
- [ ] x86_64 和 aarch64 均通过

---

## 第五阶段：采集节点

**前提**：第四阶段验收全部通过。

### 任务

| 节点 | 关键内容 |
|------|---------|
| V4L2CaptureNode | open/mmap/VIDIOC_DQBUF，MemoryBlock::fromExternal 零拷贝 |
| AudioCaptureNode | snd_pcm_open/snd_pcm_readi |
| Demo | 采集预览、采集录音 |

### 验收标准

- [ ] V4L2CaptureNode → VideoRenderNode 实时预览
- [ ] AudioCaptureNode → AudioPlayNode 实时播放
- [ ] x86_64 和 aarch64 均通过

---

## 后续优化方向

| 方向 | 说明 |
|------|------|
| MemoryPool | 分级内存池减少 malloc 碎片（当前 Buffer 用 new/delete） |
| Buffer 零拷贝 | 引用计数替代 clone() 深拷贝 |
| DMA-BUF | V4L2 采集零拷贝 |
| 硬件编解码 | VAAPI / V4L2 M2M |
| RTSP 推流 | RTSPPushNode |
| 线程绑核 | pthread_setaffinity_np |
| AV Sync 自适应 | 丢帧阈值动态调整 |

## 框架层技术债（P2 清理项）

进入第三阶段之前审查发现、当前不阻塞但值得记账的架构级改动。按优先级排列：

| # | 项 | 说明 | 触发时机 |
|---|---|---|---|
| 1 | 队列所有权类型化 | `BoundedQueue::tryPush(QueueItem)` / `pushBlocking(QueueItem)` 目前值传参 + 内部 `std::move`，"进来即归我" 靠注释表达。改为 `QueueItem&&` 让类型系统承载所有权，caller 必须显式 `std::move`，moved-from 后编译器/lint 帮着盯用错。同期把 `TransformNode::process` 的 `std::vector<Buffer*>& outputs` 改为 `std::vector<BufferRef>&`，让处理节点也统一到 RAII 语义 | 第三阶段 DemuxNode/DecodeNode 编写完后一次性重构，避免节点子类刚写好就大改 |
| 2 | DemuxNode / MuxNode core 基类缺席 | 文档 §14 声明这两个基类放在 `include/pipeline/core/`，含 requestPad/pad_to_type_/waitAnyPadReady/selectMinDtsPad/eos_pads_ 等**与 FFmpeg 无关的**通用骨架；当前只有 Source/Sink/Transform 三个基类。若不补，第三阶段 AVDemuxNode 会把通用骨架和 FFmpeg 具体实现混在一起，后续 RtspDemux 等要重造 | 进入第三阶段实现 DemuxNode 前 |
| 3 | `Graph::build()` 拓扑排序主循环 O(V×E) | 孤立节点检测已修，但 Kahn 主循环仍在每次弹节点时全扫 `edges_`。应一次性构建邻接表 `node → vector<node*>` 后 O(V+E) 完成 | 与项 1 同期或独立小 PR |
| 4 | `BoundedQueue::setExternalNotify` 无锁读 std::function | `notifyAfterPush` 在锁外读 `external_notify_`，`setExternalNotify` 在锁内写，理论 race。当前 MuxNode 只在 `onStreamInfo`（线程未启动）设置，不触发；接口文档未明说约束 | 进入第三阶段实现 MuxNode 时明确约束或改为持锁 copy 后锁外调 |
| 5 | `BaseNode::state_` 死字段 | 声明为 `NodeState state_` 但从未维护，`node->state()` API 存在但永远返回 `NULL_STATE`。要么按文档补全状态转换（在 onReady 成功/线程启动/onStop 等位置更新），要么删掉 | 进入第三阶段前顺手清理 |

以上项每一条都对应设计文档明确承诺过、当前实现未落地或有偏差的地方。第三阶段实施 demo 时按需触发，触发时把对应项从此表移到 refactoring_decisions.md 作为决策记录。
