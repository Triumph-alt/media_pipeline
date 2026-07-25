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

**第四阶段不额外依赖 FFmpeg 输入组件**：`V4L2CaptureNode` 直接使用 Linux V4L2 ioctl/mmap API，不通过 FFmpeg `avdevice` 或 v4l2 demuxer。

**第五阶段需额外开启**（按实际选择的编码器、容器和协议启用）：

```bash
--enable-encoder=libx264 \
--enable-encoder=libx265 \
--enable-muxer=flv \
--enable-muxer=mpegts \
--enable-protocol=rtmp \
--enable-protocol=tcp
```

传统 MP4 需要回写文件头，不属于当前通用流式 MuxNode 的目标；若未来需要，单独设计专用文件输出节点。

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

**目标**：按照设计文档，实现完整的框架骨架。此阶段完成后框架可接入具体节点。

**设计依据**：`Media_Pipeline_Framework.md`

### 任务

| 模块 | 关键内容 |
|------|---------|
| 基础类型 | MediaType 5 枚举、TemplateCaps、CapsEvent、Event variant、NodeType、PipelineState |
| Buffer + BufferRef | 原子引用计数、RAII 包装、fromAVPacket/fromAVFrame、发布后只读、分叉共享 payload |
| OutputRoute | 静态有界多订阅者日志、Subscription 独立游标、Delivery 处理后 ack、可靠阻塞背压、cancel |
| Edge | 每条边持有源 OutputRoute 的一个 RouteSubscription |
| Pad | SrcPad 绑定逻辑 Route，SinkPad 通过 Edge Subscription acquire/ack |
| BaseNode 体系 | BaseNode + SourceNode + TransformNode + SinkNode + DemuxNode + MuxNode 基类 |
| Graph | 邻接表、link（requestPad 动态 Pad + TemplateCaps 检查）、build（拓扑排序 + 环路/孤立节点检测）、ready（三步穿插） |
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
- [x] Buffer + BufferRef 生命周期测试通过，消费接口发布后只读，Transform 待发布输出和发布入口使用移动 RAII 所有权
- [x] OutputRoute 共享 BufferRef、独立订阅游标、处理后 ack、最慢订阅者背压、取消唤醒和事件顺序测试通过
- [x] Pad/Edge 通过共享 Route 和独立 Subscription 正确传递数据
- [x] 同源分叉对每项只 publish 一次，全部可靠订阅者收到完整序列且不深拷贝 payload
- [x] Graph::build 检出不兼容连接、环路、孤立节点并报错
- [x] Graph::ready 三步穿插执行，CapsEvent 顺流传递
- [x] Pipeline build → play → stop 完整生命周期测试
- [x] 线程按拓扑逆序启动，stop 后全部退出无泄漏（并发 stop + waitEOS+stop 测试通过）
- [x] 编译时 `-fno-exceptions -fno-rtti`
- [x] Ready 阶段 `postMessage(ERROR)` 后 `lastError()` 可查询到错误文本（bus 提前启动 + 失败路径 join drain）
- [x] 分叉路径在慢订阅者下通过 Route 硬容量可靠背压，两路都收到完整序列，无 UAF / double-unref
- [x] Route publish/acquire 等待可被 Pipeline stop/cancel 唤醒
- [x] Ready 失败时事务性回滚，前置节点 `onStop()` 按拓扑逆序被调用

---

## 第三阶段：Demo 跑通 — 本地播放器

目标：实现 DemuxNode、DecodeNode、VideoRenderNode、AudioPlayNode，组装出完整播放器管线。

### 任务

| 节点 | 关键内容 |
|------|---------|
| DemuxNode | av_read_frame、时间戳转微秒、多路分发、worker 在每条 Route 首个 Packet 前发布 encoded Caps、EOF 发 EOS |
| DecodeNode | Running encoded Caps 驱动 decoder 配置；真实 AVFrame 前发布 RAW Caps；重配前 drain、完整 send/receive EAGAIN 状态机、EOS flush；Frame PTS 优先使用 FFmpeg `best_effort_timestamp` |
| VideoRenderNode | Running Caps 应用格式边界；YUV420P/YUVJ420P 紧密帧直传 SDL IYUV，其余 CPU 可访问格式按真实 Caps 通过 swscale 转为 YUV420P；SDL VIDEO、Window、Renderer、Texture 和转换资源在 VideoRender 工作线程中初始化、使用和销毁；worker 退出前清理 SDL TLS；按帧处理自身窗口关闭请求并通过 `STOP_REQUESTED` 请求 Pipeline 停止；Clock 启动 rendezvous 与基于 `max(Buffer.duration, 40ms)` 的动态晚帧丢弃 |
| AudioPlayNode | Ready 建固定 canonical SDL 提交端；Running AudioRaw Caps 重建 input→canonical swr；canonical Clock、背压、EOS drain；音频 worker 退出前清理 SDL TLS |
| Demo | `player` 组装完整音视频链路；`player_video_only` / `player_audio_only` 分别独立组图验收单流；均由 Pipeline 管理 SDL 基础设施生命周期（同一进程同一时刻至多一个 Pipeline 存活） |

### 验收标准

- [x] 播放 H.264/AAC 的 mp4 文件正常（画面 + 声音；《那天下雨了原版MV.mp4》自然 EOS：5703 rendered + 3 dropped = 5706）
- [x] 纯视频 / 纯音频独立组图正常播放到 EOS（ASAN：H.264/YUV420P 纯视频 75 rendered / 0 dropped；AAC 纯音频完成 SDL drain）
- [x] EOS 后正常退出，无项目自身内存错误（普通与 ASAN 回归；ASAN player 自然 EOS 覆盖 YUV420P、YUV444P、YUV420P10LE）
- [x] Ctrl+C 中断后正常退出（ASAN 长音视频素材实测）
- [x] 当前进程仅使用一个 VideoRenderNode，且无其他模块提前初始化 SDL VIDEO
- [x] VideoRender 只处理自身的 `SDL_EVENT_WINDOW_CLOSE_REQUESTED`，其他 SDL 输入事件暂不纳入范围
- [x] 窗口关闭后通过 `STOP_REQUESTED` 唤醒 `waitEOS()` 并正常完成 Pipeline 停止（真实 X11 `WM_DELETE_WINDOW` 实测；ASAN 路径无项目内存错误）
- [x] ASAN 下自然 EOS、SIGINT 和窗口关闭路径无项目自身内存错误；真实 X11 GUI 仍仅报告已隔离的 Mesa/Gallium LeakSanitizer 基线
- [x] x86_64 通过

---

## 第四阶段：视频采集预览与 SourceNode 有序生产模型

**目标**：先完成 V4L2 视频采集到本地预览的最小闭环，并以此将 `SourceNode` 从旧的 `capture() -> Buffer*` 骨架改造成能产生有序 Running `QueueItem` 的正式生产接口。

**设计前提**：采集节点不能沿用“只返回 Buffer”的旧接口，因为 VIDEO_RAW 消费者必须先收到完整 Caps，设备协商或重配也必须在正确的 Buffer 边界插入新的 Caps。首版只承诺设备打开时协商出的固定格式；运行期设备重配、PTS discontinuity、Caps generation 不混入本阶段。

### 任务

| 模块 | 关键内容 |
|---|---|
| SourceNode | 定义并实现有序生产 `CapsEvent → BufferRef → … → EOSEvent` 的接口与统一发布边界；保持 Source 分叉共享 Route、可靠背压、stop/cancel 唤醒和 BufferRef RAII 合同 |
| V4L2CaptureNode | 打开 V4L2 设备、枚举/选择输入格式、协商固定 width/height/pix_fmt、申请并 mmap 驱动 Buffer、`VIDIOC_QBUF/DQBUF` 采集；首个 Buffer 前发布真实 VIDEO_RAW Caps；本阶段将帧深拷贝入框架 Buffer，DQBUF 后及时归还驱动 Buffer；不要求设备运行期格式重配 |
| 时间戳 | 明确 V4L2 buffer timestamp 到框架微秒 PTS 的映射；无有效设备时间戳时保留 NOPTS，不伪造媒体时间 |
| 预览 Demo | `V4L2CaptureNode → VideoRenderNode`；复用 VideoRender 的真实 pix_fmt/swscale、窗口关闭 STOP_REQUESTED 和单参与者启动栅栏语义 |
| 验证 | 无设备时至少覆盖 Source 有序 Caps/Buffer/EOS、分叉、stop/cancel 的单测；有设备时进行真实摄像头预览、窗口关闭和 SIGINT 验证 |

### 验收标准

- [ ] SourceNode 能在同一 Route 上可靠发布 `CapsEvent → Buffer* → EOSEvent`，Buffer 不会越过 active Caps；现有 Source 分叉、Route 背压、cancel/stop 语义保持成立
- [ ] V4L2CaptureNode 能打开指定设备并协商、发布与实际 DQBUF 图像一致的 VIDEO_RAW Caps
- [ ] `V4L2CaptureNode → VideoRenderNode` 在真实设备上持续预览；YUV420P 直传，其他 CPU 可访问协商格式经现有 swscale 正确显示
- [ ] 采集帧在拷贝后立即归还 V4L2 driver buffer；持续预览期间无驱动 Buffer 耗尽、框架内存单调增长或 Buffer 所有权错误
- [ ] SIGINT、窗口关闭、外部 `Pipeline::stop()` 都能取消阻塞 DQBUF/Route 等待、join 全部线程并正常退出
- [ ] 单元测试和真实设备路径分别完成 ASAN 验证；真实 GUI 的 Mesa/Gallium 基线与项目自身错误分开报告
- [ ] x86_64 通过；aarch64 仅在存在目标板与 V4L2 设备时验收

---

## 第五阶段：视频编码、流式复用与推流

**前提**：第四阶段的 SourceNode 有序 Caps 合同与 V4L2 预览闭环通过。

**目标**：将采集到的 RAW 视频编码为 H.264/H.265，经 FLV 或 MPEG-TS 流式复用后输出到可取消的网络推流端；文件录制、音频编码和传统 MP4 不混入本阶段。

### 任务

| 节点 | 关键内容 |
|---|---|
| EncodeNode | 接收有序 VIDEO_RAW Caps，按 encoder 所需 pix_fmt/尺寸建立或重建 swscale 与 AVCodecContext；处理 send_frame/receive_packet、延迟 Packet flush；在首个 encoded Packet 前发布完整 VIDEO_ENCODED Caps（codec、尺寸、extradata 等） |
| MuxNode / AVMuxNode | 接收有序 encoded Caps，全部输入初始 Caps 到齐后建立 Header 和固定 Pad→stream 映射；按 DTS 交织、写 trailer；Header 后 encoded Caps 改变按既定冻结合同报错 |
| RTSPPushNode / 网络 Sink | 建立可取消的输出 I/O，处理阻塞写入、网络失败与 stop；本阶段至少打通一种实际启用的流式协议和容器组合（例如 FLV/RTMP 或 MPEG-TS/TCP） |
| 推流 Demo | `V4L2CaptureNode → EncodeNode → AVMuxNode → 网络 Sink`；可按 Route 分叉扩展到预览，但分叉不是首个闭环的必要条件 |
| 兼容性 | 必要时在 CMake / FFmpeg 构建中启用编码器、muxer 和 protocol；不将音频编码、传统 MP4 seek-back 输出或运行期 Header 重配混入本阶段 |

### 验收标准

- [ ] EncodeNode 对第四阶段协商出的固定 RAW 视频格式稳定输出 H.264 或 H.265；输出 Packet 可由 ffprobe/ffmpeg 解码，PTS/DTS 合法，EOS 后延迟 Packet 完整输出
- [ ] AVMuxNode 仅在初始 encoded Caps 全部就绪后写 Header；写出的 FLV 或 MPEG-TS 可被 ffprobe 识别并连续解码；Header 后的 encoded Caps 改变按既定冻结合同明确报错
- [ ] 网络 Sink 在本地可控接收端完成端到端推流；断连、写失败和外部 stop 可在有限时间内取消并完整回收线程与资源
- [ ] `V4L2CaptureNode → EncodeNode → AVMuxNode → 网络 Sink` 连续运行时，框架不静默丢弃已获得的 encoded Packet，Route 不发生无界积压；必要时以预览分叉验证最慢可靠订阅者背压
- [ ] 单测、端到端推流与 ASAN 验证通过；第三方协议库/驱动泄漏与项目自身问题分开报告
- [ ] x86_64 通过；aarch64 在具备目标板、摄像头和接收端时验收

---

## 第六阶段及后续

- AudioCapture、音频编码、音视频采集同步与音视频复用；
- FileSink 与传统 MP4 专用文件输出；
- 采集设备的运行期重配、PTS discontinuity、Caps generation；
- DMA-BUF / 硬件帧零拷贝、硬件编解码、动态插件与多 Pipeline 模型。
