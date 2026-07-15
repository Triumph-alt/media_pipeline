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
- [x] Buffer + BufferRef 生命周期测试通过，消费接口发布后只读
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

**目标**：实现 DemuxNode、DecodeNode、VideoRenderNode、AudioPlayNode，组装出完整播放器管线。

### 任务

| 节点 | 关键内容 |
|------|---------|
| DemuxNode | av_read_frame、时间戳转微秒、多路分发、EOF 发 EOS |
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
| 分叉传输零拷贝 | 已完成：Route Entry 单份 BufferRef，订阅者共享只读 payload；端到端 FFmpeg/设备零拷贝另行优化 |
| Route 字节预算 | 当前按条目数硬限，后续增加 payload 字节上限和节点级总内存预算 |
| DMA-BUF | V4L2 采集零拷贝 |
| 硬件编解码 | VAAPI / V4L2 M2M |
| RTSP 推流 | RTSPPushNode |
| 线程绑核 | pthread_setaffinity_np |
| AV Sync 自适应 | 丢帧阈值动态调整 |
| 输出所有权类型化 | `TransformNode::process` 的 outputs 后续改为 `std::vector<BufferRef>`；输入已通过 const Buffer + RouteDelivery ack 收紧 |
