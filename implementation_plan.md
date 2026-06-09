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

**第三阶段需额外开启**：

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

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
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

- [ ] `cmake -B build/x86_64 && cmake --build build/x86_64` 编译通过
- [ ] `cmake -B build/aarch64 -DCMAKE_TOOLCHAIN_FILE=cmake/toolchains/aarch64.cmake && cmake --build build/aarch64` 编译通过
- [ ] x86_64 二进制本机运行，输出 FFmpeg 版本号
- [ ] aarch64 二进制在目标板运行，输出 FFmpeg 版本号（如有板子）
- [ ] Git 仓库初始化完成，`.gitignore` 配置正确

---

## 第二阶段：核心框架实现

**目标**：实现 Buffer/MemoryPool、BoundedQueue、Pad、INode 体系、Event、MessageBus、Clock、Pipeline 状态机。所有核心模块有单元测试。

### 任务

#### 2.1 Types.h

定义所有枚举：

```cpp
enum class NodeState { NULL_STATE, READY, PAUSED, PLAYING, ERROR };
enum class MediaType { UNKNOWN, VIDEO, AUDIO };
enum class OverflowPolicy { BLOCK, DROP_OLDEST, DROP_NEWEST };
enum class PadDirection { SRC, SINK };
```

#### 2.2 Buffer + MemoryPool

- `MemoryPool`：5 级池，`posix_memalign` 预分配，`release()` 通过自定义 deleter 调用
- `MemoryBlock`：工厂方法 `fromExternal` 用于零拷贝（V4L2/DMA-BUF）
- `Buffer`：`shared_ptr<MemoryBlock>`，工厂方法 `fromAVPacket`、`fromAVFrame`、`fromRawData`

测试：多线程并发 alloc/release，验证池耗尽 fallback，验证 fromExternal 回调触发。

#### 2.3 BoundedQueue

模板类，`std::variant<Buffer, Event>` 作为元素类型。

接口：`push`、`pop(timeout)`、`peek`、`flush`、`canPush`。

三种溢出策略：BLOCK、DROP_OLDEST、DROP_NEWEST。

测试：生产者-消费者多线程，验证背压阻塞，验证 DROP_OLDEST 丢旧行为。

#### 2.4 Event

EOS、FLUSH_START、FLUSH_DONE、STREAM_INFO_CHANGED、SEEQ。工厂方法创建。

#### 2.5 StreamInfo

结构体，包含视频（宽高、帧率、像素格式）和音频（采样率、声道、采样格式）字段。

#### 2.6 Pad

- `Pad` 基类：direction、mediaType、peer、streamInfo、state
- `SrcPad`：内置 `BoundedQueue`，`push(Buffer)`、`pushEvent(Event)`、`canPush()`
- `SinkPad`：内置 `BoundedQueue`，`pop(timeout)`、`peek()`

连接过程：`srcPad->connect(sinkPad)` 做方向检查、类型检查、建立双向引用、传递 StreamInfo、触发 `onLink` 回调。

测试：Pad 连接/断开，StreamInfo 传递，类型不匹配拒绝。

#### 2.7 INode 体系

- `INode`：基类，持有 Pad 列表、状态机、参数系统（`ParamValue = std::variant`）
- `SourceNode`：`workerLoop` 调用 `generateData()` → push 到 SrcPad
- `TransformNode`：`workerLoop` pop → `process()` → push
- `SinkNode`：`workerLoop` pop → `consume()`

生命周期：`probe()` → `ready()` → `createThread()` → `setState(PLAYING)` → `waitThreadExit()` → `null()`。

#### 2.8 MessageBus

`Message` 类型：ERROR、WARNING、STATE_CHANGED、EOS、STREAM_INFO。

`post()` 投递消息，`setCallback()` 设置回调，`poll()` 轮询。

#### 2.9 Clock

音频主时钟：`AudioPlayNode` 每帧调用 `advance(durationUs)`。

`getPositionUs()`：有音频返回音频位置，无音频返回墙钟差值。

#### 2.10 Pipeline

- `addNode<T>()`：创建节点并注册
- `link()`：记录 PendingLink
- `play()`：七阶段流程（拓扑排序 → probe → 解析连接 → 验证 → ready → 创建线程 → 启动）
- `stop()`：逆拓扑序停止，FLUSH 传播
- `reportSinkEOS()`：所有 Sink EOS 后触发用户回调
- `memoryPool()`：返回 MemoryPool 指针

### 验收标准

- [ ] Buffer 多线程并发测试通过，Valgrind 无报错
- [ ] BoundedQueue 背压测试通过
- [ ] Pad 连接测试通过（类型检查、StreamInfo 传递）
- [ ] Pipeline 状态机测试通过（NULL → READY → PAUSED → PLAYING → 停止）
- [ ] Pipeline 拓扑排序正确（Sink 先就绪 Source 最后启动）
- [ ] MessageBus 回调和轮询均正常工作
- [ ] 以上所有编译时使用 `-fno-exceptions -fno-rtti`
- [ ] 以上所有在 x86_64 和 aarch64 上均通过

---

## 第三阶段：Demo 跑通 — 本地播放器

**目标**：实现 DemuxNode、DecodeNode、VideoRenderNode、AudioPlayNode，组装出完整播放器管线。

### 任务

#### 3.1 DemuxNode

- `onProbe()`：`avformat_open_input` + `avformat_find_stream_info`，为每个流创建 SrcPad + StreamInfo
- `workerLoop`：`av_read_frame` → `Buffer::fromAVPacket` → push 到对应 SrcPad
- EOF 时发送 EOS Event

#### 3.2 DecodeNode

- `onLink()`：保存 codecpar 和 time_base
- `onReady()`：`avcodec_alloc_context3` + `avcodec_open2`，更新输出 StreamInfo，push STREAM_INFO_CHANGED
- `process()`：`avcodec_send_packet` → `avcodec_receive_frame` → `Buffer::fromAVFrame`
- 收到 EOS：flush decoder，取出 B 帧缓冲，推送剩余帧

#### 3.3 VideoRenderNode

- `onReady()`：SDL_CreateWindow + SDL_CreateRenderer + SDL_CreateTexture
- `consume()`：A/V 同步等待 → `SDL_UpdateYUVTexture` → `SDL_RenderPresent`
- 支持 YUV420P 输入，如需格式转换用 `sws_scale`

#### 3.4 AudioPlayNode

- `onReady()`：SDL_OpenAudioDevice + SDL_CreateAudioStream
- `consume()`：`swr_convert`（如需重采样）→ `SDL_PutAudioStreamData` → 推进 Clock

#### 3.5 Demo player.cpp

```cpp
auto pipeline = Pipeline::create("player");

auto demux  = pipeline->addNode<DemuxNode>("demux");
demux->setParam("url", "/home/user/video.mp4");

auto decV   = pipeline->addNode<DecodeNode>("dec-v");
auto decA   = pipeline->addNode<DecodeNode>("dec-a");
auto render = pipeline->addNode<VideoRenderNode>("render");
auto audio  = pipeline->addNode<AudioPlayNode>("audio");

demux->link(decV, "video_0");
demux->link(decA, "audio_0");
decV->link(render);
decA->link(audio);

pipeline->setEosCallback([&]() { pipeline->stop(); });
pipeline->setMessageCallback([](const Message& msg) {
    if (msg.type == Message::Type::ERROR)
        fprintf(stderr, "Error: %s\n", msg.text.c_str());
});

pipeline->play();
pipeline->waitForEos();
pipeline->stop();
```

### 验收标准

- [ ] 能播放 H.264/AAC 的 mp4 文件（视频画面正常，声音正常）
- [ ] 能播放纯音频文件（无视频流时不崩溃）
- [ ] 能播放纯视频文件（无音频流时不崩溃）
- [ ] 音视频同步误差在 ±40ms 以内
- [ ] EOS 后正常退出，无线程泄漏
- [ ] 以上在 x86_64 和 aarch64 上均通过

---

## 第四阶段：扩展功能

**前提**：第三阶段所有验收标准通过后才进入第四阶段。

### 任务

#### 4.1 摄像头采集（V4L2CaptureNode）

- `onProbe()`：`open(device)` + `VIDIOC_QUERYCAP` + `VIDIOC_ENUM_FMT`
- `onReady()`：`VIDIOC_S_FMT` + `VIDIOC_REQBUFS` + `mmap` + `VIDIOC_STREAMON`
- `generateData()`：`VIDIOC_DQBUF` → `MemoryBlock::fromExternal`（零拷贝）→ return buffer
- Buffer 释放时回调 `VIDIOC_QBUF`

Demo：`V4L2CaptureNode → VideoRenderNode` 实时预览。

#### 4.2 音频采集（AudioCaptureNode）

- `onProbe()`：`snd_pcm_open` + query
- `onReady()`：`snd_pcm_hw_params` + `snd_pcm_prepare`
- `generateData()`：`snd_pcm_readi` → `Buffer::fromRawData`

#### 4.3 编码（EncodeNode）

- `onReady()`：`avcodec_find_encoder` + `avcodec_open2`，初始化 `sws_scale` / `swr_convert`
- `process()`：格式转换 → `avcodec_send_frame` → `avcodec_receive_packet` → `Buffer::fromAVPacket`

FFmpeg 需额外开启 encoder（见第一阶段配置）。

#### 4.4 复用（MuxNode）

- `requestSinkPad()`：动态创建 SinkPad
- `onReady()`：`avformat_alloc_output_context2` + `avformat_new_stream` × N + 自定义 AVIOContext
- `workerLoop`：轮询所有 SinkPad 选 DTS 最小的 → `av_interleaved_write_frame`

#### 4.5 文件输出（FileSinkNode）

- `onReady()`：`fopen`
- `consume()`：`fwrite`
- `handleEOS()`：`fclose` + `reportSinkEOS`

#### 4.6 采集编码录制 Demo

```
V4L2CaptureNode → EncodeNode → MuxNode → FileSinkNode
AudioCaptureNode → EncodeNode → MuxNode
```

#### 4.7 文件推流 Demo（不转码）

```
DemuxNode → MuxNode → RTSPPushNode（后期）
```

### 验收标准

- [ ] `V4L2CaptureNode → VideoRenderNode` 实时显示摄像头画面
- [ ] 采集编码录制 Demo 能录制 mp4 文件
- [ ] MuxNode 多流交织正确
- [ ] 以上在 x86_64 和 aarch64 上均通过

---

## 工具与参考

### FFmpeg 静态库依赖顺序

静态链接时库的顺序很重要，错误顺序会导致 undefined reference：

```
libavformat → libavcodec → libswscale → libswresample → libavutil
```

以及系统库：`-lpthread -lm -ldl -lz -llzma`

### SDL3 静态链接注意

SDL3 静态链接时需要额外的系统依赖，CMake 中：

```cmake
find_package(SDL3 REQUIRED)
target_link_libraries(media_pipeline PRIVATE
    SDL3::SDL3-static
    pthread dl m
)
```

### Valgrind 测试

```bash
valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes \
    ./build/x86_64/tests/test_buffer
```

### 交叉编译验证

```bash
file build/aarch64/src/media_pipeline
# 应输出: ELF 64-bit LSB executable, ARM aarch64
```
