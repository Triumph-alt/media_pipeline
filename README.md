# Media Pipeline Framework

面向嵌入式 Linux 的 C++ 媒体管线框架：显式 Graph、有界多订阅者 Route、可靠背压，以及采集、解/编码、复用、本地呈现与顺序 CONTAINER 输出等节点。

## 当前能力

| 类别 | 内容 |
|---|---|
| 采集 | V4L2 视频采集（固定协商 RAW） |
| 解/编码 | 文件解复用、解码、H.264/H.265 编码（libx264 / libx265） |
| 复用 | MPEG-TS、FLV、fragmented MP4（项目中 MP4 固定为 fMP4） |
| 呈现 | SDL3 视频渲染（默认适配显示器）、SDL3 音频播放 |
| 输出 | 顺序文件写出、TCP 主动连接推流（CONTAINER 字节） |
| 示例 | 本地播放、摄像头预览、编码回环、录制与转码 demo |

当前**不包含**：RTMP 会话、RTSP Server/客户端、TCP Listen、传统 seek-back 整文件 MP4、HTTPS/TLS/RTMPS。

## 依赖

项目只消费 `third_party/<组件>/<架构>/` 下的安装前缀，不直接链接外部源码树。

```text
third_party/
  encoders/<arch>/   # x264、x265
  ffmpeg/<arch>/     # 静态 FFmpeg（含 libx264 / libx265）
  SDL3/<arch>/
```

先按架构构建第三方（顺序：x264 → x265 → FFmpeg）：

```bash
# 本机
./scripts/build_x264.sh x86_64
./scripts/build_x265.sh x86_64
./scripts/build_ffmpeg.sh x86_64
# 如需：./scripts/build_sdl3.sh x86_64

# 交叉 aarch64（需工具链，见 cmake/toolchains/aarch64.cmake）
./scripts/build_x264.sh aarch64
./scripts/build_x265.sh aarch64
./scripts/build_ffmpeg.sh aarch64
```

脚本路径、外部源码位置与交叉前缀以 `scripts/build_*.sh` 与工具链文件为准。

## 编译

要求：CMake ≥ 3.16，C++17。

```bash
# 本机
cmake -S . -B build
cmake --build build -j"$(nproc)"

# aarch64 交叉
cmake -S . -B build/aarch64 \
  -DCMAKE_TOOLCHAIN_FILE=cmake/toolchains/aarch64.cmake \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build/aarch64 -j"$(nproc)"
```

单测：

```bash
cmake --build build --target test_pipeline -j"$(nproc)"
./build/tests/test_pipeline
```

交叉产物体积较大时可 strip 后再拷到板子：

```bash
# 示例（工具链 strip 路径以本机安装为准）
aarch64-linux-gnu-strip -o player.stripped build/aarch64/demo/player
```

## 运行 Demo

可执行文件位于 `build/demo/`（本机）或 `build/aarch64/demo/`（交叉）。

| 程序 | 用法 |
|---|---|
| `player` | `./player <媒体文件>` |
| `player_video_only` | `./player_video_only <视频文件>` |
| `player_audio_only` | `./player_audio_only <音频文件>` |
| `v4l2_preview` | `./v4l2_preview [设备] [宽] [高] [帧率]` |
| `v4l2_encode_decode_preview` | `./v4l2_encode_decode_preview [设备] [宽] [高] [编码器] [帧率]` |
| `v4l2_record_flv` | `./v4l2_record_flv <设备> <输出.flv> [宽] [高] [编码器] [帧率] [--overwrite]` |
| `v4l2_record_mpegts` | `./v4l2_record_mpegts <设备> <输出.ts> ...` |
| `v4l2_push_mpegts_tcp` | `./v4l2_push_mpegts_tcp <设备> <host> <port> ...`（对端需先 listen） |
| `transcode_to_flv` | `./transcode_to_flv <输入> <输出.flv> [编码器] [帧率] [--overwrite]` |
| `transcode_to_mpegts` | `./transcode_to_mpegts <输入> <输出.ts> ...` |
| `transcode_to_fmp4` | `./transcode_to_fmp4 <输入> <输出.mp4> ...`（输出为 fMP4） |
| `transcode_to_mpegts_tcp` | `./transcode_to_mpegts_tcp <输入> <host> <port> ...` |

示例：

```bash
./build/demo/player /path/to/clip.mp4

./build/demo/v4l2_preview /dev/video0 640 480 30
./build/demo/v4l2_encode_decode_preview /dev/video0 640 480 libx264 30

./build/demo/transcode_to_fmp4 in.mp4 /tmp/out.mp4 libx264 30 --overwrite
./build/demo/transcode_to_flv  in.mp4 /tmp/out.flv libx264 30 --overwrite
./build/demo/transcode_to_mpegts in.mp4 /tmp/out.ts libx264 30 --overwrite

# 接收端先 listen，例如：
# ffmpeg -y -i tcp://0.0.0.0:12345?listen -c copy /tmp/recv.ts
./build/demo/transcode_to_mpegts_tcp in.mp4 127.0.0.1 12345 libx264 30
```

说明：

- 摄像头等实时源没有自然结束；用 `Ctrl+C` 或关闭预览窗口停止。中断录制不保证写完容器尾。
- 文件输入会自然结束，并完成编码 flush 与容器收尾。
- 项目中的 MP4 输出是 **fragmented MP4**，不是传统整文件回写 moov 的 MP4。

## 目录结构（简要）

```text
include/pipeline/   # 公共头文件（core + nodes）
src/                # 实现
demo/               # 示例程序
tests/              # test_pipeline 等
scripts/            # 第三方静态库构建脚本
cmake/              # Find* 与交叉工具链
third_party/        # 按架构安装的 FFmpeg / 编码器 / SDL3
```

## 运行环境注意

- 视频预览需要可用的图形显示（SDL3 VIDEO）；音频播放需要可用音频设备。
- 同一进程同一时刻只应运行一个管线实例。
- 静态链接 FFmpeg/SDL 后，可执行文件体积较大；板端建议使用 strip 后的二进制。
