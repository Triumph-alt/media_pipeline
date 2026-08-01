# Demo 程序 - 第三阶段：本地播放器
add_executable(player player.cpp)
target_link_libraries(player PRIVATE pipeline)

# 独立组图的纯视频 / 纯音频验收入口；不让 AVDemuxNode 为缺失且未连接的流伪造分支。
add_executable(player_video_only player_video_only.cpp)
target_link_libraries(player_video_only PRIVATE pipeline)

add_executable(player_audio_only player_audio_only.cpp)
target_link_libraries(player_audio_only PRIVATE pipeline)

# 第四阶段最小真实设备闭环：V4L2CaptureNode → VideoRenderNode。
add_executable(v4l2_preview v4l2_preview.cpp)
target_link_libraries(v4l2_preview PRIVATE pipeline)

# 第四/五阶段节点闭环：采集 RAW → 编码 → 解码回 RAW → 本地渲染。
add_executable(v4l2_encode_decode_preview v4l2_encode_decode_preview.cpp)
target_link_libraries(v4l2_encode_decode_preview PRIVATE pipeline)

# 实时采集流式录制：SIGINT 使用强制 stop，不承诺生成 Mux trailer。
add_executable(v4l2_record_flv v4l2_record_flv.cpp)
target_link_libraries(v4l2_record_flv PRIVATE pipeline)

# 有限文件自然 EOS 转码：覆盖 Encode flush、Mux trailer 和 FileSink fsync。
add_executable(transcode_to_flv transcode_to_flv.cpp)
target_link_libraries(transcode_to_flv PRIVATE pipeline)

# 实时采集 MPEG-TS/TCP 推流：SIGINT 强制 stop，不承诺 Mux trailer。
add_executable(v4l2_push_mpegts_tcp v4l2_push_mpegts_tcp.cpp)
target_link_libraries(v4l2_push_mpegts_tcp PRIVATE pipeline)

# 有限文件自然 EOS：Encode flush、Mux trailer、TcpSink shutdown(SHUT_WR)。
add_executable(transcode_to_mpegts_tcp transcode_to_mpegts_tcp.cpp)
target_link_libraries(transcode_to_mpegts_tcp PRIVATE pipeline)
