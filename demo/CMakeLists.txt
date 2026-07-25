# Demo 程序 - 第三阶段：本地播放器
add_executable(player player.cpp)
target_link_libraries(player PRIVATE pipeline)

# 独立组图的纯视频 / 纯音频验收入口；不让 AVDemuxNode 为缺失且未连接的流伪造分支。
add_executable(player_video_only player_video_only.cpp)
target_link_libraries(player_video_only PRIVATE pipeline)

add_executable(player_audio_only player_audio_only.cpp)
target_link_libraries(player_audio_only PRIVATE pipeline)
