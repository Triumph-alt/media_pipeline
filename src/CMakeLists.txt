# 暂时只有一个 main.cpp 验证 FFmpeg 链接
# 后续 core/ 和 nodes/ 的源码会在这里添加

add_executable(media_pipeline main.cpp)

target_include_directories(media_pipeline PRIVATE ${FFMPEG_INCLUDE_DIRS})
target_link_libraries(media_pipeline PRIVATE ${FFMPEG_LIBRARIES})

# 静态链接 FFmpeg 时需要这些系统依赖
target_link_libraries(media_pipeline PRIVATE
    pthread
    m
    dl
)
