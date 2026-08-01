# ---- 核心框架源码 ----
set(CORE_SOURCES
    core/Buffer.cpp
    core/Caps.cpp
    core/OutputRoute.cpp
    core/Edge.cpp
    core/Pad.cpp
    core/BaseNode.cpp
    core/Graph.cpp
    core/LatencyTrace.cpp
    core/Pipeline.cpp
    core/MessageBus.cpp
)

# ---- 节点源码（第三阶段）----
set(NODE_SOURCES
    nodes/AVDemuxNode.cpp
    nodes/AVMuxNode.cpp
    nodes/DecodeNode.cpp
    nodes/EncodeNode.cpp
    nodes/FileSinkNode.cpp
    nodes/V4L2CaptureNode.cpp
    nodes/VideoRenderNode.cpp
    nodes/AudioPlayNode.cpp
)

# ---- 构建静态库 ----
add_library(pipeline STATIC
    ${CORE_SOURCES}
    ${NODE_SOURCES}
)

# ---- 头文件搜索路径 ----
target_include_directories(pipeline PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_include_directories(pipeline PUBLIC
    ${FFMPEG_INCLUDE_DIRS}
    ${SDL3_INCLUDE_DIRS}
)

# ---- 链接库 ----
target_link_libraries(pipeline PRIVATE
    ${FFMPEG_LIBRARIES}
    ${SDL3_LIBRARIES}
)

# FFmpeg 和 SDL3 的静态传递依赖分别由其安装前缀的 pkg-config 元数据与 FindSDL3.cmake 提供
