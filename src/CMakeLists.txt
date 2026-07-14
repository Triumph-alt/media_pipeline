# ---- 核心框架源码 ----
set(CORE_SOURCES
    core/Buffer.cpp
    core/BoundedQueue.cpp
    core/Edge.cpp
    core/Pad.cpp
    core/BaseNode.cpp
    core/Graph.cpp
    core/Pipeline.cpp
    core/MessageBus.cpp
)

# ---- 节点源码（第三阶段）----
set(NODE_SOURCES
    nodes/AVDemuxNode.cpp
    nodes/DecodeNode.cpp
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

# ---- 系统依赖 ----
target_link_libraries(pipeline PUBLIC
    pthread
    m
    dl
    z
    lzma
    drm
)
