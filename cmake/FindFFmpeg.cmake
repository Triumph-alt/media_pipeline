# FindFFmpeg.cmake
# 查找 third_party/ffmpeg/<arch>/ 下的 FFmpeg 静态库
#
# 设置以下变量:
#   FFMPEG_FOUND
#   FFMPEG_INCLUDE_DIRS
#   FFMPEG_LIBRARIES

set(FFMPEG_ARCH_DIR "${CMAKE_SOURCE_DIR}/third_party/ffmpeg/${CMAKE_SYSTEM_PROCESSOR}")

if(NOT EXISTS "${FFMPEG_ARCH_DIR}/include")
    message(FATAL_ERROR "FFmpeg headers not found: ${FFMPEG_ARCH_DIR}/include")
endif()

set(FFMPEG_INCLUDE_DIRS "${FFMPEG_ARCH_DIR}/include")

# 按依赖顺序排列: avformat -> avcodec -> avutil, swresample, swscale
set(FFMPEG_LIBRARIES
    ${FFMPEG_ARCH_DIR}/lib/libavformat.a
    ${FFMPEG_ARCH_DIR}/lib/libavcodec.a
    ${FFMPEG_ARCH_DIR}/lib/libswresample.a
    ${FFMPEG_ARCH_DIR}/lib/libswscale.a
    ${FFMPEG_ARCH_DIR}/lib/libavutil.a
)

# 检查静态库是否存在
foreach(lib ${FFMPEG_LIBRARIES})
    if(NOT EXISTS "${lib}")
        message(FATAL_ERROR "FFmpeg library not found: ${lib}")
    endif()
endforeach()

set(FFMPEG_FOUND TRUE)
message(STATUS "FFmpeg found: ${FFMPEG_ARCH_DIR}")
