# FindFFmpeg.cmake
# 查找 third_party/ffmpeg/<arch>/ 下的 FFmpeg 静态安装前缀
#
# FFmpeg 的 .pc 文件是静态链接闭包的唯一权威。它会包含 FFmpeg 各库、启用的
# libx264/libx265 wrapper 及其平台传递依赖，避免项目 CMake 手工复制这些依赖。
#
# 设置以下变量:
#   FFMPEG_FOUND
#   FFMPEG_INCLUDE_DIRS
#   FFMPEG_LIBRARIES
#
# FFMPEG_LIBRARIES 是供 target_link_libraries() 使用的 FFmpeg::Static interface target

set(FFMPEG_ARCH_DIR "${CMAKE_SOURCE_DIR}/third_party/ffmpeg/${CMAKE_SYSTEM_PROCESSOR}")
set(FFMPEG_PKGCONFIG_DIR "${FFMPEG_ARCH_DIR}/lib/pkgconfig")

if(NOT EXISTS "${FFMPEG_PKGCONFIG_DIR}/libavformat.pc")
    message(FATAL_ERROR
        "FFmpeg pkg-config metadata not found: ${FFMPEG_PKGCONFIG_DIR}/libavformat.pc")
endif()

find_package(PkgConfig REQUIRED)

# 隔离 pkg-config 搜索路径，交叉编译时不能回退到宿主机不同架构的 FFmpeg 或 encoder
set(_ffmpeg_saved_pkg_config_path "$ENV{PKG_CONFIG_PATH}")
set(_ffmpeg_saved_pkg_config_libdir "$ENV{PKG_CONFIG_LIBDIR}")
set(ENV{PKG_CONFIG_PATH} "")
set(ENV{PKG_CONFIG_LIBDIR} "${FFMPEG_PKGCONFIG_DIR}")

pkg_check_modules(FFMPEG_PC REQUIRED
    NO_CMAKE_PATH
    NO_CMAKE_ENVIRONMENT_PATH
    libavformat
    libavcodec
    libswresample
    libswscale
    libavutil
)

set(ENV{PKG_CONFIG_PATH} "${_ffmpeg_saved_pkg_config_path}")
set(ENV{PKG_CONFIG_LIBDIR} "${_ffmpeg_saved_pkg_config_libdir}")

if(NOT TARGET FFmpeg::Static)
    add_library(FFmpeg::Static INTERFACE IMPORTED GLOBAL)

    # _STATIC_LDFLAGS 是 pkg-config --static 返回的完整有序闭包，包含 -L 搜索路径、
    # 库和 -pthread 等链接选项。pipeline 是静态库，CMake 不会可靠地把拆分后的
    # INTERFACE_LINK_DIRECTORIES 传递到最终可执行文件，因此这里必须把完整序列作为
    # interface link item 保留，避免手写外部 encoder 或系统传递依赖
    set_property(TARGET FFmpeg::Static PROPERTY
        INTERFACE_INCLUDE_DIRECTORIES "${FFMPEG_PC_STATIC_INCLUDE_DIRS}")
    set_property(TARGET FFmpeg::Static PROPERTY
        INTERFACE_COMPILE_OPTIONS "${FFMPEG_PC_STATIC_CFLAGS_OTHER}")
    set_property(TARGET FFmpeg::Static PROPERTY
        INTERFACE_LINK_LIBRARIES "${FFMPEG_PC_STATIC_LDFLAGS}")
endif()

set(FFMPEG_INCLUDE_DIRS "${FFMPEG_PC_STATIC_INCLUDE_DIRS}")
set(FFMPEG_LIBRARIES FFmpeg::Static)
set(FFMPEG_FOUND TRUE)
message(STATUS "FFmpeg static pkg-config closure found: ${FFMPEG_ARCH_DIR}")
