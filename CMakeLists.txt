cmake_minimum_required(VERSION 3.16)
project(media_pipeline LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 检测目标架构
message(STATUS "Target architecture: ${CMAKE_SYSTEM_PROCESSOR}")

# 查找 FFmpeg
include(cmake/FindFFmpeg.cmake)

# 添加子目录
add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(demo)
