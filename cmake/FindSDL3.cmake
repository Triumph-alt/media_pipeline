# FindSDL3.cmake
# 查找 third_party/SDL3/<arch>/ 下的 SDL3 静态库
#
# 设置以下变量:
#   SDL3_FOUND
#   SDL3_INCLUDE_DIRS
#   SDL3_LIBRARIES

set(SDL3_ARCH_DIR "${CMAKE_SOURCE_DIR}/third_party/SDL3/${CMAKE_SYSTEM_PROCESSOR}")

if(NOT EXISTS "${SDL3_ARCH_DIR}/include")
    message(FATAL_ERROR "SDL3 headers not found: ${SDL3_ARCH_DIR}/include")
endif()

set(SDL3_INCLUDE_DIRS "${SDL3_ARCH_DIR}/include")
set(SDL3_LIBRARIES "${SDL3_ARCH_DIR}/lib/libSDL3.a")

if(NOT EXISTS "${SDL3_LIBRARIES}")
    message(FATAL_ERROR "SDL3 library not found: ${SDL3_LIBRARIES}")
endif()

# SDL3 依赖的系统库
find_package(Threads REQUIRED)
list(APPEND SDL3_LIBRARIES Threads::Threads)

find_library(DL_LIBRARY dl)
if(DL_LIBRARY)
    list(APPEND SDL3_LIBRARIES ${DL_LIBRARY})
endif()

find_library(M_LIBRARY m)
if(M_LIBRARY)
    list(APPEND SDL3_LIBRARIES ${M_LIBRARY})
endif()

set(SDL3_FOUND TRUE)
message(STATUS "SDL3 found: ${SDL3_ARCH_DIR}")
