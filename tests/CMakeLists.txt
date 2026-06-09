# 单元测试
add_executable(test_bounded_queue test_bounded_queue.cpp)
target_link_libraries(test_bounded_queue PRIVATE pipeline)

add_executable(test_buffer test_buffer.cpp)
target_link_libraries(test_buffer PRIVATE pipeline)

add_executable(test_pad test_pad.cpp)
target_link_libraries(test_pad PRIVATE pipeline)

add_executable(test_pipeline test_pipeline.cpp)
target_link_libraries(test_pipeline PRIVATE pipeline)

add_executable(test_demux test_demux.cpp)
target_link_libraries(test_demux PRIVATE pipeline)
