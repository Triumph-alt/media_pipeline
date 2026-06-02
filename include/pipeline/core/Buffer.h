#pragma once

#include <cstddef>
#include <cstdint>

namespace pipeline {

// 相比于enum，enum class枚举值作用域在类型作用域内，且无法隐式转换
enum class BufferMemoryType {
    CPU_MEMORY, // 软解：cpu_ptr 有效
    DMA_BUF,    // 硬解预留：dma_buf_fd 有效
};

// 纯数据聚合体（POD），是每个步骤都用来装数据的容器
struct Buffer {
    uint8_t* cpu_ptr = nullptr;                                  // CPU 可访问的数据指针
    size_t size = 0;
    int dma_buf_fd = -1;                                         // Linux DMA-BUF 的文件描述符
    BufferMemoryType memory_type = BufferMemoryType::CPU_MEMORY; // 标记当前走的是软解码还是硬解
    int64_t pts = -1;
    int64_t dts = -1;
    void* pool_slot_ = nullptr;                                  // BufferPool 内部用，外部不要碰

    // 重置到初始状态
    void reset() {
        cpu_ptr = nullptr;
        size = 0;
        dma_buf_fd = -1;
        memory_type = BufferMemoryType::CPU_MEMORY;
        pts = -1;
        dts = -1;
        // pool_slot_ 不重置，它是 Buffer 与 Slot 的绑定关系，生命周期由 BufferPool 管理
    }

    // 有效性检查：CPU 模式下，指针不能为空；DMA 模式下，fd 必须 >= 0
    bool is_valid() const {
        return (memory_type == BufferMemoryType::CPU_MEMORY && cpu_ptr != nullptr) || 
                (memory_type == BufferMemoryType::DMA_BUF && dma_buf_fd >= 0);
    }
};

} // namespace pipeline