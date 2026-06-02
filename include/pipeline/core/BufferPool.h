#pragma once

#include "Buffer.h"

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <vector>

namespace pipeline {

class BufferPool {
    private:
        // posix_memalign 分配的内存必须用 free() 释放，不能用 delete[]
        struct FreeDeleter {
            void operator()(uint8_t* ptr) const { std::free(ptr); }
        };

        // 池子内部的最小管理单元
        struct Slot {
            std::unique_ptr<uint8_t[], FreeDeleter> memory; // posix_memalign 分配，free() 释放
            Buffer buffer;                                  // 向外暴露的接口，其中 cpu_ptr 指向 memory
            std::atomic<int> refcount{0};
        };

        const size_t block_size_;                  // 每块内存多大
        const size_t block_count_;                 // 总共多少块
        const size_t alignment_;                   // 内存对齐字节数

        std::vector<std::unique_ptr<Slot>> slots_; // 所有 slot，索引 = Buffer 指针
        std::vector<Slot*> free_list_;             // 空闲 slot

        std::mutex mutex_;                         // 普通 mutex
        std::condition_variable cv_;               // 普通 condition_variable

        void return_to_pool(Slot* slot);
    public:
        BufferPool(size_t block_size, size_t block_count, size_t alignment = 4096);
        ~BufferPool();

        BufferPool(const BufferPool&) = delete;
        BufferPool& operator=(const BufferPool&) = delete;

        // 从池中取一块，池空时阻塞等待
        Buffer* acquire();
        // 归还一块到池中（refcount 归零时自动调用）,也可手动归还，线程安全
        void release(Buffer* buf);
        // 增加引用计数（把 Buffer 传给下一节点前调用）
        void add_ref(Buffer* buf);

        size_t block_size() const {
            return block_size_; 
        }
        size_t block_count() const {
            return block_count_;
        }
};

} // namespace pipeline