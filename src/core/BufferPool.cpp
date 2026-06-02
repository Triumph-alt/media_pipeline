#include "BufferPool.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <new>

namespace pipeline {

BufferPool::BufferPool(size_t block_size, size_t block_count, size_t alignment)
    : block_size_(block_size)
    , block_count_(block_count)
    , alignment_(alignment)
{
    assert(block_size > 0);
    assert(block_count > 0);
    assert(alignment >= alignof(std::max_align_t));

    slots_.reserve(block_count);
    free_list_.reserve(block_count);

    // 预分配所有内存
    for (size_t i = 0; i < block_count; ++i) {
        // 创建一个 Slot，在堆上分配并返回 unique_ptr
        auto slot = std::make_unique<Slot>();

        void* ptr = nullptr;
        if (posix_memalign(&ptr, alignment, block_size) != 0) {
            assert(false && "posix_memalign failed");
            std::abort();
        }

        // 把 void* 转为 uint8_t*，交给 unique_ptr 管理
        slot->memory.reset(static_cast<uint8_t*>(ptr));
        std::memset(slot->memory.get(), 0, block_size);

        slot->buffer.cpu_ptr = slot->memory.get();
        slot->buffer.size = block_size;
        slot->buffer.memory_type = BufferMemoryType::CPU_MEMORY;
        slot->buffer.pool_slot_ = slot.get();

        Slot* raw = slot.get();            // 拿到裸指针（不转移所有权）
        slots_.push_back(std::move(slot)); // 所有权转移给 slots_ 数组
        free_list_.push_back(raw);         // 裸指针放入空闲列表
    }
}

BufferPool::~BufferPool() {
    for (auto& slot : slots_) {
        assert( slot->refcount.load() == 0 && 
                "BufferPool destroyed while buffers are still in use");
    }
}

Buffer* BufferPool::acquire() {
    std::unique_lock<std::mutex> lock(mutex_);

    cv_.wait(lock, [this] { 
        return !free_list_.empty(); 
    });

    // 从空闲列表尾部取一个 Slot
    Slot* slot = free_list_.back();
    free_list_.pop_back();

    // 每次借出都重置 Buffer 状态
    slot->buffer.reset();
    slot->buffer.cpu_ptr = slot->memory.get();
    slot->buffer.size = block_size_;
    slot->buffer.memory_type = BufferMemoryType::CPU_MEMORY;
    slot->buffer.pool_slot_ = slot;
    slot->refcount.store(1, std::memory_order_release);

    // 返回 Slot 内部 Buffer 的地址，不是拷贝
    return &slot->buffer;
}

void BufferPool::release(Buffer* buf) {
    if (!buf || !buf->pool_slot_) {
        return;
    }

    // 通过 void* 回溯到 Slot
    Slot* slot = static_cast<Slot*>(buf->pool_slot_);

    // 原子地减 1，并返回减之前的旧值
    int old = slot->refcount.fetch_sub(1, std::memory_order_acq_rel);
    if (old == 1) {
        // 如果是最后一个使用者，归还回池
        return_to_pool(slot);
    }
}

void BufferPool::add_ref(Buffer* buf) {
    if (!buf || !buf->pool_slot_) {
        return;
    }

    Slot* slot = static_cast<Slot*>(buf->pool_slot_);
    slot->refcount.fetch_add(1, std::memory_order_acq_rel);
}

void BufferPool::return_to_pool(Slot* slot) {
    std::lock_guard<std::mutex> lock(mutex_);
    free_list_.push_back(slot);
    cv_.notify_one();
}

}