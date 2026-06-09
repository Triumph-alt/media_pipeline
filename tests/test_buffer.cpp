#include "pipeline/core/Buffer.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

using namespace pipeline;

// 测试 1：MemoryPool 分级分配——小块走 TINY，大块走 LARGE
void testTierSelection() {
    printf("  testTierSelection...");
    MemoryPool::TierConfig configs[MemoryPool::TIER_COUNT] = {
        {  4 * 1024,          8  },
        { 64 * 1024,          4  },
        { 512 * 1024,         4  },
        {  4 * 1024 * 1024,   2  },
        { 16 * 1024 * 1024,   1  },
    };
    MemoryPool pool(configs);
    pool.init();

    // 100 字节应该走 TINY (4KB)
    auto a = pool.alloc(100);
    assert(a->capacity() == 4 * 1024);
    assert(a->tier() == MemoryTier::TINY);

    // 100KB 应该走 MEDIUM (512KB)
    auto b = pool.alloc(100 * 1024);
    assert(b->capacity() == 512 * 1024);
    assert(b->tier() == MemoryTier::MEDIUM);

    // 5MB 应该走 HUGE (16MB)
    auto c = pool.alloc(5 * 1024 * 1024);
    assert(c->capacity() == 16 * 1024 * 1024);
    assert(c->tier() == MemoryTier::HUGE);

    printf(" OK\n");
}

// 测试 2：alloc 后数据可读写
void testAllocReadWrite() {
    printf("  testAllocReadWrite...");
    MemoryPool::TierConfig configs[MemoryPool::TIER_COUNT] = {
        {  4 * 1024,   8 },
        { 64 * 1024,   4 },
        { 512 * 1024,  4 },
        {  4 * 1024 * 1024, 2 },
        { 16 * 1024 * 1024, 1 },
    };
    MemoryPool pool(configs);
    pool.init();

    auto block = pool.alloc(128);
    memset(block->data(), 0xAB, 128);
    assert(static_cast<uint8_t*>(block->data())[0] == 0xAB);
    assert(static_cast<uint8_t*>(block->data())[127] == 0xAB);
    printf(" OK\n");
}

// 测试 3：释放后池恢复空闲块
void testReleaseRestoresBlock() {
    printf("  testReleaseRestoresBlock...");
    MemoryPool::TierConfig configs[MemoryPool::TIER_COUNT] = {
        {  4 * 1024,   2 },  // TINY 只有 2 块
        { 64 * 1024,   4 },
        { 512 * 1024,  4 },
        {  4 * 1024 * 1024, 2 },
        { 16 * 1024 * 1024, 1 },
    };
    MemoryPool pool(configs);
    pool.init();

    // 分配 2 块 TINY，池耗尽
    auto a = pool.alloc(100);
    auto b = pool.alloc(100);

    // 第 3 块应该 fallback 到 malloc
    auto c = pool.alloc(100);
    assert(c->tier() == MemoryTier::COUNT);  // fallback

    // 释放 a，池恢复 1 块
    a.reset();

    // 再分配应该从池里取
    auto d = pool.alloc(100);
    assert(d->tier() == MemoryTier::TINY);  // 从池里取的

    printf(" OK\n");
}

// 测试 4：Buffer::fromRawData
void testBufferFromRawData() {
    printf("  testBufferFromRawData...");
    MemoryPool::TierConfig configs[MemoryPool::TIER_COUNT] = {
        {  4 * 1024,   8 },
        { 64 * 1024,   4 },
        { 512 * 1024,  4 },
        {  4 * 1024 * 1024, 2 },
        { 16 * 1024 * 1024, 1 },
    };
    MemoryPool pool(configs);
    pool.init();

    uint32_t value = 42;
    auto buf = Buffer::fromRawData(&value, sizeof(uint32_t), &pool);

    assert(buf->size == sizeof(uint32_t));
    assert(*static_cast<uint32_t*>(buf->data->data()) == 42);
    printf(" OK\n");
}

// 测试 5：Buffer 引用计数——多持有者共享同一块内存
void testBufferSharedOwnership() {
    printf("  testBufferSharedOwnership...");
    MemoryPool::TierConfig configs[MemoryPool::TIER_COUNT] = {
        {  4 * 1024,   8 },
        { 64 * 1024,   4 },
        { 512 * 1024,  4 },
        {  4 * 1024 * 1024, 2 },
        { 16 * 1024 * 1024, 1 },
    };
    MemoryPool pool(configs);
    pool.init();

    auto buf1 = Buffer::fromRawData("hello", 5, &pool);
    auto buf2 = buf1;  // 共享

    assert(buf1->data.get() == buf2->data.get());  // 同一块内存
    buf1.reset();
    assert(buf2->size == 5);  // buf2 还在
    printf(" OK\n");
}

// 测试 6：多线程并发 alloc/release
void testConcurrentAllocRelease() {
    printf("  testConcurrentAllocRelease...");
    MemoryPool::TierConfig configs[MemoryPool::TIER_COUNT] = {
        {  4 * 1024,   32 },
        { 64 * 1024,   16 },
        { 512 * 1024,   8 },
        {  4 * 1024 * 1024, 4 },
        { 16 * 1024 * 1024, 2 },
    };
    MemoryPool pool(configs);
    pool.init();

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&pool] {
            for (int i = 0; i < 50; i++) {
                auto block = pool.alloc(100);
                memset(block->data(), i & 0xFF, 100);
                // block 在这里释放
            }
        });
    }

    for (auto& t : threads) t.join();
    printf(" OK\n");
}

// 测试 7：MemoryBlock::fromExternal 零拷贝
void testFromExternal() {
    printf("  testFromExternal...");
    uint8_t externalData[] = {1, 2, 3, 4, 5};
    bool callbackCalled = false;

    {
        auto block = MemoryBlock::fromExternal(
            externalData, 5,
            [&callbackCalled]() { callbackCalled = true; });

        assert(block->data() == externalData);
        assert(block->size() == 5);
        assert(block->tier() == MemoryTier::COUNT);
        assert(!callbackCalled);

        // block 释放时触发回调
    }

    assert(callbackCalled);
    printf(" OK\n");
}

int main() {
    printf("=== Buffer Tests ===\n");

    testTierSelection();
    testAllocReadWrite();
    testReleaseRestoresBlock();
    testBufferFromRawData();
    testBufferSharedOwnership();
    testConcurrentAllocRelease();
    testFromExternal();

    printf("\nAll tests passed.\n");
    return 0;
}
