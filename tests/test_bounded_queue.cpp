#include "pipeline/core/BoundedQueue.h"
#include "pipeline/core/Buffer.h"
#include "pipeline/core/Event.h"

#include <cassert>
#include <cstdio>
#include <thread>
#include <chrono>

using namespace pipeline;

// 测试 1：基本 push/pop
void testBasicPushPop() {
    printf("  testBasicPushPop...");
    BoundedQueue<int> q(4);

    assert(q.push(1));
    assert(q.push(2));
    assert(q.size() == 2);

    auto v = q.pop(std::chrono::milliseconds(100));
    assert(v.has_value() && v.value() == 1);

    v = q.pop(std::chrono::milliseconds(100));
    assert(v.has_value() && v.value() == 2);

    assert(q.empty());
    printf(" OK\n");
}

// 测试 2：pop 超时返回 nullopt
void testPopTimeout() {
    printf("  testPopTimeout...");
    BoundedQueue<int> q(4);

    auto start = std::chrono::steady_clock::now();
    auto v = q.pop(std::chrono::milliseconds(50));
    auto elapsed = std::chrono::steady_clock::now() - start;

    assert(!v.has_value());
    assert(elapsed >= std::chrono::milliseconds(40));
    printf(" OK\n");
}

// 测试 3：BLOCK 策略背压——push 在队列满时最终能成功
void testBlockBackpressure() {
    printf("  testBlockBackpressure...");
    BoundedQueue<int> q(2, OverflowPolicy::BLOCK);

    assert(q.push(1));
    assert(q.push(2));
    assert(q.size() == 2);

    // 队列满，开一个线程 push(3)，它会阻塞直到有空位
    std::thread producer([&] {
        q.push(3);  // 阻塞等待
    });

    // 消费一个，腾出空位，producer 的 push 应该成功
    auto v = q.pop(std::chrono::milliseconds(1000));
    assert(v.has_value() && v.value() == 1);

    producer.join();  // producer 应该已经完成

    // 验证队列里有 2 和 3
    v = q.pop(std::chrono::milliseconds(100));
    assert(v.has_value() && v.value() == 2);
    v = q.pop(std::chrono::milliseconds(100));
    assert(v.has_value() && v.value() == 3);

    printf(" OK\n");
}

// 测试 4：DROP_OLDEST 策略
void testDropOldest() {
    printf("  testDropOldest...");
    BoundedQueue<int> q(3, OverflowPolicy::DROP_OLDEST);

    assert(q.push(1));
    assert(q.push(2));
    assert(q.push(3));

    // 队列满，push(4) 应该丢掉最老的 (1)
    assert(q.push(4));
    assert(q.size() == 3);

    auto v = q.pop(std::chrono::milliseconds(100));
    assert(v.has_value() && v.value() == 2);  // 1 被丢了

    v = q.pop(std::chrono::milliseconds(100));
    assert(v.has_value() && v.value() == 3);

    v = q.pop(std::chrono::milliseconds(100));
    assert(v.has_value() && v.value() == 4);

    printf(" OK\n");
}

// 测试 5：DROP_NEWEST 策略
void testDropNewest() {
    printf("  testDropNewest...");
    BoundedQueue<int> q(2, OverflowPolicy::DROP_NEWEST);

    assert(q.push(1));
    assert(q.push(2));

    // 队列满，push(3) 应该丢掉新来的 (3)
    assert(!q.push(3));
    assert(q.size() == 2);

    auto v = q.pop(std::chrono::milliseconds(100));
    assert(v.has_value() && v.value() == 1);

    v = q.pop(std::chrono::milliseconds(100));
    assert(v.has_value() && v.value() == 2);

    printf(" OK\n");
}

// 测试 6：flush 唤醒阻塞的 pop
void testFlushWakesPop() {
    printf("  testFlushWakesPop...");
    BoundedQueue<int> q(4);

    bool popped = false;
    std::thread consumer([&] {
        auto v = q.pop(std::chrono::milliseconds(5000));
        popped = true;
        assert(!v.has_value());  // flush 后返回 nullopt
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    assert(!popped);

    q.flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    assert(popped);

    consumer.join();
    printf(" OK\n");
}

// 测试 7：flush 后 push 失败
void testFlushRejectsPush() {
    printf("  testFlushRejectsPush...");
    BoundedQueue<int> q(4);

    q.flush();
    assert(!q.push(1));
    assert(q.isFlushed());
    printf(" OK\n");
}

// 测试 8：flush 后 pop 已有数据仍然能取
void testFlushDrainsRemaining() {
    printf("  testFlushDrainsRemaining...");
    BoundedQueue<int> q(4);

    q.push(1);
    q.push(2);
    q.flush();  // flush 清空队列

    auto v = q.pop(std::chrono::milliseconds(100));
    assert(!v.has_value());  // 队列已空
    printf(" OK\n");
}

// 测试 9：多线程生产者-消费者
void testProducerConsumer() {
    printf("  testProducerConsumer...");
    BoundedQueue<int> q(8);

    const int count = 100;
    std::thread producer([&] {
        for (int i = 0; i < count; i++) {
            q.push(i);
        }
    });

    int sum = 0;
    std::thread consumer([&] {
        for (int i = 0; i < count; i++) {
            auto v = q.pop(std::chrono::milliseconds(1000));
            assert(v.has_value());
            sum += v.value();
        }
    });

    producer.join();
    consumer.join();

    int expected = count * (count - 1) / 2;
    assert(sum == expected);
    printf(" OK\n");
}

int main() {
    printf("=== BoundedQueue Tests ===\n");

    testBasicPushPop();
    testPopTimeout();
    testBlockBackpressure();
    testDropOldest();
    testDropNewest();
    testFlushWakesPop();
    testFlushRejectsPush();
    testFlushDrainsRemaining();
    testProducerConsumer();

    printf("\nAll tests passed.\n");
    return 0;
}
