#include "pipeline/core/Pipeline.h"

namespace pipeline {

// ===================================================================
// link: 声明连接，委托给 Graph
// ===================================================================
bool Pipeline::link(BaseNode* src, const std::string& src_pad,
                    BaseNode* dst, const std::string& dst_pad,
                    MediaType hint_type)
{
    if (state_ != PipelineState::NULL_STATE) {
        return false;
    }
    return graph_.link(src, src_pad, dst, dst_pad, hint_type);
}

// ===================================================================
// build: Build 阶段，静态校验 + 拓扑排序
// ===================================================================
bool Pipeline::build() {
    if (state_ != PipelineState::NULL_STATE) {
        return false;
    }

    if (!graph_.build()) {
        state_ = PipelineState::ERROR;
        return false;
    }

    state_ = PipelineState::BUILT;
    return true;
}

// ===================================================================
// play: 驱动 Ready → Running
// ===================================================================
bool Pipeline::play() {
    if (state_ != PipelineState::BUILT) return false;

    // 重置墙钟基准（Clock::reset 注释：play 时调用）
    clock_.reset();

    // 先启动 MessageBus 监听线程：Ready 阶段节点 postMessage(ERROR/WARNING/INFO)
    // 需要有人接收，否则 lastError() 在 Ready 失败路径上永远为空。
    // 顺序必须早于 graph_.ready()。
    bus_running_ = true;
    bus_thread_ = std::thread([this]() { messageBusLoop(); });

    // Ready 阶段：三步穿插初始化
    if (!graph_.ready()) {
        // Ready 失败：先把 bus 收干净，保证 Ready 期间的 ERROR 消息全部落入 last_error_，
        // 然后再置 ERROR 返回。bus_running_ 翻为 false 后 notify()，
        // waitMessage 的“队列非空优先返回消息”语义保证 pending 消息不会丢。
        bus_running_ = false;
        bus_.notify();
        if (bus_thread_.joinable()) bus_thread_.join();
        state_ = PipelineState::ERROR;
        return false;
    }

    // 统计 SinkNode 数量，用于 EOS 计数
    for (auto* node : graph_.topoOrder()) {
        if (node->nodeType() == NodeType::SINK) {
            active_sink_count_++;
        }
    }

    // 按拓扑逆序启动节点线程（Sink 先，Source 后）
    auto& order = graph_.topoOrder();
    for (auto it = order.rbegin(); it != order.rend(); ++it) {
        threads_[*it] = std::thread([node = *it]() { node->runLoop(); });
    }

    state_ = PipelineState::RUNNING;
    return true;
}

// ===================================================================
// stop: 停止所有线程，释放资源
// ===================================================================
void Pipeline::stop() {
    // CAS：只有第一个到达的线程执行清理，其他线程直接返回
    PipelineState expected = PipelineState::RUNNING;
    if (!state_.compare_exchange_strong(expected, PipelineState::STOPPING)) {
        return;
    }

    // 1. 设置所有节点退出标志
    for (auto& [node, _] : threads_) {
        node->stop_requested_.store(true);
    }

    // 2. cancel 所有 OutputRoute，唤醒阻塞中的 publish/acquire 以及 Mux 外部等待
    graph_.cancelAllRoutes();

    // 3. join 所有节点线程
    for (auto& [node, thread] : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads_.clear();

    // 4. 停止 MessageBus 监听线程
    bus_running_ = false;
    bus_.notify();
    if (bus_thread_.joinable()) bus_thread_.join();

    // 5. 按拓扑逆序调用 onStop()（资源层面释放）
    auto& order = graph_.topoOrder();
    for (auto it = order.rbegin(); it != order.rend(); ++it) {
        (*it)->onStop();
    }

    state_ = PipelineState::STOPPED;
    eos_cv_.notify_all();  // 唤醒可能在阻塞的 waitEOS()
}

// ===================================================================
// waitEOS: 阻塞等待所有 Sink 正常 EOS、节点错误或节点 STOP_REQUESTED
// ===================================================================
void Pipeline::waitEOS() {
    std::unique_lock lock(eos_mutex_);
    eos_cv_.wait(lock, [this] {
        return active_sink_count_.load() == 0
            || error_occurred_.load()
            || stop_requested_by_node_.load()
            || state_.load() != PipelineState::RUNNING;  // 外部 stop() 已执行
    });
    lock.unlock();

    // CAS 自动处理"已经被停过"的情况
    stop();
}

// ===================================================================
// messageBusLoop: MessageBus 监听线程主循环。
// STOP_REQUESTED 只设置状态并唤醒 waitEOS，由 waitEOS 所在线程执行 stop。
// ===================================================================
void Pipeline::messageBusLoop() {
    while (true) {
        auto msg = bus_.waitMessage(bus_running_);
        if (!msg) break;  // bus_running_ 为 false 且队列空，退出

        switch (msg->type) {
            case MessageType::EOS:
                if (--active_sink_count_ == 0) {
                    eos_cv_.notify_all();
                }
                break;

            case MessageType::ERROR:
                error_occurred_ = true;
                {
                    std::lock_guard lock(error_mutex_);
                    last_error_ = msg->text;
                }
                eos_cv_.notify_all();
                break;

            case MessageType::STOP_REQUESTED:
                stop_requested_by_node_.store(true);
                eos_cv_.notify_all();
                break;

            case MessageType::WARNING:
            case MessageType::INFO:
                bus_.notifyObserver(*msg);
                break;
        }
    }
}

} // namespace pipeline
