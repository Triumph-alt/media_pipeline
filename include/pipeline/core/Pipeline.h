#pragma once

#include "pipeline/core/Clock.h"
#include "pipeline/core/Graph.h"
#include "pipeline/core/MessageBus.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace pipeline {

// ===================================================================
// PipelineState: Pipeline 整体生命周期
//
// NULL_STATE ──(build)──→ BUILT ──(play)──→ RUNNING ──(stop)──→ STOPPING ──→ STOPPED
// ===================================================================
enum class PipelineState {
    NULL_STATE,
    BUILT,
    RUNNING,
    STOPPING,   // CAS 占位，保证只有一个线程执行清理
    STOPPED,
    ERROR,
};

// ===================================================================
// Pipeline: 管线管理器
//
// 职责：
//   - 持有 Graph、Clock、MessageBus
//   - 驱动三阶段启动：build() → play() → stop()
//   - 统一创建和管理所有节点线程
//   - 内部运行 MessageBus 监听线程，统一处理 EOS/ERROR/STOP_REQUESTED/WARNING/INFO
//   - waitEOS() 阻塞等待所有 Sink 收到 EOS、节点错误或节点停止请求
//
// 用户侧暴露两个接口，可并发使用：
//   pipeline.waitEOS() — 阻塞等待自然结束、错误或节点停止请求，内部自动调 stop()
//   pipeline.stop()    — 用户主动停止（可从信号处理、另一线程等调用）
//
// stop() 内部用 CAS 保证只有一个线程执行清理，可安全重复调用。
// Pipeline 不提供通用应用输入监听；VideoRenderNode 的窗口关闭请求通过 MessageBus
// 唤醒 waitEOS()，最终仍由 waitEOS() 所在线程调用 stop()。
//
// 进程约束：同一进程同一时刻至多允许一个存活的 Pipeline 实例。Pipeline 构造/析构
// 直接管理进程全局 SDL 基础设施的 SDL_Init(0)/SDL_Quit()，不支持并存 Pipeline。
// ===================================================================

class Pipeline {
public:
    Pipeline();
    ~Pipeline();

    // 禁止拷贝/移动
    Pipeline(const Pipeline&) = delete;
    Pipeline& operator=(const Pipeline&) = delete;

    // 添加节点
    template<typename T, typename... Args>
    T* addNode(const std::string& name, Args&&... args);

    // 声明连接
    bool link(BaseNode* src, const std::string& src_pad,
              BaseNode* dst, const std::string& dst_pad,
              MediaType hint_type = MediaType::CONTAINER);

    // 生命周期
    bool build();
    bool play();
    void waitEOS();
    void stop();

    // 访问
    Clock*      clock() { return &clock_; }
    MessageBus* bus()   { return &bus_; }
    std::string lastError() {
        std::lock_guard lock(error_mutex_);
        return last_error_;
    }

private:
    // MessageBus 监听线程主循环
    void messageBusLoop();

    Graph graph_;
    Clock clock_;
    MessageBus bus_;

    // 节点线程
    std::unordered_map<BaseNode*, std::thread> threads_;

    // MessageBus 监听线程
    std::thread bus_thread_;
    std::atomic<bool> bus_running_{false};

    // EOS / Error 状态
    std::atomic<int> active_sink_count_{0};
    std::atomic<bool> error_occurred_{false};
    std::atomic<bool> stop_requested_by_node_{false};
    std::string last_error_;
    std::mutex error_mutex_;       // 保护 last_error_
    std::mutex eos_mutex_;
    std::condition_variable eos_cv_;

    // state_ 用 atomic + CAS 保证 stop() 幂等和并发安全
    std::atomic<PipelineState> state_{PipelineState::NULL_STATE};
};

// ===================================================================
// addNode: 模板方法，在头文件中实现
// ===================================================================
template<typename T, typename... Args>
T* Pipeline::addNode(const std::string& name, Args&&... args) {
    if (state_ != PipelineState::NULL_STATE) {
        return nullptr;
    }
    auto node = std::make_unique<T>(name, std::forward<Args>(args)...);
    node->pipeline_ = this;
    T* ptr = node.get();
    graph_.addNode(std::move(node));
    return ptr;
}

} // namespace pipeline
