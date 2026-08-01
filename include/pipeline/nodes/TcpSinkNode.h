#pragma once

#include "pipeline/core/BaseNode.h"

#include <cstdint>
#include <string>

namespace pipeline {

// 顺序 CONTAINER TCP 输出配置
// 首版只做主动 Connect：对端需先 listen（例如 ffmpeg/ffplay 的 tcp://host:port?listen）
struct TcpSinkConfig {
    std::string host = "127.0.0.1";
    uint16_t port = 0;
    // Ready 阶段建连总超时；超时后 onReady 失败并 ERROR
    int connect_timeout_ms = 5000;
    // consume 内每次 poll 等待可写的上限，用于周期观察 stop_requested_
    int io_poll_timeout_ms = 100;
};

// 顺序 CONTAINER TCP 输出节点
// 与 FileSinkNode 同构：只可靠写出上游容器字节，不解析或修改具体容器格式
// 使用非阻塞 socket + poll，保证 Pipeline stop 能在有限时间内打断阻塞写
class TcpSinkNode final : public SinkNode {
public:
    TcpSinkNode(const std::string& name, TcpSinkConfig config);

protected:
    bool onReady() override;
    bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override;
    void consume(const Buffer* buf) override;
    void onDrain() override;
    void onStop() override;

private:
    bool connectWithTimeout();
    bool waitSocketWritable();
    void closeSocket();

    const TcpSinkConfig config_;
    int sock_ = -1;
    uint64_t bytes_written_ = 0;
};

} // namespace pipeline
