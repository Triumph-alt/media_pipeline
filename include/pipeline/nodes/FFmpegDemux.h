#pragma once

#include "pipeline/core/INode.h"

#include <string>

struct AVPacket;

namespace pipeline {

class FFmpegDemux : public INode {
public:
    // uri: 文件路径或网络流 URL（如 "rtmp://...", "http://.../stream.m3u8"）
    explicit FFmpegDemux(const std::string& uri);
    ~FFmpegDemux() override;

    const char* name() const override { 
        return "FFmpegDemux"; 
    }

    // 输出队列约定（继承自 INode，使用数组接口）：
    //   output_queues_[0] = 视频流
    //   output_queues_[1] = 音频流
    // 构建 pipeline 时通过 set_output_queue(q, index) 手动注入

protected:
    void run() override;
    void process(Buffer* /*buf*/) override {}

private:
    std::string uri_;

    // 把 AVPacket 数据拷进 Buffer，设置 pts/dts
    // 返回 false 表示 Buffer 太小装不下，调用方负责 release
    bool fill_buffer(Buffer* buf, AVPacket* pkt, int64_t time_base_num, int64_t time_base_den);
};

} // namespace pipeline
