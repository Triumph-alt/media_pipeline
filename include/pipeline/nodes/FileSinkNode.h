#pragma once

#include "pipeline/core/BaseNode.h"

#include <cstdint>
#include <string>

namespace pipeline {

struct FileSinkConfig {
    std::string path;
    bool overwrite = false;
};

// 顺序 CONTAINER 文件输出节点
// 节点只负责可靠写入上游容器字节，不解析或修改具体容器格式
class FileSinkNode final : public SinkNode {
public:
    FileSinkNode(const std::string& name, FileSinkConfig config);

protected:
    bool onReady() override;
    bool onCaps(const std::string& sink_pad_name, const CapsEvent& caps,
                std::vector<QueueItem>* outputs) override;
    void consume(const Buffer* buf) override;
    void onDrain() override;
    void onStop() override;

private:
    bool syncFile();
    void closeFile();

    const FileSinkConfig config_;
    int fd_ = -1;
    uint64_t bytes_written_ = 0;
};

} // namespace pipeline
