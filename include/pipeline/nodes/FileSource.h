#pragma once

#include "pipeline/core/INode.h"

#include <cstddef>
#include <string>

namespace pipeline {

class FileSource : public INode
{
    public:
        // filename 是输入文件路径，read_size 是每次读取的字节数
        explicit FileSource(const std::string& filename, size_t read_size = 65536);
        ~FileSource() override;

        const char* name() const override { 
            return "FileSource"; 
        }

    protected:
        // Source 节点重写 run()，自己产生数据
        void run() override;

        // Source 节点不处理 Buffer，process() 不会被调用
        void process(Buffer* /*buf*/) override {

        }

    private:
        std::string filename_;
        size_t read_size_;
};

} // namespace pipeline