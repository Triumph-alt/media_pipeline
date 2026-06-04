#include "FileSource.h"

#include <cassert>
#include <cstring>

extern "C" {
#include <libavformat/avformat.h>
#include <libavutil/error.h>
}

namespace pipeline {

FileSource::FileSource(const std::string& filename, size_t read_size)
    :filename_(filename)
    ,read_size_(read_size)
{
    assert(!filename_.empty());
    assert(read_size_ > 0);
}

FileSource::~FileSource() = default;

void FileSource::run() {
    /* 打开文件 */
    AVFormatContext* fmt_ctx = nullptr;
    int ret = avformat_open_input(&fmt_ctx, filename_.c_str(), nullptr, nullptr);
    if (ret < 0) {
        char err_buf[AV_ERROR_MAX_STRING_SIZE]{};
        av_strerror(ret, err_buf, sizeof(err_buf));
        // TODO: 替换为项目日志系统
        return;
    }

    /* 读取数据，memory_order_acquire 保障读取安全 */
    while (!stop_requested_.load(std::memory_order_acquire))
    {
        // 从池中取一块 Buffer，池空时阻塞等待
        Buffer* buf = pool_->acquire();

        // 用 avio 读一块原始字节到 Buffer
        ret = avio_read(fmt_ctx->pb, buf->cpu_ptr, static_cast<int>(read_size_));
        if (ret == AVERROR_EOF) {
            // 文件读完，释放这块未使用的 Buffer，退出
            pool_->release(buf);
            break;
        } else if (ret < 0) {
            // 读取出错，释放 Buffer，退出
            pool_->release(buf);
            break;
        }

        // 实际读到的字节数可能小于 read_size_（文件末尾）
        buf->size = static_cast<size_t>(ret);

        // 数据推送到输出队列
        output_queue_->push(buf);
    }
    
    avformat_close_input(&fmt_ctx);
}

} // namespace pipeline