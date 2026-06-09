#pragma once

#include "StreamInfo.h"

#include <memory>
#include <optional>

namespace pipeline {

class Event {
public:
    enum class Type {
        EOS,                    // 流结束
        FLUSH_START,            // 开始 flush
        FLUSH_DONE,             // flush 完成
        STREAM_INFO_CHANGED,    // 流参数变化
        SEEK,                   // seek 请求
        CUSTOM,                 // 用户自定义
    };

    Type type;
    int streamIndex = -1;       // 针对哪个流（-1 表示全局）

    // 附加数据（按需使用）
    std::optional<int64_t> seekPosition;         // SEEK 时有效
    std::optional<StreamInfo> changedStreamInfo; // STREAM_INFO_CHANGED 时有效

    // 工厂方法，负责创建对象并返回
    static std::shared_ptr<Event> makeEOS(int streamIdx = -1);
    static std::shared_ptr<Event> makeFlushStart();
    static std::shared_ptr<Event> makeFlushDone();
    static std::shared_ptr<Event> makeStreamInfoChanged(const StreamInfo& info);
    static std::shared_ptr<Event> makeSeek(int64_t position);

private:
    Event() = default;
};

} // namespace pipeline
