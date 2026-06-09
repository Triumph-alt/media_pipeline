#include "pipeline/core/Event.h"

namespace pipeline {

std::shared_ptr<Event> Event::makeEOS(int streamIdx) {
    auto e = std::shared_ptr<Event>(new Event());
    e->type = Type::EOS;
    e->streamIndex = streamIdx;
    return e;
}

std::shared_ptr<Event> Event::makeFlushStart() {
    auto e = std::shared_ptr<Event>(new Event());
    e->type = Type::FLUSH_START;
    return e;
}

std::shared_ptr<Event> Event::makeFlushDone() {
    auto e = std::shared_ptr<Event>(new Event());
    e->type = Type::FLUSH_DONE;
    return e;
}

std::shared_ptr<Event> Event::makeStreamInfoChanged(const StreamInfo& info) {
    auto e = std::shared_ptr<Event>(new Event());
    e->type = Type::STREAM_INFO_CHANGED;
    e->changedStreamInfo = info;
    return e;
}

std::shared_ptr<Event> Event::makeSeek(int64_t position) {
    auto e = std::shared_ptr<Event>(new Event());
    e->type = Type::SEEK;
    e->seekPosition = position;
    return e;
}

} // namespace pipeline
