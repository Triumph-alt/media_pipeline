#include "pipeline/core/Pad.h"
#include "pipeline/core/Edge.h"

namespace pipeline {

// ===================================================================
// SrcPad: 通过 Edge Queue 推送数据
// ===================================================================

void SrcPad::pushBlocking(QueueItem item) {
    if (edge_ && edge_->queue) {
        edge_->queue->pushBlocking(std::move(item));
    }
}

bool SrcPad::tryPush(QueueItem item) {
    if (edge_ && edge_->queue) {
        return edge_->queue->tryPush(std::move(item));
    }
    return false;
}

// ===================================================================
// SinkPad: 从 Edge Queue 拉取数据
// ===================================================================

std::optional<QueueItem> SinkPad::popBlocking() {
    if (edge_ && edge_->queue) {
        return edge_->queue->popBlocking();
    }
    return std::nullopt;
}

std::optional<QueueItem> SinkPad::tryPop() {
    if (edge_ && edge_->queue) {
        return edge_->queue->tryPop();
    }
    return std::nullopt;
}

std::optional<QueueItem> SinkPad::peek() {
    if (edge_ && edge_->queue) {
        return edge_->queue->peek();
    }
    return std::nullopt;
}

} // namespace pipeline
