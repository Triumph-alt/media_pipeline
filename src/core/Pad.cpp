#include "pipeline/core/Edge.h"
#include "pipeline/core/Pad.h"

namespace pipeline {

std::optional<RouteDelivery> SinkPad::acquireBlocking() {
    if (!edge_ || !edge_->subscription) {
        return std::nullopt;
    }
    return edge_->subscription.acquireBlocking();
}

std::optional<RouteDelivery> SinkPad::tryAcquire() {
    if (!edge_ || !edge_->subscription) {
        return std::nullopt;
    }
    return edge_->subscription.tryAcquire();
}

std::optional<QueueItem> SinkPad::peek() const {
    if (!edge_ || !edge_->subscription) {
        return std::nullopt;
    }
    return edge_->subscription.peek();
}

void SinkPad::setRouteNotify(std::function<void()> callback) {
    if (!edge_ || !edge_->subscription) {
        return;
    }
    auto route = edge_->subscription.route_;
    if (route) {
        route->setNotifyCallback(std::move(callback));
    }
}

} // namespace pipeline
