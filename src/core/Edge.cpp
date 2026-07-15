#include "pipeline/core/Edge.h"

namespace pipeline {

namespace {
constexpr size_t ROUTE_CAPACITY_VIDEO_RAW = 4;
constexpr size_t ROUTE_CAPACITY_AUDIO_RAW = 50;
constexpr size_t ROUTE_CAPACITY_ENCODED = 32;
constexpr size_t ROUTE_CAPACITY_CONTAINER = 32;
} // namespace

std::unique_ptr<Edge> Edge::create(
    BaseNode* src, const std::string& src_pad,
    BaseNode* dst, const std::string& dst_pad)
{
    auto edge = std::make_unique<Edge>();
    edge->src_node     = src;
    edge->src_pad_name = src_pad;
    edge->dst_node     = dst;
    edge->dst_pad_name = dst_pad;
    return edge;
}

size_t selectRouteCapacity(MediaType type) {
    switch (type) {
        case MediaType::VIDEO_RAW:     return ROUTE_CAPACITY_VIDEO_RAW;
        case MediaType::AUDIO_RAW:     return ROUTE_CAPACITY_AUDIO_RAW;
        case MediaType::VIDEO_ENCODED:
        case MediaType::AUDIO_ENCODED: return ROUTE_CAPACITY_ENCODED;
        case MediaType::CONTAINER:     return ROUTE_CAPACITY_CONTAINER;
    }
    return ROUTE_CAPACITY_ENCODED;
}

} // namespace pipeline
