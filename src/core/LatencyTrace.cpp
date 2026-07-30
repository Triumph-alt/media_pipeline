#include "pipeline/core/LatencyTrace.h"

extern "C" {
#include <libavutil/avutil.h>
}

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace pipeline {
namespace {

int64_t steadyNowUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

} // namespace

bool latencyTraceEnabled() {
    static const bool enabled = [] {
        const char* value = std::getenv("PIPELINE_LATENCY_TRACE");
        return value && std::strcmp(value, "1") == 0;
    }();
    return enabled;
}

int64_t latencyTraceNowUs() {
    return steadyNowUs();
}

void traceStartupStep(const char* node_name, const char* stage,
                      int64_t elapsed_us, int64_t total_us) {
    if (!latencyTraceEnabled()) {
        return;
    }

    fprintf(stderr,
            "[startup] node=%s stage=%s elapsed_us=%lld total_us=%lld now_us=%lld\n",
            node_name ? node_name : "", stage ? stage : "",
            static_cast<long long>(elapsed_us), static_cast<long long>(total_us),
            static_cast<long long>(steadyNowUs()));
}

void traceLatencySample(const char* node_name, const char* stage,
                        uint64_t sequence, int64_t pts_us,
                        int64_t dts_us, uint64_t related_sequence) {
    if (!latencyTraceEnabled() || pts_us == AV_NOPTS_VALUE) {
        return;
    }

    const int64_t age_us = steadyNowUs() - pts_us;
    fprintf(stderr,
            "[latency] node=%s stage=%s seq=%llu related=%llu pts_us=%lld "
            "dts_us=%lld age_us=%lld\n",
            node_name ? node_name : "", stage ? stage : "",
            static_cast<unsigned long long>(sequence),
            static_cast<unsigned long long>(related_sequence),
            static_cast<long long>(pts_us), static_cast<long long>(dts_us),
            static_cast<long long>(age_us));
}

} // namespace pipeline
