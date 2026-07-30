#pragma once

#include <cstdint>

namespace pipeline {

// 仅在 PIPELINE_LATENCY_TRACE=1 时记录真实媒体链路的分段延迟
// PTS 必须与 steady_clock 同属 monotonic 时钟域；NOPTS 样本不会输出
bool latencyTraceEnabled();
int64_t latencyTraceNowUs();
void traceStartupStep(const char* node_name, const char* stage,
                      int64_t elapsed_us, int64_t total_us);
void traceLatencySample(const char* node_name, const char* stage,
                        uint64_t sequence, int64_t pts_us,
                        int64_t dts_us, uint64_t related_sequence = 0);

} // namespace pipeline
