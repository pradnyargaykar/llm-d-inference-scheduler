# Comparison: aware-02-vs-default-vs-session

Runs: decode-default, decode-session-affinity, program-aware-single-02

| metric | decode-default | decode-session-affinity | program-aware-single-02 |
|---|---|---|---|
| requests | 33,138 | 33,140 | 33,140 |
| failures | 2 | 1 | 1 |
| benchmark_time_s | 6,675 | 5,358 | 4,193 |
| sessions | 1,600 | 1,600 | 1,600 |
| sessions_failed | 2 | 1 | 1 |
| input_tokens_per_sec | 67,234 | 83,728 | 101,811 |
| output_tokens_per_sec | 4,106 | 5,155 | 4,407 |
| total_tokens_per_sec | 71,340 | 88,883 | 106,217 |
| requests_per_sec | 4.9646 | 6.1852 | 7.9045 |
| request_latency p50 | 9.9147 | 10.2731 | 7.0561 |
| request_latency p90 | 90.6115 | 89.8239 | 43.0912 |
| request_latency p99 | 335.0245 | 268.9281 | 223.9852 |
| request_latency mean | 35.9746 | 34.0029 | 22.1092 |
| ttft p50 | 2.1603 | 2.3016 | 2.133 |
| ttft p90 | 16.6677 | 17.2927 | 11.3333 |
| ttft p99 | 72.6786 | 71.6801 | 48.5892 |
| ttft mean | 7.1684 | 7.0248 | 5.0264 |
| tpot p50 | 0.0308 | 0.033 | 0.0284 |
| tpot p90 | 0.0509 | 0.0491 | 0.048 |
| tpot p99 | 0.0753 | 0.0601 | 0.0623 |
| tpot mean | 0.0308 | 0.0314 | 0.0286 |
| itl p50 | 0.0246 | 0.0294 | 0.0229 |
| itl p90 | 0.1444 | 0.145 | 0.1584 |
| itl p99 | 0.2807 | 0.2968 | 0.3072 |
| itl mean | 0.0613 | 0.0582 | 0.0525 |
| session_duration p50 | 447.8112 | 431.1432 | 356.5092 |
| session_duration p90 | 2,431 | 2,322 | 1,519 |
| session_duration p99 | 4,792 | 4,050 | 2,854 |
| session_duration mean | 880.9905 | 839.4557 | 593.239 |
