# Program-Aware Fairness Plugin

**Type:** `program-aware-fairness`
**Interfaces:** `flowcontrol.FairnessPolicy`, `requestcontrol.DataProducer`, `requestcontrol.PreRequest`, `requestcontrol.ResponseBodyProcessor`, `plugin.StateDumper`

Program-level fairness for agentic workloads: requests are grouped by program ID, and dispatch decisions are made on aggregated per-program metrics rather than per-request attributes.

## What It Does

Agentic workloads (coding agents, research pipelines, multi-step reasoning chains) generate sequences of LLM inference requests that form a logical program. Scheduling those requests individually ignores the program-level context: one program may have consumed far more compute than another, or a program's requests may be consistently starved while others proceed.

This plugin recognizes that requests belong to higher-level programs and:

- **Identifies programs** via the fairness ID header (`x-gateway-inference-fairness-id` or `x-llm-d-inference-fairness-id`).
- **Tracks program-level metrics** across the full request lifecycle — accumulated token usage, queue wait times, dispatch counts, and service rates.
- **Selects which program to dispatch next** using a configurable scoring strategy that compares per-program metrics.

## Strategies

The `strategy` config field selects the scoring algorithm.

| Strategy | `strategy` value | Description |
|---|---|---|
| Least-Attained Service | `las` (default) | Equitable resource allocation; promotes underserved programs |
| Deficit Round Robin | `drr` | Proportional bandwidth allocation for variable request sizes |
| Round-Robin | `rr` | Equal turns regardless of usage |

## Configuration

```yaml
plugins:
  - type: program-aware-fairness
    parameters:
      strategy: las
      weightService: 0.8
      weightServiceHeadWait: 0.2
      serviceHalfLifeSeconds: 60
      evictionTtlSeconds: 3600
      evictionSweepSeconds: 300

flowControl:
  defaultPriorityBand:
    fairnessPolicyRef: program-aware-fairness
```

## Observability

Exposes Prometheus metrics under the `llm_d_epp` subsystem:
- `program_aware_jains_fairness_index`
- `program_aware_avg_wait_time_milliseconds`
- `program_aware_attained_service_tokens`
- `program_aware_requests_total`
- `program_aware_dispatched_total`
