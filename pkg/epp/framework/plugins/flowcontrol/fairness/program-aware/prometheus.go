/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package programaware

import (
	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

var (
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "program_aware_requests_total",
			Help:      metricsutil.HelpMsgWithStability("Total requests received per program", compbasemetrics.ALPHA),
		},
		[]string{"program_id"},
	)

	dispatchedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "program_aware_dispatched_total",
			Help:      metricsutil.HelpMsgWithStability("Total requests dispatched per program", compbasemetrics.ALPHA),
		},
		[]string{"program_id"},
	)

	inputTokensTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "program_aware_input_tokens_total",
			Help:      metricsutil.HelpMsgWithStability("Total input (prompt) tokens consumed per program", compbasemetrics.ALPHA),
		},
		[]string{"program_id"},
	)

	outputTokensTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "program_aware_output_tokens_total",
			Help:      metricsutil.HelpMsgWithStability("Total output (completion) tokens produced per program", compbasemetrics.ALPHA),
		},
		[]string{"program_id"},
	)

	pickLatencyUs = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "program_aware_pick_latency_microseconds",
			Help:      metricsutil.HelpMsgWithStability("Latency of the Pick() call in microseconds", compbasemetrics.ALPHA),
			Buckets:   []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000},
		},
	)

	fairnessIndex = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "program_aware_jains_fairness_index",
			Help:      metricsutil.HelpMsgWithStability("Jain's fairness index over average wait time across active programs.", compbasemetrics.ALPHA),
		},
	)

	avgWaitTimeMs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "program_aware_avg_wait_time_milliseconds",
			Help:      metricsutil.HelpMsgWithStability("Cumulative mean of flow-control queue wait time per program in milliseconds.", compbasemetrics.ALPHA),
		},
		[]string{"program_id"},
	)

	serviceRateTokensPerSec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "program_aware_service_rate_tokens_per_second",
			Help:      metricsutil.HelpMsgWithStability("EWMA of weighted tokens per second per program (used for Jain's fairness index)", compbasemetrics.ALPHA),
		},
		[]string{"program_id"},
	)

	queueScore = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
			Name:      "program_aware_queue_score",
			Help:      metricsutil.HelpMsgWithStability("Scheduling priority score computed by the scoring strategy for each program queue during Pick()", compbasemetrics.ALPHA),
		},
		[]string{"program_id"},
	)
)

func DeleteSharedSeries(id string) {
	requestsTotal.DeleteLabelValues(id)
	dispatchedTotal.DeleteLabelValues(id)
	inputTokensTotal.DeleteLabelValues(id)
	outputTokensTotal.DeleteLabelValues(id)
	avgWaitTimeMs.DeleteLabelValues(id)
	serviceRateTokensPerSec.DeleteLabelValues(id)
	queueScore.DeleteLabelValues(id)
}

func GetCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		requestsTotal,
		dispatchedTotal,
		inputTokensTotal,
		outputTokensTotal,
		pickLatencyUs,
		fairnessIndex,
		avgWaitTimeMs,
		serviceRateTokensPerSec,
		queueScore,
	}
}
