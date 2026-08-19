package programaware

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
)

const scorerSubsystem = "program_aware_scorer"

var (
	// routingDecisionsTotal tracks how many times candidate endpoints were evaluated,
	// labeled by program_id, pod, and decision_type (cache_hit | cache_miss).
	routingDecisionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: scorerSubsystem,
			Name:      "routing_decisions_total",
			Help:      metricsutil.HelpMsgWithStability("Total endpoint evaluations made by the program-aware scorer, labelled by program, pod, and decision type", compbasemetrics.ALPHA),
		},
		[]string{"program_id", "pod", "decision_type"},
	)

	// endpointScore tracks the computed score for each (program_id, pod) pair at the last Score() evaluation.
	endpointScore = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: scorerSubsystem,
			Name:      "endpoint_score",
			Help:      metricsutil.HelpMsgWithStability("Computed score for each (program_id, pod) pair at the last Score() call", compbasemetrics.ALPHA),
		},
		[]string{"program_id", "pod"},
	)
)

// registerScorerMetrics registers the program-aware scorer Prometheus telemetry collectors.
func registerScorerMetrics(registerer prometheus.Registerer) error {
	if registerer == nil {
		return errors.New("program-aware scorer: metrics registerer is required")
	}
	for _, collector := range []prometheus.Collector{
		routingDecisionsTotal,
		endpointScore,
	} {
		if err := registerer.Register(collector); err != nil {
			var alreadyRegistered prometheus.AlreadyRegisteredError
			if errors.As(err, &alreadyRegistered) && alreadyRegistered.ExistingCollector == collector {
				continue
			}
			return fmt.Errorf("register program-aware scorer metric: %w", err)
		}
	}
	return nil
}
