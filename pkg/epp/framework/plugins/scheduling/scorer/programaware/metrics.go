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
	// routingDecisionsTotal tracks how many times each program was routed to each pod,
	// and whether it was a first-time, cache-hit, or budget-exhausted (forced migration) decision.
	// Labels: program_id, pod, decision_type (first_time | cache_hit | budget_exhausted)
	routingDecisionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: scorerSubsystem,
			Name:      "routing_decisions_total",
			Help:      metricsutil.HelpMsgWithStability("Total routing decisions made by the program-aware scorer, labelled by program, pod, and decision type (first_time, cache_hit, budget_exhausted)", compbasemetrics.ALPHA),
		},
		[]string{"program_id", "pod", "decision_type"},
	)

	// budgetAtScore is a gauge tracking the available budget for each (program, pod) pair
	// at the time of the last Score() call. Lets you see in real-time how budgets drain.
	budgetAtScore = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: scorerSubsystem,
			Name:      "budget_available",
			Help:      metricsutil.HelpMsgWithStability("Available budget for each (program_id, pod) pair at the last Score() call", compbasemetrics.ALPHA),
		},
		[]string{"program_id", "pod"},
	)

	// budgetReservationsTotal counts how many times budget was reserved (PreRequest).
	budgetReservationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: scorerSubsystem,
			Name:      "budget_reservations_total",
			Help:      metricsutil.HelpMsgWithStability("Total budget reservations made (in-flight request slots claimed)", compbasemetrics.ALPHA),
		},
		[]string{"program_id", "pod"},
	)

	// budgetDeductionsTotal counts how many times budget was deducted (ResponseBody, end of stream).
	budgetDeductionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: scorerSubsystem,
			Name:      "budget_deductions_total",
			Help:      metricsutil.HelpMsgWithStability("Total budget deductions (completed requests that consumed a budget slot)", compbasemetrics.ALPHA),
		},
		[]string{"program_id", "pod"},
	)

	// budgetRefreshesTotal counts how many refresh cycles fired per (program, pod).
	budgetRefreshesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: scorerSubsystem,
			Name:      "budget_refreshes_total",
			Help:      metricsutil.HelpMsgWithStability("Total budget refresh events per (program_id, pod)", compbasemetrics.ALPHA),
		},
		[]string{"program_id", "pod"},
	)

	// budgetRefreshAmount tracks how much budget was added in each refresh per (program, pod).
	budgetRefreshAmount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: scorerSubsystem,
			Name:      "budget_refresh_amount_total",
			Help:      metricsutil.HelpMsgWithStability("Cumulative budget units added by refresh cycles per (program_id, pod)", compbasemetrics.ALPHA),
		},
		[]string{"program_id", "pod"},
	)

	// forcedMigrationsTotal counts how many times a program was forced off a pod
	// because budget was exhausted (score=0 path in scoreSubsequent).
	forcedMigrationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: scorerSubsystem,
			Name:      "forced_migrations_total",
			Help:      metricsutil.HelpMsgWithStability("Total times a program was forced to migrate away from a pod due to budget exhaustion", compbasemetrics.ALPHA),
		},
		[]string{"program_id", "pod"},
	)
)

func registerScorerMetrics(registerer prometheus.Registerer) error {
	if registerer == nil {
		return errors.New("program-aware scorer: metrics registerer is required")
	}
	for _, collector := range []prometheus.Collector{
		routingDecisionsTotal,
		budgetAtScore,
		budgetReservationsTotal,
		budgetDeductionsTotal,
		budgetRefreshesTotal,
		budgetRefreshAmount,
		forcedMigrationsTotal,
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
