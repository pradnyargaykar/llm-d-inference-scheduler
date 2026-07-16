package programaware

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	requestcontrol "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
)

const (
	// ProgramAwareType is the type of the ProgramAware scorer
	
	// ProgramAwareScorerPluginType is the exported type name for registration
	ProgramAwareScorerPluginType = ProgramAwareType
	ProgramAwareType = "program-aware-scorer"

	// DefaultBudget is the default budget per program per pod
	DefaultBudget = 1000
)

// Config defines the configuration for the program-aware scorer plugin
type Config struct {
	// DefaultBudget is the initial budget allocated to each program per pod (also max budget)
	DefaultBudget int64 `json:"defaultBudget,omitempty"`

	// RefreshAmount is the base amount to add per refresh cycle (before multipliers)
	RefreshAmount int64 `json:"refreshAmount,omitempty"`

	// PrefixMatchInfoProducerName is the name of the data producer that produces PrefixCacheMatchInfo
	PrefixMatchInfoProducerName string `json:"prefixMatchInfoProducerName,omitempty"`

	// RefreshInterval is the interval for budget refresh (e.g., "30s", "1m")
	RefreshInterval string `json:"refreshInterval,omitempty"`

	// Budget refresh multipliers based on utilization
	LowUtilThreshold     float64 `json:"lowUtilThreshold,omitempty"`
	MediumUtilThreshold  float64 `json:"mediumUtilThreshold,omitempty"`
	LowUtilMultiplier    float64 `json:"lowUtilMultiplier,omitempty"`
	MediumUtilMultiplier float64 `json:"mediumUtilMultiplier,omitempty"`
	HighUtilMultiplier   float64 `json:"highUtilMultiplier,omitempty"`
}

// Plugin implements the program-aware scoring logic
type Plugin struct {
	typedName          plugin.TypedName
	budgetTracker      *BudgetTracker
	budgetRefresher    *BudgetRefresher
	prefixMatchDataKey plugin.DataKey
}

// compile-time type assertions
var (
	_ scheduling.Scorer                    = &Plugin{}
	_ requestcontrol.PreRequest            = &Plugin{}
	_ requestcontrol.ResponseBodyProcessor = &Plugin{}
)

// Factory defines the factory function for the ProgramAware scorer
func Factory(name string, rawParameters *json.Decoder, handle plugin.Handle) (plugin.Plugin, error) {
	cfg := Config{
		DefaultBudget: DefaultBudget,
	}

	if rawParameters != nil {
		if err := rawParameters.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' scorer - %w", ProgramAwareType, err)
		}
	}

	if err := registerScorerMetrics(handle.Metrics()); err != nil {
		return nil, fmt.Errorf("failed to register program-aware scorer metrics: %w", err)
	}

	return New(handle.Context(), name, cfg), nil
}

// ProgramAwareScorerPluginFactory is the exported factory function for registration
// It's an alias to Factory for consistency with other plugins
var ProgramAwareScorerPluginFactory = Factory

// New creates a new program-aware scorer
func New(ctx context.Context, name string, cfg Config) *Plugin {
	logger := log.FromContext(ctx)

	if cfg.DefaultBudget <= 0 {
		logger.V(logutil.DEFAULT).Info(fmt.Sprintf("defaultBudget %d should be positive, using default %d", cfg.DefaultBudget, DefaultBudget))
		cfg.DefaultBudget = DefaultBudget
	}

	// Set default refresh amount (10% of default budget if not specified)
	if cfg.RefreshAmount <= 0 {
		cfg.RefreshAmount = cfg.DefaultBudget / 10
		logger.V(logutil.DEFAULT).Info(fmt.Sprintf("refreshAmount not set, using default %d (10%% of defaultBudget)", cfg.RefreshAmount))
	}

	// Set default refresh configuration
	refreshInterval := 30 * time.Second
	if cfg.RefreshInterval != "" {
		if duration, err := time.ParseDuration(cfg.RefreshInterval); err == nil {
			refreshInterval = duration
		}
	}

	if cfg.LowUtilThreshold == 0 {
		cfg.LowUtilThreshold = 0.3
	}
	if cfg.MediumUtilThreshold == 0 {
		cfg.MediumUtilThreshold = 0.7
	}
	if cfg.LowUtilMultiplier == 0 {
		cfg.LowUtilMultiplier = 1.5
	}
	if cfg.MediumUtilMultiplier == 0 {
		cfg.MediumUtilMultiplier = 1.0
	}
	if cfg.HighUtilMultiplier == 0 {
		cfg.HighUtilMultiplier = 0.5
	}

	budgetTracker := NewBudgetTracker(cfg.DefaultBudget)

	// Create budget refresher
	refresher := NewBudgetRefresher(budgetTracker, RefresherConfig{
		RefreshInterval:      refreshInterval,
		RefreshAmount:        cfg.RefreshAmount,
		LowUtilThreshold:     cfg.LowUtilThreshold,
		MediumUtilThreshold:  cfg.MediumUtilThreshold,
		LowUtilMultiplier:    cfg.LowUtilMultiplier,
		MediumUtilMultiplier: cfg.MediumUtilMultiplier,
		HighUtilMultiplier:   cfg.HighUtilMultiplier,
	})

	// Start the refresher
	if err := refresher.Start(ctx); err != nil {
		logger.Error(err, "Failed to start budget refresher")
	}

	return &Plugin{
		typedName: plugin.TypedName{
			Type: ProgramAwareType,
			Name: name,
		},
		budgetTracker:      budgetTracker,
		budgetRefresher:    refresher,
		prefixMatchDataKey: attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(cfg.PrefixMatchInfoProducerName),
	}
}

// TypedName returns the typed name of the plugin
func (p *Plugin) TypedName() plugin.TypedName {
	return p.typedName
}

// Category returns the preference the scorer applies when scoring candidate endpoints
func (p *Plugin) Category() scheduling.ScorerCategory {
	return scheduling.Affinity
}

// Produces returns the data produced by the plugin
func (p *Plugin) Produces() map[plugin.DataKey]any {
	return map[plugin.DataKey]any{}
}

// Consumes returns the data consumed by the plugin
func (p *Plugin) Consumes() plugin.DataDependencies {
	return plugin.DataDependencies{
		Optional: map[plugin.DataKey]any{p.prefixMatchDataKey: attrprefix.PrefixCacheMatchInfo{}},
	}
}

// Score scores the given endpoints based on budget availability and cache affinity
// Implements the program-aware routing logic from the design document
func (p *Plugin) Score(ctx context.Context, req *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) map[scheduling.Endpoint]float64 {
	logger := log.FromContext(ctx)
	scores := make(map[scheduling.Endpoint]float64, len(endpoints))

	// Extract program ID from request
	programID := req.FairnessID
	if programID == "" {
		programID = metadata.DefaultFairnessID
	}

	// Check if this is a first-time request (optimization: skip cache queries)
	isFirstTime := p.budgetTracker.IsFirstTimeProgram(programID)

	logger.V(logutil.VERBOSE).Info("Scoring endpoints",
		"programID", programID,
		"isFirstTime", isFirstTime,
		"numEndpoints", len(endpoints))

	if isFirstTime {
		// First-time request: Budget-first routing
		p.scoreFirstTime(ctx, programID, endpoints, scores)
	} else {
		// Subsequent request: Cache-aware routing
		p.scoreSubsequent(ctx, programID, endpoints, scores)
	}

	return scores
}

// scoreFirstTime scores endpoints for first-time requests
// Logic: Highest budget → Lowest KV utilization (tie-breaker)
func (p *Plugin) scoreFirstTime(ctx context.Context, programID string, endpoints []scheduling.Endpoint, scores map[scheduling.Endpoint]float64) {
	logger := log.FromContext(ctx)

	for _, endpoint := range endpoints {
		podID := endpoint.GetMetadata().NamespacedName.String()

		// Get available budget
		availableBudget := p.budgetTracker.GetAvailable(programID, podID)

		// Always emit budget gauge so it's visible in Prometheus
		budgetAtScore.WithLabelValues(programID, podID).Set(float64(availableBudget))

		if availableBudget <= 0 {
			scores[endpoint] = 0.0
			routingDecisionsTotal.WithLabelValues(programID, podID, "budget_exhausted").Inc()
			logger.V(logutil.VERBOSE).Info("First-time: No budget available",
				"endpoint", podID,
				"programID", programID,
				"score", 0.0)
			continue
		}

		// Calculate budget score (0-0.9 range)
		budgetScore := (float64(availableBudget) / float64(p.budgetTracker.defaultBudget)) * 0.9

		// Get KV cache utilization for tie-breaking
		kvUtilization := p.getKVUtilization(endpoint)

		// Update utilization cache for budget refresher
		p.budgetRefresher.UpdateUtilization(podID, kvUtilization)

		// Lower utilization = higher score (tie-breaker, 0-0.1 range)
		utilizationScore := (1.0 - kvUtilization) * 0.1

		// Final score: budget (0-0.9) + utilization tie-breaker (0-0.1)
		scores[endpoint] = budgetScore + utilizationScore

		routingDecisionsTotal.WithLabelValues(programID, podID, "first_time").Inc()

		logger.V(logutil.VERBOSE).Info("First-time: Scored endpoint",
			"endpoint", podID,
			"programID", programID,
			"availableBudget", availableBudget,
			"kvUtilization", kvUtilization,
			"budgetScore", budgetScore,
			"utilizationScore", utilizationScore,
			"finalScore", scores[endpoint])
	}
}

// scoreSubsequent scores endpoints for subsequent requests
// Logic: Cache match + budget → Force migration if budget exhausted
func (p *Plugin) scoreSubsequent(ctx context.Context, programID string, endpoints []scheduling.Endpoint, scores map[scheduling.Endpoint]float64) {
	logger := log.FromContext(ctx)

	for _, endpoint := range endpoints {
		podID := endpoint.GetMetadata().NamespacedName.String()

		// Get available budget
		availableBudget := p.budgetTracker.GetAvailable(programID, podID)

		// Always emit budget gauge so it's visible in Prometheus
		budgetAtScore.WithLabelValues(programID, podID).Set(float64(availableBudget))

		// Get KV cache utilization and update cache for refresher
		kvUtilization := p.getKVUtilization(endpoint)
		p.budgetRefresher.UpdateUtilization(podID, kvUtilization)

		// Get cache score
		cacheScore := p.getCacheScore(ctx, endpoint)

		if availableBudget > 0 {
			// Budget available: Prioritize cache match
			// Cache score (0-1) * 0.9 + budget bonus (0-0.1)
			budgetBonus := (float64(availableBudget) / float64(p.budgetTracker.defaultBudget)) * 0.1
			scores[endpoint] = cacheScore*0.9 + budgetBonus

			decisionType := "cache_hit"
			if cacheScore == 0 {
				decisionType = "no_cache"
			}
			routingDecisionsTotal.WithLabelValues(programID, podID, decisionType).Inc()

			logger.V(logutil.VERBOSE).Info("Subsequent: Budget available",
				"endpoint", podID,
				"programID", programID,
				"availableBudget", availableBudget,
				"cacheScore", cacheScore,
				"budgetBonus", budgetBonus,
				"finalScore", scores[endpoint])
		} else {
			// Budget exhausted: Force migration to pod with budget
			scores[endpoint] = 0.0
			routingDecisionsTotal.WithLabelValues(programID, podID, "budget_exhausted").Inc()
			forcedMigrationsTotal.WithLabelValues(programID, podID).Inc()

			logger.V(logutil.VERBOSE).Info("Subsequent: Budget exhausted, forcing migration",
				"endpoint", podID,
				"programID", programID,
				"cacheScore", cacheScore,
				"finalScore", 0.0)
		}
	}
}

// getCacheScore retrieves the cache score from the endpoint data
// Reuses the same logic as the prefix scorer plugin
func (p *Plugin) getCacheScore(ctx context.Context, endpoint scheduling.Endpoint) float64 {
	logger := log.FromContext(ctx)

	info, ok := endpoint.Get(p.prefixMatchDataKey.String())
	if !ok {
		// No cache info available, return 0
		return 0.0
	}

	prefixMatchInfo, ok := info.(*attrprefix.PrefixCacheMatchInfo)
	if !ok {
		logger.V(logutil.DEFAULT).Error(nil, "PrefixCacheMatchInfo has unexpected type",
			"endpoint", endpoint.GetMetadata().NamespacedName.String())
		return 0.0
	}

	if prefixMatchInfo.TotalBlocks() == 0 {
		return 0.0
	}

	// Return cache hit ratio (0-1)
	// Same calculation as prefix scorer: MatchBlocks / TotalBlocks
	return float64(prefixMatchInfo.MatchBlocks()) / float64(prefixMatchInfo.TotalBlocks())
}

// getKVUtilization retrieves the KV cache utilization from endpoint metrics
func (p *Plugin) getKVUtilization(endpoint scheduling.Endpoint) float64 {
	metrics := endpoint.GetMetrics()
	
	// Use the pre-calculated KV cache utilization percentage (0.0 to 1.0)
	return metrics.KVCacheUsagePercent
}

// GetBudgetTracker returns the budget tracker (for testing and monitoring)
func (p *Plugin) GetBudgetTracker() *BudgetTracker {
	return p.budgetTracker
}

// Stop stops the plugin and cleans up resources
func (p *Plugin) Stop() {
	if p.budgetTracker != nil {
		p.budgetTracker.Stop()
	}
}

// Made with Bob


// PreRequest reserves budget after scheduling decision is made
func (p *Plugin) PreRequest(ctx context.Context, req *scheduling.InferenceRequest, result *scheduling.SchedulingResult) {
	logger := log.FromContext(ctx)

	// Validate scheduling result
	if result == nil || len(result.ProfileResults) == 0 {
		logger.V(logutil.VERBOSE).Info("PreRequest: No scheduling result, skipping budget reservation",
			"requestID", req.RequestID)
		return
	}

	// Extract program ID
	programID := req.FairnessID
	if programID == "" {
		programID = metadata.DefaultFairnessID
	}

	// Get the primary profile result
	primaryResult := result.ProfileResults[result.PrimaryProfileName]
	if primaryResult == nil || len(primaryResult.TargetEndpoints) == 0 {
		logger.V(logutil.VERBOSE).Info("PreRequest: No target endpoint selected, skipping budget reservation",
			"requestID", req.RequestID,
			"programID", programID)
		return
	}

	// Get the selected endpoint (first one in case of multiple)
	selectedEndpoint := primaryResult.TargetEndpoints[0]
	podID := selectedEndpoint.GetMetadata().NamespacedName.String()

	// Reserve budget for this request
	err := p.budgetTracker.Reserve(req.RequestID, programID, podID)
	if err != nil {
		logger.Error(err, "PreRequest: Failed to reserve budget",
			"requestID", req.RequestID,
			"programID", programID,
			"podID", podID)
		return
	}

	budgetReservationsTotal.WithLabelValues(programID, podID).Inc()

	logger.V(logutil.VERBOSE).Info("PreRequest: Budget reserved",
		"requestID", req.RequestID,
		"programID", programID,
		"podID", podID)
}

// ResponseBody processes the response body after request completion.
// Deducts budget when the request is complete (end of stream).
func (p *Plugin) ResponseBody(ctx context.Context, req *scheduling.InferenceRequest, resp *requestcontrol.Response, targetEndpoint *datalayer.EndpointMetadata) {
	logger := log.FromContext(ctx)
	
	// Only process on the final chunk of the response
	if !resp.EndOfStream {
		return
	}

	// Extract program ID
	programID := req.FairnessID
	if programID == "" {
		programID = metadata.DefaultFairnessID
	}

	// Check if request was successful
	success := true
	if resp.ReqMetadata != nil {
		if val, ok := resp.ReqMetadata["success"]; ok {
			if b, ok := val.(bool); ok {
				success = b
			}
		}
	}

	podID := ""
	if targetEndpoint != nil {
		podID = targetEndpoint.NamespacedName.String()
	}

	err := p.budgetTracker.ReleaseAndDeduct(req.RequestID, success)
	if err != nil {
		logger.Error(err, "ResponseBody: Failed to release and deduct budget",
			"requestID", req.RequestID,
			"programID", programID)
		return
	}

	budgetDeductionsTotal.WithLabelValues(programID, podID).Inc()

	logger.V(logutil.VERBOSE).Info("ResponseBody: Budget deducted",
		"requestID", req.RequestID,
		"programID", programID,
		"podID", podID)
}
