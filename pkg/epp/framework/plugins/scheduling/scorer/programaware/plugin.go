package programaware

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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
	// ProgramAwareScorerPluginType is the exported type name for registration
	ProgramAwareScorerPluginType = ProgramAwareType
	ProgramAwareType             = "program-aware-scorer"
)

// Config defines the configuration for the program-aware scorer plugin
type Config struct {
	// Old budget parameters retained for compatibility with old configs
	DefaultBudget        int64   `json:"defaultBudget,omitempty"`
	RefreshAmount        int64   `json:"refreshAmount,omitempty"`
	RefreshInterval      string  `json:"refreshInterval,omitempty"`
	LowUtilThreshold     float64 `json:"lowUtilThreshold,omitempty"`
	MediumUtilThreshold  float64 `json:"mediumUtilThreshold,omitempty"`
	LowUtilMultiplier    float64 `json:"lowUtilMultiplier,omitempty"`
	MediumUtilMultiplier float64 `json:"mediumUtilMultiplier,omitempty"`
	HighUtilMultiplier   float64 `json:"highUtilMultiplier,omitempty"`

	// PrefixMatchInfoProducerName is the name of the data producer that produces PrefixCacheMatchInfo
	PrefixMatchInfoProducerName string `json:"prefixMatchInfoProducerName,omitempty"`

	// Unified cost-based coefficients
	LoadCoefficient      float64 `json:"loadCoefficient,omitempty"`
	KvCoefficient        float64 `json:"kvCoefficient,omitempty"`
	RecomputeCoefficient float64 `json:"recomputeCoefficient,omitempty"`
	QueueThreshold       float64 `json:"queueThreshold,omitempty"`
}

// Plugin implements the program-aware scoring logic
type Plugin struct {
	typedName          plugin.TypedName
	prefixMatchDataKey plugin.DataKey

	programTokens map[string]int64
	tokensMu      sync.RWMutex

	loadCoefficient      float64
	kvCoefficient        float64
	recomputeCoefficient float64
	queueThreshold       float64
}

// compile-time type assertions
var (
	_ scheduling.Scorer                    = &Plugin{}
	_ requestcontrol.PreRequest            = &Plugin{}
	_ requestcontrol.ResponseBodyProcessor = &Plugin{}
)

// Factory defines the factory function for the ProgramAware scorer
func Factory(name string, rawParameters *json.Decoder, handle plugin.Handle) (plugin.Plugin, error) {
	cfg := Config{}

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
var ProgramAwareScorerPluginFactory = Factory

// New creates a new program-aware scorer
func New(ctx context.Context, name string, cfg Config) *Plugin {
	logger := log.FromContext(ctx)

	// Set defaults
	if cfg.LoadCoefficient <= 0 {
		cfg.LoadCoefficient = 0.1
	}
	if cfg.KvCoefficient <= 0 {
		cfg.KvCoefficient = 0.5
	}
	if cfg.RecomputeCoefficient <= 0 {
		cfg.RecomputeCoefficient = 0.0001
	}
	if cfg.QueueThreshold <= 0 {
		cfg.QueueThreshold = 100.0
	}

	logger.V(logutil.DEFAULT).Info("Program-aware scorer initialized",
		"loadCoefficient", cfg.LoadCoefficient,
		"kvCoefficient", cfg.KvCoefficient,
		"recomputeCoefficient", cfg.RecomputeCoefficient,
		"queueThreshold", cfg.QueueThreshold)

	return &Plugin{
		typedName: plugin.TypedName{
			Type: ProgramAwareType,
			Name: name,
		},
		prefixMatchDataKey:   attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(cfg.PrefixMatchInfoProducerName),
		programTokens:        make(map[string]int64),
		loadCoefficient:      cfg.LoadCoefficient,
		kvCoefficient:        cfg.KvCoefficient,
		recomputeCoefficient: cfg.RecomputeCoefficient,
		queueThreshold:       cfg.QueueThreshold,
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

// Score scores the given endpoints based on load, KV utilization, and recomputation cost
func (p *Plugin) Score(ctx context.Context, req *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) map[scheduling.Endpoint]float64 {
	logger := log.FromContext(ctx)
	scores := make(map[scheduling.Endpoint]float64, len(endpoints))

	// Extract program ID from request
	programID := req.FairnessID
	if programID == "" {
		programID = metadata.DefaultFairnessID
	}

	p.tokensMu.RLock()
	tokensSoFar := p.programTokens[programID]
	p.tokensMu.RUnlock()

	logger.V(logutil.VERBOSE).Info("Scoring endpoints",
		"programID", programID,
		"tokensSoFar", tokensSoFar,
		"numEndpoints", len(endpoints))

	for _, endpoint := range endpoints {
		podID := endpoint.GetMetadata().NamespacedName.String()

		metrics := endpoint.GetMetrics()
		load := 0.0
		kvUtil := 0.0
		if metrics != nil {
			load = float64(metrics.WaitingQueueSize)
			kvUtil = metrics.KVCacheUsagePercent
		}

		// Calculate load/util penalty
		loadTerm := 1.0 - (load / p.queueThreshold)
		if loadTerm < 0 {
			loadTerm = 0
		}
		penalty := p.loadCoefficient*load + p.kvCoefficient*kvUtil

		// Get cache score (hit ratio 0.0 to 1.0)
		cacheScore := p.getCacheScore(ctx, endpoint)

		// Calculate recompute cost for missing part of the program KV cache
		recomputeCost := (1.0 - cacheScore) * float64(tokensSoFar) * p.recomputeCoefficient

		// Final score
		score := 1.0 - penalty - recomputeCost
		scores[endpoint] = score

		// Record prometheus metric for visibility
		budgetAtScore.WithLabelValues(programID, podID).Set(score)

		decisionType := "cache_miss"
		if cacheScore > 0 {
			isSaturated := kvUtil >= 0.85 || load >= 3.0
			if isSaturated {
				if float64(tokensSoFar)*p.recomputeCoefficient >= penalty {
					decisionType = "cache_hit_saturated_wait"
				} else {
					decisionType = "cache_hit_saturated_migrate"
					forcedMigrationsTotal.WithLabelValues(programID, podID).Inc()
				}
			} else {
				decisionType = "cache_hit"
			}
		}
		routingDecisionsTotal.WithLabelValues(programID, podID, decisionType).Inc()

		logger.V(logutil.VERBOSE).Info("Scored endpoint",
			"endpoint", podID,
			"programID", programID,
			"load", load,
			"kvUtil", kvUtil,
			"cacheScore", cacheScore,
			"penalty", penalty,
			"recomputeCost", recomputeCost,
			"finalScore", score)
	}

	return scores
}

// getCacheScore retrieves the cache score from the endpoint data
func (p *Plugin) getCacheScore(ctx context.Context, endpoint scheduling.Endpoint) float64 {
	logger := log.FromContext(ctx)

	info, ok := endpoint.Get(p.prefixMatchDataKey.String())
	if !ok {
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

	return float64(prefixMatchInfo.MatchBlocks()) / float64(prefixMatchInfo.TotalBlocks())
}

// GetProgramTokens returns the accumulated tokens for a program ID (used for testing)
func (p *Plugin) GetProgramTokens(programID string) int64 {
	p.tokensMu.RLock()
	defer p.tokensMu.RUnlock()
	return p.programTokens[programID]
}

// SetProgramTokens sets the accumulated tokens for a program ID (used for testing)
func (p *Plugin) SetProgramTokens(programID string, tokens int64) {
	p.tokensMu.Lock()
	defer p.tokensMu.Unlock()
	p.programTokens[programID] = tokens
}

// PreRequest is a no-op since budget reservation is no longer needed
func (p *Plugin) PreRequest(ctx context.Context, req *scheduling.InferenceRequest, result *scheduling.SchedulingResult) {
}

// ResponseBody processes the response body after request completion.
// Accumulates the token usage processed by this program ID so far.
func (p *Plugin) ResponseBody(ctx context.Context, req *scheduling.InferenceRequest, resp *requestcontrol.Response, targetEndpoint *datalayer.EndpointMetadata) {
	if !resp.EndOfStream {
		return
	}

	programID := req.FairnessID
	if programID == "" {
		programID = metadata.DefaultFairnessID
	}

	promptTokens := int64(resp.Usage.PromptTokens)
	completionTokens := int64(resp.Usage.CompletionTokens)
	totalTokens := promptTokens + completionTokens

	var totalTokensSoFar int64
	p.tokensMu.Lock()
	if totalTokens > 0 {
		p.programTokens[programID] += totalTokens
	}
	totalTokensSoFar = p.programTokens[programID]
	p.tokensMu.Unlock()

	logger := log.FromContext(ctx)
	logger.V(logutil.VERBOSE).Info("ResponseBody: updated program tokens",
		"programID", programID,
		"promptTokens", promptTokens,
		"completionTokens", completionTokens,
		"totalTokensSoFar", totalTokensSoFar)
}

// Stop is a no-op
func (p *Plugin) Stop() {
}
