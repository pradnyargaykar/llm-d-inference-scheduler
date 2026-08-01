package programaware

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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

	defaultMissThreshold = 3
	defaultMaxContext    = 32768
)

// Config defines the configuration for the program-aware scorer plugin
type Config struct {
	// PrefixMatchInfoProducerName is the name of the data producer that produces PrefixCacheMatchInfo
	PrefixMatchInfoProducerName string `json:"prefixMatchInfoProducerName,omitempty"`

	// MissThreshold for pin migration
	MissThreshold int `json:"missThreshold,omitempty"`

	// Retained for backward compatibility with existing YAML configs
	DefaultBudget        int64   `json:"defaultBudget,omitempty"`
	RefreshAmount        int64   `json:"refreshAmount,omitempty"`
	RefreshInterval      string  `json:"refreshInterval,omitempty"`
	LowUtilThreshold     float64 `json:"lowUtilThreshold,omitempty"`
	MediumUtilThreshold  float64 `json:"mediumUtilThreshold,omitempty"`
	LowUtilMultiplier    float64 `json:"lowUtilMultiplier,omitempty"`
	MediumUtilMultiplier float64 `json:"mediumUtilMultiplier,omitempty"`
	HighUtilMultiplier   float64 `json:"highUtilMultiplier,omitempty"`
	LoadCoefficient      float64 `json:"loadCoefficient,omitempty"`
	KvCoefficient        float64 `json:"kvCoefficient,omitempty"`
	RecomputeCoefficient float64 `json:"recomputeCoefficient,omitempty"`
	QueueThreshold       float64 `json:"queueThreshold,omitempty"`
}

// Plugin implements the program-aware scoring logic
type Plugin struct {
	typedName          plugin.TypedName
	prefixMatchDataKey plugin.DataKey

	programTokens   map[string]int64
	maxActiveTokens int64
	tokensMu        sync.RWMutex

	mu            sync.RWMutex
	pins          map[string]string
	podCount      map[string]int
	misses        map[string]int
	rrCursor      int
	missThreshold int
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
	missThreshold := defaultMissThreshold
	if cfg.MissThreshold > 0 {
		missThreshold = cfg.MissThreshold
	}

	return &Plugin{
		typedName:          plugin.TypedName{Type: ProgramAwareType, Name: name},
		prefixMatchDataKey: attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(cfg.PrefixMatchInfoProducerName),
		programTokens:      make(map[string]int64),
		maxActiveTokens:    1000,
		pins:               make(map[string]string),
		podCount:           make(map[string]int),
		misses:             make(map[string]int),
		missThreshold:      missThreshold,
	}
}

// TypedName returns the type and name tuple of this plugin instance
func (p *Plugin) TypedName() plugin.TypedName {
	return p.typedName
}

// Category returns the preference the scorer applies
func (p *Plugin) Category() scheduling.ScorerCategory {
	return scheduling.Affinity
}

// Score computes a clean model-agnostic, zero-tuning 2-penalty score for candidate endpoints.
// Formula: Score_i = CacheScore + PinBoost - RelLoad - RecomputePenalty
func (p *Plugin) Score(ctx context.Context, req *scheduling.InferenceRequest, endpoints []scheduling.Endpoint) map[scheduling.Endpoint]float64 {
	logger := log.FromContext(ctx)
	scores := make(map[scheduling.Endpoint]float64, len(endpoints))
	if len(endpoints) == 0 {
		return scores
	}

	programID := req.FairnessID
	if programID == "" {
		programID = metadata.DefaultFairnessID
	}

	p.tokensMu.RLock()
	tokensSoFar := p.programTokens[programID]
	maxActive := p.maxActiveTokens
	p.tokensMu.RUnlock()

	// 1. Relative Queue Load Normalization across candidate endpoints
	minQueue := -1.0
	maxQueue := 0.0
	for _, ep := range endpoints {
		m := ep.GetMetrics()
		q := 0.0
		if m != nil {
			q = float64(m.WaitingQueueSize)
		}
		if minQueue < 0 || q < minQueue {
			minQueue = q
		}
		if q > maxQueue {
			maxQueue = q
		}
	}
	queueDiff := maxQueue - minQueue

	// 2. Relative KV Context Ratio (0.0 to 1.0) dynamically scaled by Max Workload KV
	if maxActive < 1000 {
		maxActive = 1000
	}
	contextRatio := float64(tokensSoFar) / float64(maxActive)
	if contextRatio > 1.0 {
		contextRatio = 1.0
	}

	for _, endpoint := range endpoints {
		podID := endpoint.GetMetadata().NamespacedName.String()

		// 3. Prefix Cache Match Ratio (0.0 to 1.0)
		cacheScore := p.getCacheScore(ctx, endpoint)

		// 4. Endpoint Metrics
		metrics := endpoint.GetMetrics()
		queueSize := 0.0
		kvUtil := 0.0
		if metrics != nil {
			queueSize = float64(metrics.WaitingQueueSize)
			kvUtil = metrics.KVCacheUsagePercent
		}

		// 5. Absolute Queue Saturation (clamped to 1.0 at queue >= 20)
		absQueueLoad := queueSize / 20.0
		if absQueueLoad > 1.0 {
			absQueueLoad = 1.0
		}

		// 6. Relative Queue Load
		relQueueLoad := 0.0
		if queueDiff >= 1.0 {
			relQueueLoad = (queueSize - minQueue) / queueDiff
		}

		// 7. Triple-Guard Unified Load Penalty (0.0 to 1.0)
		pLoad := absQueueLoad
		if kvUtil > pLoad {
			pLoad = kvUtil
		}
		if relQueueLoad > pLoad {
			pLoad = relQueueLoad
		}

		// 8. Physical KV Cache Recompute Penalty (0.0 to 1.0)
		recomputePenalty := (1.0 - cacheScore) * contextRatio

		// Audited Universal Scorer Formula:
		// Score = CacheScore - pLoad - RecomputePenalty
		score := cacheScore - pLoad - recomputePenalty
		scores[endpoint] = score

		// Record metrics for observability
		budgetAtScore.WithLabelValues(programID, podID).Set(score)

		decisionType := "cache_miss"
		if cacheScore > 0 {
			if pLoad >= 0.75 {
				decisionType = "cache_hit_saturated_migrate"
				forcedMigrationsTotal.WithLabelValues(programID, podID).Inc()
			} else {
				decisionType = "cache_hit"
			}
		}
		routingDecisionsTotal.WithLabelValues(programID, podID, decisionType).Inc()

		logger.V(logutil.VERBOSE).Info("Scored endpoint universal 2-penalty",
			"endpoint", podID,
			"programID", programID,
			"cacheScore", cacheScore,
			"pLoad", pLoad,
			"recomputePenalty", recomputePenalty,
			"contextRatio", contextRatio,
			"finalScore", score)
	}

	return scores
}

// leastLoadedPod returns the candidate pod pinned by the fewest programs.
func (p *Plugin) leastLoadedPod(endpoints []scheduling.Endpoint) scheduling.Endpoint {
	keys := make([]string, len(endpoints))
	byKey := make(map[string]scheduling.Endpoint, len(endpoints))
	for i, ep := range endpoints {
		k := ep.GetMetadata().NamespacedName.String()
		keys[i] = k
		byKey[k] = ep
	}
	sort.Strings(keys)

	p.mu.RLock()
	defer p.mu.RUnlock()

	minCount := -1
	candidates := make([]string, 0, len(keys))
	for _, k := range keys {
		c := p.podCount[k]
		switch {
		case minCount == -1 || c < minCount:
			minCount = c
			candidates = append(candidates[:0], k)
		case c == minCount:
			candidates = append(candidates, k)
		}
	}
	return byKey[candidates[p.rrCursor%len(candidates)]]
}

// PreRequest tracks pin commitments and token usage.
func (p *Plugin) PreRequest(ctx context.Context, req *scheduling.InferenceRequest, result *scheduling.SchedulingResult) {
	if result == nil {
		return
	}

	chosen, ok := chosenPod(result)
	if !ok {
		return
	}

	programID := req.FairnessID
	if programID == "" {
		programID = metadata.DefaultFairnessID
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.rrCursor++

	existing, pinned := p.pins[programID]
	switch {
	case !pinned:
		p.pins[programID] = chosen
		p.podCount[chosen]++
		delete(p.misses, programID)
	case existing == chosen:
		delete(p.misses, programID)
	default:
		p.misses[programID]++
		if p.misses[programID] >= p.missThreshold {
			p.podCount[existing]--
			if p.podCount[existing] <= 0 {
				delete(p.podCount, existing)
			}
			p.pins[programID] = chosen
			p.podCount[chosen]++
			delete(p.misses, programID)
		}
	}
}

// ResponseBody processes token counts from response stream.
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

	p.tokensMu.Lock()
	if totalTokens > 0 {
		p.programTokens[programID] += totalTokens
	}
	totalTokensSoFar := p.programTokens[programID]
	if totalTokensSoFar > p.maxActiveTokens {
		p.maxActiveTokens = totalTokensSoFar
	}
	p.tokensMu.Unlock()

	logger := log.FromContext(ctx)
	logger.V(logutil.VERBOSE).Info("ResponseBody: updated program tokens",
		"programID", programID,
		"promptTokens", promptTokens,
		"completionTokens", completionTokens,
		"totalTokensSoFar", totalTokensSoFar,
		"maxActiveTokens", p.maxActiveTokens)
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
	if tokens > p.maxActiveTokens {
		p.maxActiveTokens = tokens
	}
}

// getCacheScore retrieves the cache hit score (0.0 to 1.0)
func (p *Plugin) getCacheScore(ctx context.Context, endpoint scheduling.Endpoint) float64 {
	info, ok := endpoint.Get(p.prefixMatchDataKey.String())
	if !ok {
		return 0.0
	}

	prefixMatchInfo, ok := info.(*attrprefix.PrefixCacheMatchInfo)
	if !ok || prefixMatchInfo.TotalBlocks() == 0 {
		return 0.0
	}

	return float64(prefixMatchInfo.MatchBlocks()) / float64(prefixMatchInfo.TotalBlocks())
}

// chosenPod extracts the pod selected by the picker
func chosenPod(result *scheduling.SchedulingResult) (string, bool) {
	if result == nil {
		return "", false
	}
	profile, ok := result.ProfileResults[result.PrimaryProfileName]
	if !ok || profile == nil || len(profile.TargetEndpoints) == 0 {
		return "", false
	}
	return profile.TargetEndpoints[0].GetMetadata().NamespacedName.String(), true
}
