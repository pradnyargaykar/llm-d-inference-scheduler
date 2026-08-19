package programaware

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	// ProgramAwareScorerPluginType is the exported type name for registration in the EPP plugin registry.
	ProgramAwareScorerPluginType = ProgramAwareType
	ProgramAwareType             = "program-aware-scorer"

	// defaultMissThreshold is the consecutive off-pin route count before migrating a program's home pin.
	defaultMissThreshold = 3
)

// Config defines the configuration parameters for the program-aware scorer plugin.
type Config struct {
	// PrefixMatchInfoProducerName specifies the data producer that computes PrefixCacheMatchInfo (e.g. approx-prefix-cache-producer).
	PrefixMatchInfoProducerName string `json:"prefixMatchInfoProducerName,omitempty"`

	// MissThreshold is the number of consecutive off-home-pod routes before transferring a program's pin commitment.
	MissThreshold int `json:"missThreshold,omitempty"`
}

// Plugin implements the program-aware scoring algorithm for agentic multi-turn workloads.
type Plugin struct {
	typedName          plugin.TypedName
	prefixMatchDataKey plugin.DataKey

	// Token tracking: monitors session context growth across turns to dynamically scale recompute penalties.
	programTokens   map[string]int64
	maxActiveTokens int64
	tokensMu        sync.RWMutex

	// Home-pod pin management & migration state.
	mu            sync.RWMutex
	pins          map[string]string // programID -> pinned pod ID
	misses        map[string]int    // programID -> consecutive off-pin route count
	missThreshold int
}

// Compile-time interface compliance assertions.
var (
	_ scheduling.Scorer                    = &Plugin{}
	_ plugin.ConsumerPlugin                = &Plugin{}
	_ requestcontrol.PreRequest            = &Plugin{}
	_ requestcontrol.ResponseBodyProcessor = &Plugin{}
)

// Factory instantiates the program-aware scorer plugin from raw JSON parameters.
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

// ProgramAwareScorerPluginFactory is the exported factory variable for plugin registration.
var ProgramAwareScorerPluginFactory = Factory

// New creates and initializes a new Program-Aware scorer instance.
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
		misses:             make(map[string]int),
		missThreshold:      missThreshold,
	}
}

// TypedName returns the plugin's type and instance name.
func (p *Plugin) TypedName() plugin.TypedName {
	return p.typedName
}

// Category indicates the scheduling category (Affinity).
func (p *Plugin) Category() scheduling.ScorerCategory {
	return scheduling.Affinity
}

// Produces declares any data produced by this plugin (none).
func (p *Plugin) Produces() map[plugin.DataKey]any {
	return map[plugin.DataKey]any{}
}

// Consumes declares that this plugin requires PrefixCacheMatchInfo from the data producer.
func (p *Plugin) Consumes() plugin.DataDependencies {
	return plugin.DataDependencies{
		Required: map[plugin.DataKey]any{p.prefixMatchDataKey: attrprefix.PrefixCacheMatchInfo{}},
	}
}

// Score evaluates all candidate endpoints and assigns each a score based on:
//
//	Score = CacheScore + PinBoost - RecomputePenalty - RelLoad
//
// All 4 components are naturally bounded to [0.0, 1.0], providing parameter-free balancing
// between KV cache reuse, session stickiness, and load-aware spillover.
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

	p.mu.RLock()
	pinnedKey := p.pins[programID]
	p.mu.RUnlock()

	// 1. Relative Context Growth Ratio: estimates the program's accumulated context depth
	// relative to the longest active session in the workload (0.0 to 1.0).
	if maxActive < 1000 {
		maxActive = 1000
	}
	contextRatio := float64(tokensSoFar) / float64(maxActive)
	if contextRatio > 1.0 {
		contextRatio = 1.0
	}

	// 2. Discover Min/Max Load across candidate endpoints (Load = Running + Waiting).
	minLoad := math.MaxInt32
	maxLoad := math.MinInt32
	hasWaiting := false

	for _, ep := range endpoints {
		running := 0
		waiting := 0
		if m := ep.GetMetrics(); m != nil {
			running = m.RunningRequestsSize
			waiting = m.WaitingQueueSize
		}
		if waiting > 0 {
			hasWaiting = true
		}
		load := running + waiting
		if load < minLoad {
			minLoad = load
		}
		if load > maxLoad {
			maxLoad = load
		}
	}

	// 3. Compute score for each candidate endpoint.
	for _, endpoint := range endpoints {
		podID := endpoint.GetMetadata().GetNamespacedName().String()
		m := endpoint.GetMetrics()
		running := 0
		waiting := 0
		if m != nil {
			running = m.RunningRequestsSize
			waiting = m.WaitingQueueSize
		}
		load := running + waiting

		// Component A: Prefix Cache Match Ratio (0.0 to 1.0)
		cacheScore := p.getCacheScore(ctx, endpoint)

		// Component B: Relative Load Congestion Penalty (0.0 to 1.0)
		// Active only when there is a significant load spread (delta >= 3) or queue buildup.
		relLoad := 0.0
		if maxLoad > minLoad && (maxLoad-minLoad >= 3 || hasWaiting) {
			relLoad = float64(load-minLoad) / float64(maxLoad-minLoad)
		}

		// Component C: Sticky Home-Pod Pin Boost (+1.0 if home pod, 0.0 otherwise)
		pinBoost := 0.0
		if pinnedKey != "" && podID == pinnedKey {
			pinBoost = 1.0
		}

		// Component D: Physical KV Cache Recompute Penalty (0.0 to 1.0)
		// Penalizes migrating deep sessions to pods without cached prefix blocks.
		recomputePenalty := (1.0 - cacheScore) * contextRatio

		// Final Scoring Formulation:
		score := cacheScore + pinBoost - recomputePenalty - relLoad
		scores[endpoint] = score

		// Record telemetry metrics
		endpointScore.WithLabelValues(programID, podID).Set(score)

		decisionType := "cache_miss"
		if cacheScore > 0 {
			decisionType = "cache_hit"
		}
		routingDecisionsTotal.WithLabelValues(programID, podID, decisionType).Inc()

		logger.V(logutil.VERBOSE).Info("Scored endpoint program-aware",
			"endpoint", podID,
			"programID", programID,
			"cacheScore", cacheScore,
			"pinBoost", pinBoost,
			"relLoad", relLoad,
			"load", load,
			"running", running,
			"waiting", waiting,
			"recomputePenalty", recomputePenalty,
			"contextRatio", contextRatio,
			"finalScore", score)
	}

	return scores
}

// PreRequest commits pin assignments and manages dynamic pin migration when requests spill over.
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

	existing, pinned := p.pins[programID]
	switch {
	case !pinned:
		// First request: pin program to the chosen pod.
		p.pins[programID] = chosen
		delete(p.misses, programID)
	case existing == chosen:
		// Routed to existing home pod: clear any previous off-pin miss counter.
		delete(p.misses, programID)
	default:
		// Spilled over to a non-home pod: increment miss counter.
		p.misses[programID]++
		if p.misses[programID] >= p.missThreshold {
			// Threshold reached: migrate home pin commitment to the new pod.
			p.pins[programID] = chosen
			delete(p.misses, programID)
		}
	}
}

// ResponseBody captures token usage upon stream completion to update session context depth.
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

// SetPin manually sets the pinned pod ID for a program (used in unit tests).
func (p *Plugin) SetPin(programID string, podID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pins[programID] = podID
}

// GetProgramTokens returns the accumulated tokens for a program ID (used in unit tests).
func (p *Plugin) GetProgramTokens(programID string) int64 {
	p.tokensMu.RLock()
	defer p.tokensMu.RUnlock()
	return p.programTokens[programID]
}

// SetProgramTokens manually sets the accumulated tokens for a program ID (used in unit tests).
func (p *Plugin) SetProgramTokens(programID string, tokens int64) {
	p.tokensMu.Lock()
	defer p.tokensMu.Unlock()
	p.programTokens[programID] = tokens
	if tokens > p.maxActiveTokens {
		p.maxActiveTokens = tokens
	}
}

// getCacheScore retrieves the normalized prefix cache match ratio [0.0, 1.0] for an endpoint.
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

// chosenPod extracts the selected endpoint's namespaced name from the scheduling result.
func chosenPod(result *scheduling.SchedulingResult) (string, bool) {
	if result == nil {
		return "", false
	}
	profile, ok := result.ProfileResults[result.PrimaryProfileName]
	if !ok || profile == nil || len(profile.TargetEndpoints) == 0 {
		return "", false
	}
	return profile.TargetEndpoints[0].GetMetadata().GetNamespacedName().String(), true
}
