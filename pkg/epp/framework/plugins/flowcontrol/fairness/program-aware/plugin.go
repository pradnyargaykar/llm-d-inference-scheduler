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

// Package programaware implements a flow-control fairness policy that schedules
// programs using their accumulated metrics using scoring strategies (LAS, DRR, or RR).
package programaware

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/flowcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
)

// ProgramAwarePluginType is the registered type name for this plugin.
const ProgramAwarePluginType = "program-aware-fairness"

// enqueueTimeAttributeKey is the per-request attribute key under which Pick
// stashes the flow-control enqueue timestamp for PreRequest to read back.
const enqueueTimeAttributeKey = "program-aware/enqueue-time"

// Config holds the JSON-decoded configuration for the plugin. JSON parameters
// are merged onto a copy of DefaultConfig, so any field omitted from the
// user's JSON keeps its default value.
type Config struct {
	// Strategy selects the fairness scoring algorithm used by Pick().
	// Valid values: "las" (default), "drr", "rr".
	Strategy string `json:"strategy,omitempty"`

	// --- DRR weights (only used when strategy == "drr") ---
	WeightDeficit          float64 `json:"weightDeficit,omitempty"`
	WeightDRRHeadWait      float64 `json:"weightDrrHeadWait,omitempty"`
	QuantumTokens          int64   `json:"quantumTokens,omitempty"`
	DeficitHalfLifeSeconds float64 `json:"deficitHalfLifeSeconds,omitempty"`
	DeficitDecayFactor     float64 `json:"deficitDecayFactor,omitempty"`

	// --- Service weights (only used when strategy == "las") ---
	WeightService          float64 `json:"weightService,omitempty"`
	WeightServiceHeadWait  float64 `json:"weightServiceHeadWait,omitempty"`
	ServiceDecayFactor     float64 `json:"serviceDecayFactor,omitempty"`
	ServiceHalfLifeSeconds float64 `json:"serviceHalfLifeSeconds,omitempty"`

	// Compatibility aliases for LAS
	LASWeightService   float64 `json:"lasWeightService,omitempty"`
	LASWeightHeadWait  float64 `json:"lasWeightHeadWait,omitempty"`
	LASDecayFactor     float64 `json:"lasDecayFactor,omitempty"`
	LASHalfLifeSeconds float64 `json:"lasHalfLifeSeconds,omitempty"`

	// --- Eviction (applies to all strategies) ---
	EvictionTTLSeconds   float64 `json:"evictionTtlSeconds,omitempty"`
	EvictionSweepSeconds float64 `json:"evictionSweepSeconds,omitempty"`
}

// DefaultConfig returns the canonical Config used when JSON parameters are absent or partial.
func DefaultConfig() Config {
	return Config{
		Strategy:               "las",
		WeightDeficit:          0.8,
		WeightDRRHeadWait:      0.2,
		QuantumTokens:          1000,
		DeficitHalfLifeSeconds: 0,
		DeficitDecayFactor:     0.99997,
		WeightService:          0.8,
		WeightServiceHeadWait:  0.2,
		ServiceDecayFactor:     0.99997,
		ServiceHalfLifeSeconds: 0,
		EvictionTTLSeconds:     3600,
		EvictionSweepSeconds:   300,
	}
}

// validate checks that numeric fields fall in safe ranges.
func (c Config) validate() error {
	if c.WeightDeficit < 0 {
		return fmt.Errorf("weightDeficit must be >= 0, got %v", c.WeightDeficit)
	}
	if c.WeightDRRHeadWait < 0 {
		return fmt.Errorf("weightDrrHeadWait must be >= 0, got %v", c.WeightDRRHeadWait)
	}
	if c.WeightService < 0 {
		return fmt.Errorf("weightService must be >= 0, got %v", c.WeightService)
	}
	if c.WeightServiceHeadWait < 0 {
		return fmt.Errorf("weightServiceHeadWait must be >= 0, got %v", c.WeightServiceHeadWait)
	}
	if c.QuantumTokens <= 0 {
		return fmt.Errorf("quantumTokens must be > 0, got %d", c.QuantumTokens)
	}
	if c.DeficitHalfLifeSeconds < 0 {
		return fmt.Errorf("deficitHalfLifeSeconds must be >= 0, got %v", c.DeficitHalfLifeSeconds)
	}
	if c.ServiceHalfLifeSeconds < 0 {
		return fmt.Errorf("serviceHalfLifeSeconds must be >= 0, got %v", c.ServiceHalfLifeSeconds)
	}
	if c.DeficitDecayFactor < 0 || c.DeficitDecayFactor >= 1 {
		return fmt.Errorf("deficitDecayFactor must be in [0, 1), got %v", c.DeficitDecayFactor)
	}
	if c.ServiceDecayFactor <= 0 || c.ServiceDecayFactor > 1 {
		return fmt.Errorf("serviceDecayFactor must be in (0, 1], got %v", c.ServiceDecayFactor)
	}
	if c.EvictionTTLSeconds < 0 {
		return fmt.Errorf("evictionTtlSeconds must be >= 0, got %v", c.EvictionTTLSeconds)
	}
	if c.EvictionSweepSeconds <= 0 {
		return fmt.Errorf("evictionSweepSeconds must be > 0, got %v", c.EvictionSweepSeconds)
	}
	return nil
}

// Compile-time interface assertions.
var (
	_ flowcontrol.FairnessPolicy  = &ProgramAwarePlugin{}
	_ fwkrc.DataProducer          = &ProgramAwarePlugin{}
	_ fwkrc.PreRequest            = &ProgramAwarePlugin{}
	_ fwkrc.ResponseBodyProcessor = &ProgramAwarePlugin{}
	_ plugin.StateDumper          = &ProgramAwarePlugin{}
)

// ProgramAwarePluginFactory creates a new ProgramAwarePlugin from JSON config.
//nolint:revive
func ProgramAwarePluginFactory(name string, parameters *json.Decoder, handle plugin.Handle) (plugin.Plugin, error) {
	cfg := DefaultConfig()
	if parameters != nil {
		if err := parameters.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("invalid config for %s plugin %q: %w", ProgramAwarePluginType, name, err)
		}
	}

	// Reconcile compatibility alias fields if supplied
	if cfg.LASWeightService > 0 {
		cfg.WeightService = cfg.LASWeightService
	}
	if cfg.LASWeightHeadWait > 0 {
		cfg.WeightServiceHeadWait = cfg.LASWeightHeadWait
	}
	if cfg.LASDecayFactor > 0 {
		cfg.ServiceDecayFactor = cfg.LASDecayFactor
	}
	if cfg.LASHalfLifeSeconds > 0 {
		cfg.ServiceHalfLifeSeconds = cfg.LASHalfLifeSeconds
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("%s plugin %q: %w", ProgramAwarePluginType, name, err)
	}
	strategy, err := newStrategy(cfg)
	if err != nil {
		return nil, fmt.Errorf("%s plugin %q: %w", ProgramAwarePluginType, name, err)
	}
	p := &ProgramAwarePlugin{
		name:     name,
		strategy: strategy,
	}
	if handle != nil {
		if reg := handle.Metrics(); reg != nil {
			for _, c := range GetCollectors() {
				reg.MustRegister(c)
			}
			for _, c := range strategy.Collectors() {
				reg.MustRegister(c)
			}
		}
		if cfg.EvictionTTLSeconds > 0 {
			interval := time.Duration(cfg.EvictionSweepSeconds * float64(time.Second))
			ttl := time.Duration(cfg.EvictionTTLSeconds * float64(time.Second))
			go p.runEviction(handle.Context(), interval, ttl)
		}
	}
	return p, nil
}

// ProgramAwarePlugin implements a FairnessPolicy that selects which program's
// queue to service next, and request lifecycle hooks that track per-program metrics.
//nolint:revive
type ProgramAwarePlugin struct {
	name     string
	strategy ScoringStrategy

	// programMetrics stores aggregated metrics per program.
	// Key: program ID (string), Value: *ProgramMetrics.
	programMetrics sync.Map
}

// TypedName returns the plugin type and instance name.
func (p *ProgramAwarePlugin) TypedName() plugin.TypedName {
	return plugin.TypedName{
		Type: ProgramAwarePluginType,
		Name: p.name,
	}
}

type fairnessDumpState struct {
	TotalPrograms int     `json:"totalPrograms"`
	TotalInFlight int64   `json:"totalInFlight"`
	FairnessIndex float64 `json:"fairnessIndex"`
}

// DumpState reports aggregate fairness health.
func (p *ProgramAwarePlugin) DumpState() (json.RawMessage, error) {
	var totalPrograms int
	var totalInFlight int64
	var sum, sumSq, n float64
	p.programMetrics.Range(func(_, value any) bool {
		totalPrograms++
		m, ok := value.(*ProgramMetrics)
		if !ok {
			return true
		}
		totalInFlight += m.InFlight()
		if m.WaitCount() > 0 {
			x := m.AverageWaitTime()
			sum += x
			sumSq += x * x
			n++
		}
		return true
	})
	return json.Marshal(fairnessDumpState{
		TotalPrograms: totalPrograms,
		TotalInFlight: totalInFlight,
		FairnessIndex: jainFairnessIndex(sum, sumSq, n),
	})
}

func (p *ProgramAwarePlugin) getStrategy() ScoringStrategy {
	if p.strategy == nil {
		s, _ := newStrategy(DefaultConfig())
		return s
	}
	return p.strategy
}

func (p *ProgramAwarePlugin) getOrCreateMetrics(programID string) *ProgramMetrics {
	if metricsRaw, ok := p.programMetrics.Load(programID); ok {
		if m, ok := metricsRaw.(*ProgramMetrics); ok {
			return m
		}
	}
	fresh := &ProgramMetrics{lastCompletionTime: time.Now()}
	actual, _ := p.programMetrics.LoadOrStore(programID, fresh)
	if existing, ok := actual.(*ProgramMetrics); ok {
		return existing
	}
	p.programMetrics.Store(programID, fresh)
	return fresh
}

func programIDFor(req *fwksched.InferenceRequest) string {
	if req == nil || req.FairnessID == "" {
		return metadata.DefaultFairnessID
	}
	return req.FairnessID
}

// NewState creates per-PriorityBand state.
func (p *ProgramAwarePlugin) NewState(_ context.Context) any {
	return nil
}

// Pick selects which program queue to service next by delegating to the
// configured ScoringStrategy.
func (p *ProgramAwarePlugin) Pick(_ context.Context, band flowcontrol.PriorityBandAccessor) (flowcontrol.FlowQueueAccessor, error) {
	if band == nil {
		return nil, nil //nolint:nilnil
	}

	start := time.Now()
	defer func() {
		pickLatencyUs.Observe(float64(time.Since(start).Microseconds()))
	}()

	strategy := p.getStrategy()

	// Build QueueInfo map for the strategy.
	infos := make(map[string]QueueInfo)
	band.IterateQueues(func(queue flowcontrol.FlowQueueAccessor) bool {
		if queue == nil {
			return true
		}
		id := queue.FlowKey().ID
		infos[id] = QueueInfo{
			Queue:   queue,
			Metrics: p.getOrCreateMetrics(id),
			Len:     queue.Len(),
		}
		return true
	})

	bestQueue, scores := strategy.Pick(band.Priority(), infos)

	// Emit per-queue scores for non-empty queues.
	for id, score := range scores {
		queueScore.WithLabelValues(id).Set(score)
	}

	if bestQueue != nil {
		if head := bestQueue.Peek(); head != nil {
			if req := head.OriginalRequest().InferenceRequest(); req != nil {
				req.PutAttribute(enqueueTimeAttributeKey, head.EnqueueTime())
			}
		}
	}

	fairnessIndex.Set(p.computeFairnessIndex())

	return bestQueue, nil
}

func (p *ProgramAwarePlugin) runEviction(ctx context.Context, interval, ttl time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.evictIdle(ttl)
		}
	}
}

func (p *ProgramAwarePlugin) evictIdle(ttl time.Duration) {
	now := time.Now()
	p.programMetrics.Range(func(key, value any) bool {
		m, ok := value.(*ProgramMetrics)
		if !ok {
			p.evictKey(key)
			return true
		}
		if m.InFlight() != 0 {
			return true
		}
		if m.TotalRequests() != m.DispatchedCount() {
			return true
		}
		if m.InFlight() != 0 {
			return true
		}
		last := m.LastCompletionTime()
		if last.IsZero() || now.Sub(last) <= ttl {
			return true
		}
		p.evictKey(key)
		return true
	})
}

func (p *ProgramAwarePlugin) evictKey(key any) {
	p.programMetrics.Delete(key)
	if id, ok := key.(string); ok {
		p.getStrategy().EvictProgram(id)
		DeleteSharedSeries(id)
	}
}

func jainFairnessIndex(sum, sumSq, n float64) float64 {
	if n <= 1 || sumSq == 0 {
		return 1.0
	}
	return (sum * sum) / (n * sumSq)
}

func (p *ProgramAwarePlugin) computeFairnessIndex() float64 {
	var sum, sumSq, n float64
	p.programMetrics.Range(func(_, value any) bool {
		m, ok := value.(*ProgramMetrics)
		if !ok {
			return true
		}
		if m.WaitCount() == 0 {
			return true
		}
		x := m.AverageWaitTime()
		sum += x
		sumSq += x * x
		n++
		return true
	})
	return jainFairnessIndex(sum, sumSq, n)
}
