package programaware_test

import (
	"context"
	"testing"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	requestcontrol "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	requesthandling "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/programaware"
)

func TestProgramAwareScorer(t *testing.T) {
	producerName := "test-producer"
	matchKey := attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(producerName).String()

	// Pod A: Cache hit, but saturated/loaded (load = 5, kvUtil = 0.9)
	attrA := fwkdl.NewAttributes()
	attrA.Put(matchKey, attrprefix.NewPrefixCacheMatchInfo(10, 10, 16)) // 100% cache hit
	endpointA := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: "pod-a"}},
		&fwkdl.Metrics{
			WaitingQueueSize:    5,
			KVCacheUsagePercent: 0.9,
		},
		attrA,
	)

	// Pod B: Cache miss, completely idle/low load (load = 0, kvUtil = 0.1)
	endpointB := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: "pod-b"}},
		&fwkdl.Metrics{
			WaitingQueueSize:    0,
			KVCacheUsagePercent: 0.1,
		},
		nil,
	)

	tests := []struct {
		name         string
		tokensSoFar  int64
		wantSelected string // Which pod should have the higher score
	}{
		{
			name:         "Small program: migrate to idle cache-miss pod B",
			tokensSoFar:  1000,
			wantSelected: "pod-b", // Migrate! Because recomputing 1000 tokens is cheaper than waiting behind 5 queued requests on pod-a
		},
		{
			name:         "Large program: stay/wait on cache-hit pod A",
			tokensSoFar:  12000,
			wantSelected: "pod-a", // Stay! Because recomputing 12000 tokens is more expensive than waiting on pod-a
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := programaware.Config{
				PrefixMatchInfoProducerName: producerName,
				LoadCoefficient:             0.1,
				KvCoefficient:               0.5,
				RecomputeCoefficient:        0.0001,
				QueueThreshold:              100.0,
			}
			ctx := context.Background()
			p := programaware.New(ctx, "test-scorer", cfg)

			// Set program tokens
			p.SetProgramTokens("program-1", tc.tokensSoFar)

			req := &scheduling.InferenceRequest{
				FairnessID: "program-1",
			}

			scores := p.Score(ctx, req, []scheduling.Endpoint{endpointA, endpointB})

			scoreA := scores[endpointA]
			scoreB := scores[endpointB]

			t.Logf("Tokens: %d, Score A (hit, loaded): %f, Score B (miss, idle): %f", tc.tokensSoFar, scoreA, scoreB)

			var gotSelected string
			if scoreA > scoreB {
				gotSelected = "pod-a"
			} else {
				gotSelected = "pod-b"
			}

			if gotSelected != tc.wantSelected {
				t.Errorf("Expected higher score on %s, got A: %f, B: %f", tc.wantSelected, scoreA, scoreB)
			}
		})
	}
}

func TestResponseBodyTokenAccumulation(t *testing.T) {
	ctx := context.Background()
	cfg := programaware.Config{}
	p := programaware.New(ctx, "test-scorer", cfg)

	req := &scheduling.InferenceRequest{
		FairnessID: "program-1",
	}

	resp1 := &requestcontrol.Response{
		EndOfStream: true,
		Usage:       requesthandling.Usage{PromptTokens: 150, CompletionTokens: 50},
	}

	resp2 := &requestcontrol.Response{
		EndOfStream: true,
		Usage:       requesthandling.Usage{PromptTokens: 100, CompletionTokens: 100},
	}

	// Process first request completion
	p.ResponseBody(ctx, req, resp1, nil)
	if tokens := p.GetProgramTokens("program-1"); tokens != 200 {
		t.Errorf("Expected 200 tokens accumulated, got %d", tokens)
	}

	// Process second request completion
	p.ResponseBody(ctx, req, resp2, nil)
	if tokens := p.GetProgramTokens("program-1"); tokens != 400 {
		t.Errorf("Expected 400 tokens accumulated, got %d", tokens)
	}
}
