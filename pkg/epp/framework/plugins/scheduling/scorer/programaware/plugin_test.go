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

	// Pod A: Cache hit, moderate healthy load (running = 2, queue = 0, kvUtil = 0.5)
	attrA := fwkdl.NewAttributes()
	attrA.Put(matchKey, attrprefix.NewPrefixCacheMatchInfo(10, 10, 16)) // 100% cache hit
	endpointA := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-a"}},
		&fwkdl.Metrics{
			RunningRequestsSize: 2,
			WaitingQueueSize:    0,
			KVCacheUsagePercent: 0.5,
		},
		attrA,
	)

	// Pod B: Cache miss, completely idle (load = 0, kvUtil = 0.1)
	endpointB := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-b"}},
		&fwkdl.Metrics{
			WaitingQueueSize:    0,
			KVCacheUsagePercent: 0.1,
		},
		nil,
	)

	tests := []struct {
		name         string
		tokensSoFar  int64
		wantSelected string
	}{
		{
			name:         "Program stays on cache-hit pod A when memory is healthy",
			tokensSoFar:  1000,
			wantSelected: "pod-a",
		},
		{
			name:         "Large program stays on cache-hit pod A",
			tokensSoFar:  12000,
			wantSelected: "pod-a",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := programaware.Config{
				PrefixMatchInfoProducerName: producerName,
			}
			ctx := context.Background()
			p := programaware.New(ctx, "test-scorer", cfg)

			p.SetProgramTokens("program-1", tc.tokensSoFar)
			p.SetPin("program-1", endpointA.GetMetadata().GetNamespacedName().String())

			req := &scheduling.InferenceRequest{
				FairnessID: "program-1",
			}

			scores := p.Score(ctx, req, []scheduling.Endpoint{endpointA, endpointB})

			scoreA := scores[endpointA]
			scoreB := scores[endpointB]

			t.Logf("Tokens: %d, Score A (hit): %f, Score B (miss): %f", tc.tokensSoFar, scoreA, scoreB)

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

	p.ResponseBody(ctx, req, resp1, nil)
	if tokens := p.GetProgramTokens("program-1"); tokens != 200 {
		t.Errorf("Expected 200 tokens accumulated, got %d", tokens)
	}

	p.ResponseBody(ctx, req, resp2, nil)
	if tokens := p.GetProgramTokens("program-1"); tokens != 400 {
		t.Errorf("Expected 400 tokens accumulated, got %d", tokens)
	}
}

func TestZeroQueueLowConcurrency(t *testing.T) {
	producerName := "test-producer"
	matchKey := attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(producerName).String()

	// Pod A: Cache hit (85%), Running = 1, Queue = 0
	attrA := fwkdl.NewAttributes()
	attrA.Put(matchKey, attrprefix.NewPrefixCacheMatchInfo(85, 100, 16))
	endpointA := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-a"}},
		&fwkdl.Metrics{
			RunningRequestsSize: 1,
			WaitingQueueSize:    0,
		},
		attrA,
	)

	// Pod B: Cache miss (0%), Running = 0, Queue = 0
	endpointB := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-b"}},
		&fwkdl.Metrics{
			RunningRequestsSize: 0,
			WaitingQueueSize:    0,
		},
		nil,
	)

	ctx := context.Background()
	p := programaware.New(ctx, "test-scorer", programaware.Config{PrefixMatchInfoProducerName: producerName})
	p.SetPin("program-1", endpointA.GetMetadata().GetNamespacedName().String())

	req := &scheduling.InferenceRequest{FairnessID: "program-1"}
	scores := p.Score(ctx, req, []scheduling.Endpoint{endpointA, endpointB})

	if scores[endpointA] <= scores[endpointB] {
		t.Errorf("Expected cache hit pod A to win under zero queue, got A: %f, B: %f", scores[endpointA], scores[endpointB])
	}
}

func TestSpilloverOnOverloadedQueue(t *testing.T) {
	producerName := "test-producer"
	matchKey := attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(producerName).String()

	// Pod A: Pinned Home Pod with partial cache hit (20%), but heavily overloaded (Running = 15, Queue = 10 -> Load = 35)
	attrA := fwkdl.NewAttributes()
	attrA.Put(matchKey, attrprefix.NewPrefixCacheMatchInfo(20, 100, 16))
	endpointA := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-a"}},
		&fwkdl.Metrics{
			RunningRequestsSize: 15,
			WaitingQueueSize:    10,
		},
		attrA,
	)

	// Pod B: Idle Pod (Running = 0, Queue = 0, Cache = 0)
	endpointB := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-b"}},
		&fwkdl.Metrics{
			RunningRequestsSize: 0,
			WaitingQueueSize:    0,
		},
		nil,
	)

	ctx := context.Background()
	p := programaware.New(ctx, "test-scorer", programaware.Config{PrefixMatchInfoProducerName: producerName})
	p.SetPin("program-1", endpointA.GetMetadata().GetNamespacedName().String())

	req := &scheduling.InferenceRequest{FairnessID: "program-1"}
	scores := p.Score(ctx, req, []scheduling.Endpoint{endpointA, endpointB})

	if scores[endpointB] <= scores[endpointA] {
		t.Errorf("Expected idle pod B to win over overloaded pod A, got A: %f, B: %f", scores[endpointA], scores[endpointB])
	}
}

func TestColdStartLeastPinsBalancing(t *testing.T) {
	// Pod A: Pinned by 10 programs
	endpointA := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-a"}},
		&fwkdl.Metrics{WaitingQueueSize: 0},
		nil,
	)

	// Pod B: Pinned by 1 program
	endpointB := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-b"}},
		&fwkdl.Metrics{WaitingQueueSize: 0},
		nil,
	)

	ctx := context.Background()
	p := programaware.New(ctx, "test-scorer", programaware.Config{})
	for i := 0; i < 10; i++ {
		p.SetPin(string(rune('A'+i)), endpointA.GetMetadata().GetNamespacedName().String())
	}
	p.SetPin("other-prog", endpointB.GetMetadata().GetNamespacedName().String())

	// Brand new program (cold start)
	req := &scheduling.InferenceRequest{FairnessID: "brand-new-program"}
	scores := p.Score(ctx, req, []scheduling.Endpoint{endpointA, endpointB})

	if scores[endpointB] <= scores[endpointA] {
		t.Errorf("Expected less-pinned pod B to win cold start, got A: %f, B: %f", scores[endpointA], scores[endpointB])
	}
}

func TestSpilloverOn100PercentWarmOverloadedRunning(t *testing.T) {
	producerName := "test-producer"
	matchKey := attrprefix.PrefixCacheMatchInfoDataKey.WithNonEmptyProducerName(producerName).String()

	// Pod A: Pinned Home Pod with 100% cache hit, but heavily overloaded with running decodes (Running = 20, Queue = 0)
	attrA := fwkdl.NewAttributes()
	attrA.Put(matchKey, attrprefix.NewPrefixCacheMatchInfo(100, 100, 16))
	endpointA := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-a"}},
		&fwkdl.Metrics{
			RunningRequestsSize: 20,
			WaitingQueueSize:    0,
		},
		attrA,
	)

	// Pod B: Idle Pod (Running = 0, Queue = 0, Cache = 0)
	endpointB := scheduling.NewEndpoint(
		&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod-b"}},
		&fwkdl.Metrics{
			RunningRequestsSize: 0,
			WaitingQueueSize:    0,
		},
		nil,
	)

	ctx := context.Background()
	p := programaware.New(ctx, "test-scorer", programaware.Config{PrefixMatchInfoProducerName: producerName})
	p.SetPin("program-1", endpointA.GetMetadata().GetNamespacedName().String())

	req := &scheduling.InferenceRequest{FairnessID: "program-1"}
	scores := p.Score(ctx, req, []scheduling.Endpoint{endpointA, endpointB})

	if scores[endpointB] <= scores[endpointA] {
		t.Errorf("Expected idle pod B to win over overloaded warm pod A (20 running), got A: %f, B: %f", scores[endpointA], scores[endpointB])
	}
}

