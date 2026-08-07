package programawarepinning_test

import (
	"context"
	"testing"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/programawarepinning"
)

func TestProgramAwarePinningScorer(t *testing.T) {
	ctx := context.Background()
	p := programawarepinning.NewProgramAware("test-pinning", &programawarepinning.Parameters{
		MissThreshold: 3,
	})

	if p.TypedName().Type != "program-aware-pinning" {
		t.Fatalf("unexpected plugin type: %s", p.TypedName().Type)
	}

	podA := scheduling.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: "pod-a"}}, nil, nil)
	podB := scheduling.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Namespace: "default", Name: "pod-b"}}, nil, nil)
	pods := []scheduling.Endpoint{podA, podB}

	req1 := &scheduling.InferenceRequest{FairnessID: "prog1"}

	// Initial score: no pin yet, should pick least loaded round-robin podA
	scores := p.Score(ctx, req1, pods)
	if scores[podA] != 1.0 {
		t.Errorf("expected podA score 1.0, got %f", scores[podA])
	}

	// Commit pin to podA
	result1 := &scheduling.SchedulingResult{
		PrimaryProfileName: "default",
		ProfileResults: map[string]*scheduling.ProfileRunResult{
			"default": {
				TargetEndpoints: []scheduling.Endpoint{podA},
			},
		},
	}
	p.PreRequest(ctx, req1, result1)

	// Subsequent score for prog1 should strongly prefer podA
	scores2 := p.Score(ctx, req1, pods)
	if scores2[podA] != 1.0 || scores2[podB] != 0.0 {
		t.Errorf("expected podA=1.0, podB=0.0, got podA=%f, podB=%f", scores2[podA], scores2[podB])
	}

	// DumpState test
	dumpRaw, err := p.DumpState()
	if err != nil {
		t.Fatalf("failed to dump state: %v", err)
	}
	if len(dumpRaw) == 0 {
		t.Fatal("dumpState returned empty payload")
	}
}
