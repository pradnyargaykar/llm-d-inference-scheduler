# Program-Aware Load Balancing Scorer - Design Document

## Overview

This document describes the design for a program-aware load balancing scorer that combines **request budget management** with **prefix cache matching** to achieve fair resource distribution and efficient request routing in the LLM-D inference scheduler.

## Core Concept

Each Program ID (FairnessID) is allocated a **fixed request budget** across every pod within the cluster. The system intelligently routes requests based on:
1. **Budget availability** - Ensures fair resource distribution
2. **Prefix cache matching** - Maximizes cache hit rates for subsequent requests
3. **KV cache utilization** - Tie-breaking mechanism for optimal load distribution

## Three Core Principles

### 1. Admission Control (Initial Program ID Request)

**Purpose**: Route first-time requests from a program to the most suitable pod.

**Logic**:
- Select pod with **highest available budget**
- **Tie-breaker**: Pod with lowest KV cache utilization
- **Final tie-breaker**: Random selection

### 2. Flow Control (Subsequent Requests)

**Purpose**: Route subsequent requests to maximize cache hits while respecting budgets.

**Logic**:
- **Budget & Prefix Match**: If prefix matches a pod WITH available budget → route to that pod
- **Prefix Match without Budget**: If prefix matches a pod but budget exhausted → **force migration** to pod with highest budget (KV utilization for tie-breaking)

### 3. Budget Refreshment (Request Replenishment)

**Purpose**: Sustain ongoing requests through periodic budget replenishment.

**Logic**:
- Replenishment is **load-dependent**
- **Low utilization pods**: Aggressive budget increase (2x base)
- **Medium utilization pods**: Normal budget increase (1.5x base)
- **High utilization pods**: Base budget increase (1x base)
- Runs periodically (every T minutes/seconds - configurable)

## Detailed Logical Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Request Initiation                                        │
│    - Request arrives with FairnessID (Program ID)           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. First-Time vs Subsequent Check                           │
│    - Check if program has ANY budget used across all pods   │
│    - IF UsedRequests == 0 for all pods: First-time request  │
│    - ELSE: Subsequent request                               │
│    - (Optimization: No need to query cache scores)          │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │                   │
                    ▼                   ▼
    ┌───────────────────────┐   ┌───────────────────────┐
    │ First-Time Request    │   │ Subsequent Request    │
    │                       │   │                       │
    │ Route to pod with:    │   │ Route to pod with:    │
    │ a. Highest budget     │   │ a. Best prefix match  │
    │ b. Lowest KV util     │   │ b. IF budget OK:      │
    │    (tie-breaker)      │   │    → Use that pod     │
    │ c. Random             │   │ c. IF budget exhausted│
    │    (final tie)        │   │    → Force migration  │
    │                       │   │       to highest      │
    │                       │   │       budget pod      │
    └───────────────────────┘   └───────────────────────┘
                    │                   │
                    └─────────┬─────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Process Request                                           │
│    - Execute request on selected pod                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. Post-Response Handling                                    │
│    - Deduct 1 request from budget                           │
│    - Budget cannot go negative (hard limit)                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. Background Refresh (Every T minutes/seconds)             │
│    - Calculate pod utilization                              │
│    - Low utilization: Increase budget (2x base)             │
│    - Medium utilization: Normal budget (1.5x base)          │
│    - High utilization: Base budget (1x base)                │
└─────────────────────────────────────────────────────────────┘
```

## Architecture Components

### Component 1: Budget Tracker

**File**: `budget_tracker.go` ✅ (Completed - needs minor updates)

**Purpose**: Track request budgets per program per pod

**Data Structure**:
```go
type Budget struct {
    TotalRequests    int64  // Current budget (cannot go negative)
    UsedRequests     int64  // Historical usage
    ReservedRequests int64  // In-flight reservations
    LastRefresh      time.Time
    Reservations     map[string]time.Time  // requestID -> timestamp
}

type BudgetTracker struct {
    budgets       map[string]map[string]*Budget  // programID -> podName -> Budget
    defaultBudget int64  // Initial budget per program per pod (e.g., 1000)
    mu            sync.RWMutex
}
```

**Key Methods**:
- `Reserve(programID, podName, requestID string) error` - Reserve 1 request slot (fails if budget exhausted)
- `GetAvailable(programID, podName string) int64` - Get available budget
- `ReleaseAndDeduct(programID, podName, requestID string) error` - Release reservation and deduct 1
- `IsFirstTimeProgram(programID string) bool` - Check if program has never been used (optimization)
- `RefreshBudgets(utilizationMap map[string]float64)` - Periodic refresh based on utilization

**Budget Constraints**:
- Budgets **cannot go negative** (hard limit at 0)
- Reserve() fails if available budget = 0
- This prevents over-allocation since we're counting actual requests

**First-Time Detection Optimization**:
```go
func (bt *BudgetTracker) IsFirstTimeProgram(programID string) bool {
    bt.mu.RLock()
    defer bt.mu.RUnlock()
    
    podBudgets, exists := bt.budgets[programID]
    if !exists {
        return true  // Never seen this program
    }
    
    // Check if any pod has used requests
    for _, budget := range podBudgets {
        if budget.UsedRequests > 0 {
            return false  // Program has been used
        }
    }
    
    return true  // Program exists but never used
}
```

**Updates Needed**:
- Add `LastRefresh` field to Budget struct
- Add `IsFirstTimeProgram()` method for optimization
- Add `RefreshBudgets()` method for periodic refresh
- Add `RefreshPodBudgets()` method for per-pod refresh
- Add `EmergencyRefresh()` method for exhaustion protocol

### Component 2: Program-Aware Scorer

**File**: `program_aware_scorer.go` (To be implemented)

**Purpose**: Score pods based on budget, cache, and utilization

**Scoring Logic**:

```go
func (s *ProgramAwareScorer) Score(ctx context.Context, req *request.Request, pod *types.Pod) (float64, error) {
    programID := req.FairnessID
    if programID == "" {
        programID = metadata.DefaultFairnessID
    }
    
    // Optimization: Check if first-time program (no cache query needed)
    isFirstTime := s.budgetTracker.IsFirstTimeProgram(programID)
    
    // Get budget availability
    availableBudget := s.budgetTracker.GetAvailable(programID, pod.Name)
    
    // Get KV cache utilization
    kvUtilization := s.getKVUtilization(pod)
    
    if isFirstTime {
        // First-time request: Budget-first routing (skip cache query)
        return s.scoreFirstTime(availableBudget, kvUtilization), nil
    } else {
        // Subsequent request: Cache-aware routing
        cacheScore := s.getCacheScore(req, pod)
        return s.scoreSubsequent(cacheScore, availableBudget, kvUtilization), nil
    }
}

func (s *ProgramAwareScorer) scoreFirstTime(budget int64, kvUtil float64) float64 {
    if budget <= 0 {
        return 0.0  // No budget = no routing
    }
    
    // Higher budget = higher score
    budgetScore := float64(budget) / float64(s.defaultBudget) * 100.0
    
    // Lower KV utilization = higher score (tie-breaker)
    utilizationScore := (1.0 - kvUtil) * 10.0
    
    return budgetScore + utilizationScore
}

func (s *ProgramAwareScorer) scoreSubsequent(cacheScore float64, budget int64, kvUtil float64) float64 {
    if budget > 0 {
        // Budget available: Prioritize cache match
        return cacheScore * 100.0 + float64(budget) / float64(s.defaultBudget) * 10.0
    } else {
        // Budget exhausted: Force migration to highest budget pod
        // This pod gets score 0, forcing selection of pod with budget
        return 0.0
    }
}
```

### Component 3: Budget Reserver (PreRequest Hook)

**File**: `budget_reserver.go` (To be implemented)

**Purpose**: Reserve 1 request slot before request processing

**Implementation**:
```go
func (p *BudgetReserver) PreRequest(ctx context.Context, req *request.Request) error {
    programID := req.FairnessID
    if programID == "" {
        programID = metadata.DefaultFairnessID
    }
    
    podName := req.SelectedPod
    requestID := req.ID
    
    // Reserve 1 request slot (fails if budget = 0)
    err := p.budgetTracker.Reserve(programID, podName, requestID)
    if err != nil {
        return fmt.Errorf("budget reservation failed: %w", err)
    }
    
    return nil
}
```

### Component 4: Budget Deductor (ResponseBodyProcessor)

**File**: `budget_deductor.go` (To be implemented)

**Purpose**: Deduct 1 request after request completion

**Implementation**:
```go
func (p *BudgetDeductor) ProcessResponseBody(ctx context.Context, req *request.Request, resp *response.Response) error {
    programID := req.FairnessID
    if programID == "" {
        programID = metadata.DefaultFairnessID
    }
    
    podName := req.SelectedPod
    requestID := req.ID
    
    // Release reservation and deduct 1 request
    err := p.budgetTracker.ReleaseAndDeduct(programID, podName, requestID)
    if err != nil {
        log.Errorf("Failed to release and deduct budget: %v", err)
    }
    
    return nil
}
```

### Component 5: Budget Refresher (Background Service)

**File**: `budget_refresher.go` (To be implemented)

**Purpose**: Periodically refresh budgets based on pod utilization

**Implementation**:
```go
func (r *BudgetRefresher) Start(ctx context.Context) {
    ticker := time.NewTicker(r.refreshInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            r.refreshBudgets()
        case <-ctx.Done():
            return
        }
    }
}

func (r *BudgetRefresher) refreshBudgets() {
    // Get utilization for all pods
    utilizationMap := r.getUtilizationMap()
    
    // Calculate refresh amounts based on utilization
    for podName, utilization := range utilizationMap {
        var multiplier float64
        if utilization < 0.3 {
            multiplier = 2.0  // Low utilization: 2x base
        } else if utilization < 0.7 {
            multiplier = 1.5  // Medium utilization: 1.5x base
        } else {
            multiplier = 1.0  // High utilization: 1x base
        }
        
        // Refresh budgets for all programs on this pod
        refreshAmount := int64(float64(r.baseBudget) * multiplier)
        r.budgetTracker.RefreshPodBudgets(podName, refreshAmount)
    }
}
```

## Configuration

**File**: `deploy/config/program-aware-config.yaml`

```yaml
plugins:
  scheduling:
    scorers:
      - name: program-aware
        enabled: true
        config:
          defaultBudget: 1000  # Initial requests per program per pod
          refreshInterval: 60s  # Budget refresh interval
          baseBudget: 100  # Base refresh amount
          lowUtilizationThreshold: 0.3
          mediumUtilizationThreshold: 0.7
          lowUtilizationMultiplier: 2.0
          mediumUtilizationMultiplier: 1.5
          highUtilizationMultiplier: 1.0
  
  prerequest:
    - name: budget-reserver
      enabled: true
  
  responsebody:
    - name: budget-deductor
      enabled: true
```

## Exhaustion Protocol

**When all budgets are exhausted**:
1. Trigger immediate refresh for idle pods
2. Idle pods get emergency budget boost
3. Requests may queue or be rejected if no capacity

**Implementation**:
```go
func (s *ProgramAwareScorer) handleExhaustion(programID string) {
    // Find idle pods (low utilization)
    idlePods := s.findIdlePods()
    
    if len(idlePods) > 0 {
        // Emergency refresh for idle pods
        for _, pod := range idlePods {
            s.budgetTracker.EmergencyRefresh(programID, pod.Name, 200) // 2x base
        }
    }
}
```

## Key Design Decisions

### 1. Request-Based Budgets (Not Token-Based)
- **Decision**: Count requests, not tokens
- **Rationale**: Simpler implementation, no token estimation needed
- **Benefit**: Budgets cannot go negative since we reserve before processing
- **Trade-off**: Less accurate resource accounting, but easier to implement

### 2. Hard Budget Limit (No Overdraft)
- **Decision**: Budgets cannot go negative
- **Rationale**: Request-based counting allows us to reserve before processing
- **Benefit**: Prevents over-allocation, simpler logic
- **Trade-off**: Requires refresh mechanism to replenish budgets

### 3. First-Time Detection Optimization
- **Decision**: Use budget tracker to detect first-time programs (not cache scores)
- **Rationale**: Saves compute time by avoiding cache queries for first-time requests
- **Benefit**: Faster routing decisions, reduced latency
- **Implementation**: Check if UsedRequests == 0 across all pods

### 4. Cache vs Budget Priority
- **First-time**: Budget-first (no cache query needed)
- **Subsequent with budget**: Cache-first (maximize efficiency)
- **Subsequent without budget**: Force migration (score = 0 for exhausted pods)

### 5. Utilization-Based Refresh
- **Decision**: Load-dependent refresh rates
- **Rationale**: Idle pods can serve more requests, busy pods need throttling
- **Trade-off**: Requires accurate utilization metrics

## Implementation Status

### Completed ✅
- Budget tracker core (budget_tracker.go) - request-based
- Unit tests (budget_tracker_test.go)
- Design document

### Updates Needed 🔄
- Update budget_tracker.go to add:
  - `LastRefresh` field
  - `IsFirstTimeProgram()` method (optimization)
  - `RefreshBudgets()` method
  - `RefreshPodBudgets()` method
  - `EmergencyRefresh()` method

### TODO ⏸️
- Program-aware scorer (program_aware_scorer.go)
- Budget reserver (budget_reserver.go)
- Budget deductor (budget_deductor.go)
- Budget refresher (budget_refresher.go)
- Integration with cache scorer
- KV utilization metrics integration
- Plugin registration
- Configuration updates
- Integration tests

## Next Steps

1. **Update budget_tracker.go** to add refresh methods and IsFirstTimeProgram()
2. **Implement program_aware_scorer.go** with optimized first-time detection
3. **Implement budget_reserver.go** for PreRequest hook
4. **Implement budget_deductor.go** for ResponseBodyProcessor
5. **Implement budget_refresher.go** with utilization-based refresh
6. **Integration testing** with real workloads

---

**Document Version**: 2.3  
**Last Updated**: 2026-06-17  
**Author**: Bob (AI Assistant)  
**Status**: Living Document - Request-based with first-time optimization
