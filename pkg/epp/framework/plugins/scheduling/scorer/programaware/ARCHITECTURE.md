# Program-Aware Budget-Based Routing - Architecture Documentation

## Overview

This document provides a detailed architecture of the program-aware budget-based routing system, including all files, their functions, data structures, and call flows.

---

## Directory Structure

```
pkg/epp/framework/plugins/scheduling/scorer/programaware/
├── ARCHITECTURE.md          # This file - detailed architecture documentation
├── DESIGN.md               # High-level design and principles (449 lines)
├── budget_tracker.go       # Core budget tracking logic (455 lines)
├── budget_tracker_test.go  # Unit tests for budget tracker (385 lines)
├── plugin.go               # Main plugin with scoring and hooks (359 lines)
└── budget_refresher.go     # Background service for budget refresh (TODO)
```

---

## File-by-File Breakdown

### 1. `budget_tracker.go` (455 lines)

**Purpose**: Core budget tracking and management

#### Data Structures

```go
// Main budget tracker
type BudgetTracker struct {
    mu                    sync.RWMutex
    budgets              map[string]map[string]*PodBudget  // programID -> podID -> budget
    reservations         map[string]*Reservation            // reservationID -> reservation
    requestsPerPod       int                                // Budget quota per pod
    reservationTimeout   time.Duration                      // Timeout for stale reservations
    cleanupInterval      time.Duration                      // Cleanup goroutine interval
    stopCleanup          chan struct{}                      // Signal to stop cleanup
}

// Per-pod budget for a program
type PodBudget struct {
    ProgramID        string
    PodID            string
    TotalRequests    int       // Total budget quota
    UsedRequests     int       // Completed requests
    ReservedRequests int       // In-flight requests
    LastRefresh      time.Time // Last budget refresh time
}

// Reservation for in-flight request
type Reservation struct {
    ID          string
    ProgramID   string
    PodID       string
    RequestID   string
    CreatedAt   time.Time
}
```

#### Public Functions

| Function | Purpose | Called By |
|----------|---------|-----------|
| `NewBudgetTracker(requestsPerPod, reservationTimeout, cleanupInterval)` | Constructor, starts cleanup goroutine | Plugin initialization |
| `Reserve(ctx, programID, podID, requestID) (reservationID, error)` | Reserve budget for in-flight request | `plugin.PreRequest()` |
| `GetAvailable(ctx, programID, podID) int` | Get available budget (Total - Used - Reserved) | `plugin.Score()` |
| `ReleaseAndDeduct(ctx, programID, podID, requestID) error` | Release reservation and deduct from budget | `plugin.ResponseBody()` |
| `IsFirstTimeProgram(ctx, programID) bool` | Check if program has any budgets | `plugin.Score()` for optimization |
| `RefreshPodBudgets(ctx, programID, podIDs) error` | Refresh budgets for specific pods | Budget refresher service |
| `RefreshAllBudgets(ctx) error` | Refresh all budgets (reset counters) | Budget refresher service |
| `EmergencyRefreshMultiplePods(ctx, programID, podIDs) error` | Emergency refresh when budgets exhausted | Budget refresher service |
| `Close()` | Stop cleanup goroutine | Plugin shutdown |

#### Private Functions

| Function | Purpose |
|----------|---------|
| `getOrCreateBudget(programID, podID) *PodBudget` | Lazy initialization of budgets |
| `cleanupStaleReservations()` | Background goroutine to cleanup timeouts |
| `releaseReservation(reservationID) error` | Release a specific reservation |

#### Key Behaviors

- **Thread-safe**: All operations protected by `sync.RWMutex`
- **Lazy initialization**: Budgets created on first use
- **Auto-cleanup**: Background goroutine removes stale reservations
- **No overdraft**: Available budget cannot go negative

---

### 2. `budget_tracker_test.go` (385 lines)

**Purpose**: Comprehensive unit tests for budget tracker

#### Test Functions

| Test Function | What It Tests | Lines |
|---------------|---------------|-------|
| `TestNewBudgetTracker` | Constructor initialization | 15 |
| `TestReserve` | Basic reservation functionality | 35 |
| `TestGetAvailable` | Available budget calculation | 40 |
| `TestReleaseAndDeduct` | Release and deduction logic | 45 |
| `TestReserveExhaustedBudget` | Budget exhaustion handling | 35 |
| `TestConcurrentReserve` | Concurrent reservation safety | 50 |
| `TestCleanupStaleReservations` | Stale reservation cleanup | 45 |
| `TestMultipleProgramsAndPods` | Multi-program/pod scenarios | 40 |
| `TestIsFirstTimeProgram` | First-time program detection | 30 |
| `TestRefreshPodBudgets` | Pod budget refresh | 35 |
| `TestRefreshAllBudgets` | Global budget refresh | 30 |
| `TestEmergencyRefreshMultiplePods` | Emergency refresh | 35 |

**Test Coverage**: All public functions, concurrent access, edge cases

**Status**: ✅ All tests passing

---

### 3. `plugin.go` (359 lines)

**Purpose**: Main plugin implementing scoring and request lifecycle hooks

#### Data Structures

```go
type Plugin struct {
    name                string
    budgetTracker       *BudgetTracker
    prefixCacheScorer   scheduling.Scorer  // Delegate for cache scoring
    cacheWeight         float64            // Weight for cache score (0.0-1.0)
    budgetWeight        float64            // Weight for budget score (0.0-1.0)
}

type Config struct {
    RequestsPerPod      int           // Budget quota per pod
    ReservationTimeout  time.Duration // Timeout for reservations
    CleanupInterval     time.Duration // Cleanup interval
    CacheWeight         float64       // Cache score weight
    BudgetWeight        float64       // Budget score weight
}
```

#### Interface Implementations

```go
// Compile-time assertions
var (
    _ scheduling.Scorer                    = &Plugin{}
    _ requestcontrol.PreRequest            = &Plugin{}
    _ requestcontrol.ResponseBodyProcessor = &Plugin{}
)
```

#### Public Functions

| Function | Interface | Purpose | Called By |
|----------|-----------|---------|-----------|
| `New(config) (*Plugin, error)` | Constructor | Initialize plugin | Framework registry |
| `Name() string` | `plugin.Plugin` | Return plugin name | Framework |
| `Score(ctx, req, pods) ([]ScoredPod, error)` | `scheduling.Scorer` | Score pods for routing | Scheduler |
| `PreRequest(ctx, req, targetEndpoint) error` | `requestcontrol.PreRequest` | Reserve budget before routing | Request pipeline |
| `ResponseBody(ctx, req, resp, targetEndpoint)` | `requestcontrol.ResponseBodyProcessor` | Deduct budget after completion | Response pipeline |

#### Private Helper Functions

| Function | Purpose | Called By |
|----------|---------|-----------|
| `getCacheScore(ctx, req, pod) (float64, error)` | Get prefix cache score | `Score()` |
| `getKVUtilization(pod) float64` | Get KV cache utilization | `Score()` |

#### Scoring Logic Flow

```
Score() called by scheduler
    │
    ├─> Check if first-time program (IsFirstTimeProgram)
    │   │
    │   ├─> YES: First-time request
    │   │   └─> Use cache score only (100% cache weight)
    │   │       └─> Return scored pods
    │   │
    │   └─> NO: Subsequent request
    │       │
    │       ├─> For each pod:
    │       │   ├─> Get cache score (getCacheScore)
    │       │   ├─> Get available budget (GetAvailable)
    │       │   ├─> Calculate budget score (available / total)
    │       │   └─> Combine: score = (cache * cacheWeight) + (budget * budgetWeight)
    │       │
    │       └─> Return scored pods (sorted by score)
```

#### Request Lifecycle Flow

```
1. SCORING PHASE
   Scheduler calls Score()
   └─> Evaluates pods based on cache + budget
   └─> Returns ranked list of pods

2. PRE-REQUEST PHASE
   Request pipeline calls PreRequest()
   └─> Extracts programID and podID
   └─> Calls budgetTracker.Reserve()
   └─> Reserves budget for in-flight request
   └─> Logs success/failure (non-blocking)

3. REQUEST PROCESSING
   Request sent to selected pod
   └─> Pod processes request
   └─> Generates response (may be streaming)

4. RESPONSE PHASE
   Response pipeline calls ResponseBody()
   └─> Checks if EndOfStream (final chunk)
   └─> Extracts programID and podID
   └─> Calls budgetTracker.ReleaseAndDeduct()
   └─> Releases reservation and deducts budget
   └─> Logs success/failure (non-blocking)
```

---

### 4. `budget_refresher.go` (TODO - Not Yet Implemented)

**Purpose**: Background service for periodic budget refresh

#### Planned Data Structures

```go
type BudgetRefresher struct {
    budgetTracker   *BudgetTracker
    refreshInterval time.Duration
    stopChan        chan struct{}
}
```

#### Planned Functions

| Function | Purpose | Called By |
|----------|---------|-----------|
| `NewBudgetRefresher(tracker, interval)` | Constructor | Plugin initialization |
| `Start()` | Start background refresh goroutine | Plugin initialization |
| `Stop()` | Stop background goroutine | Plugin shutdown |
| `refreshLoop()` | Main refresh loop (private) | Background goroutine |

#### Planned Refresh Logic

```
Background goroutine runs every N minutes
    │
    ├─> Call budgetTracker.RefreshAllBudgets()
    │   └─> Resets all Used and Reserved counters
    │   └─> Updates LastRefresh timestamp
    │
    └─> Log refresh statistics
```

---

### 5. `DESIGN.md` (449 lines - Already Exists)

**Purpose**: High-level design document

**Contents**:
- System overview
- Core principles (Admission Control, Flow Control, Budget Refreshment)
- Request-based budget model
- Budget lifecycle
- Scoring algorithm
- Configuration options
- Monitoring and observability

---

## Complete Call Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     REQUEST ARRIVES                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 1: SCORING (Scheduler)                                    │
├─────────────────────────────────────────────────────────────────┤
│  1. Scheduler calls plugin.Score(ctx, req, pods)                 │
│     │                                                             │
│     ├─> Extract programID from req.FairnessID                    │
│     │                                                             │
│     ├─> Check budgetTracker.IsFirstTimeProgram(programID)        │
│     │   │                                                         │
│     │   ├─> YES: First-time request                              │
│     │   │   └─> Use cache score only (skip budget check)         │
│     │   │                                                         │
│     │   └─> NO: Subsequent request                               │
│     │       └─> For each pod:                                    │
│     │           ├─> getCacheScore(pod) via prefix cache scorer   │
│     │           ├─> budgetTracker.GetAvailable(programID, podID) │
│     │           ├─> Calculate budget score                       │
│     │           └─> Combine scores with weights                  │
│     │                                                             │
│     └─> Return sorted list of scored pods                        │
│                                                                   │
│  2. Scheduler selects highest-scoring pod                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 2: PRE-REQUEST (Request Pipeline)                         │
├─────────────────────────────────────────────────────────────────┤
│  1. Request pipeline calls plugin.PreRequest(ctx, req, endpoint) │
│     │                                                             │
│     ├─> Extract programID from req.FairnessID                    │
│     ├─> Extract podID from endpoint.ID                           │
│     ├─> Extract requestID from req.RequestMetadata.RequestID     │
│     │                                                             │
│     └─> budgetTracker.Reserve(programID, podID, requestID)       │
│         │                                                         │
│         ├─> Lock budget tracker                                  │
│         ├─> Get or create budget for (programID, podID)          │
│         ├─> Check available budget                               │
│         │   └─> Available = Total - Used - Reserved              │
│         │                                                         │
│         ├─> If available > 0:                                    │
│         │   ├─> Increment ReservedRequests                       │
│         │   ├─> Create reservation record                        │
│         │   └─> Return reservationID                             │
│         │                                                         │
│         └─> If available <= 0:                                   │
│             └─> Return error (budget exhausted)                  │
│                                                                   │
│  2. Request proceeds to pod (budget reserved)                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 3: REQUEST PROCESSING (Pod)                               │
├─────────────────────────────────────────────────────────────────┤
│  1. Pod receives request                                         │
│  2. Pod processes request (may take seconds/minutes)             │
│  3. Pod generates response (may be streaming)                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  PHASE 4: RESPONSE (Response Pipeline)                           │
├─────────────────────────────────────────────────────────────────┤
│  1. Response pipeline calls plugin.ResponseBody(ctx, req, resp)  │
│     │                                                             │
│     ├─> Check if resp.EndOfStream == true                        │
│     │   └─> If false: return (not final chunk)                   │
│     │                                                             │
│     ├─> Extract programID from req.FairnessID                    │
│     ├─> Extract podID from endpoint.ID                           │
│     ├─> Extract requestID from req.RequestMetadata.RequestID     │
│     │                                                             │
│     └─> budgetTracker.ReleaseAndDeduct(programID, podID, reqID)  │
│         │                                                         │
│         ├─> Lock budget tracker                                  │
│         ├─> Find reservation by requestID                        │
│         ├─> Delete reservation record                            │
│         ├─> Decrement ReservedRequests                           │
│         ├─> Increment UsedRequests                               │
│         └─> Unlock budget tracker                                │
│                                                                   │
│  2. Response returned to client                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  BACKGROUND: CLEANUP & REFRESH                                   │
├─────────────────────────────────────────────────────────────────┤
│  Cleanup Goroutine (runs every cleanupInterval):                 │
│  └─> budgetTracker.cleanupStaleReservations()                    │
│      └─> Find reservations older than timeout                    │
│      └─> Release them (decrement ReservedRequests)               │
│                                                                   │
│  Refresh Service (runs every refreshInterval):                   │
│  └─> budgetRefresher.refreshLoop()                               │
│      └─> budgetTracker.RefreshAllBudgets()                       │
│          └─> Reset UsedRequests = 0                              │
│          └─> Reset ReservedRequests = 0                          │
│          └─> Update LastRefresh timestamp                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Example

### Scenario: Program "prog1" sends 3 requests to 2 pods

**Initial State**:
```
Pod A: Total=1000, Used=0, Reserved=0, Available=1000
Pod B: Total=1000, Used=0, Reserved=0, Available=1000
```

**Request 1**:
```
1. Score() → Pod A score=0.9, Pod B score=0.8 → Select Pod A
2. PreRequest() → Reserve(prog1, podA, req1) → reservationID="res1"
   Pod A: Total=1000, Used=0, Reserved=1, Available=999
3. Process request on Pod A
4. ResponseBody() → ReleaseAndDeduct(prog1, podA, req1)
   Pod A: Total=1000, Used=1, Reserved=0, Available=999
```

**Request 2**:
```
1. Score() → Pod A score=0.899, Pod B score=0.9 → Select Pod B
2. PreRequest() → Reserve(prog1, podB, req2) → reservationID="res2"
   Pod B: Total=1000, Used=0, Reserved=1, Available=999
3. Process request on Pod B
4. ResponseBody() → ReleaseAndDeduct(prog1, podB, req2)
   Pod B: Total=1000, Used=1, Reserved=0, Available=999
```

**Request 3** (while Request 2 still processing):
```
1. Score() → Pod A score=0.899, Pod B score=0.899 → Select Pod A
2. PreRequest() → Reserve(prog1, podA, req3) → reservationID="res3"
   Pod A: Total=1000, Used=1, Reserved=1, Available=998
3. Process request on Pod A
4. ResponseBody() → ReleaseAndDeduct(prog1, podA, req3)
   Pod A: Total=1000, Used=2, Reserved=0, Available=998
```

**After Budget Refresh**:
```
RefreshAllBudgets() called
Pod A: Total=1000, Used=0, Reserved=0, Available=1000
Pod B: Total=1000, Used=0, Reserved=0, Available=1000
```

---

## Integration Points

### 1. Framework Registry
**File**: `pkg/epp/framework/plugins/registry.go` (or similar)

**Integration**:
```go
import programaware "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/programaware"

func init() {
    // Register program-aware scorer plugin
    registry.RegisterScorer("programaware", func(config interface{}) (scheduling.Scorer, error) {
        cfg := config.(*programaware.Config)
        return programaware.New(cfg)
    })
}
```

### 2. Configuration
**File**: `deploy/config/sim-program-aware-config.yaml`

**Configuration**:
```yaml
plugins:
  scoring:
    - name: programaware
      enabled: true
      config:
        requestsPerPod: 1000
        reservationTimeout: 5m
        cleanupInterval: 1m
        cacheWeight: 0.3
        budgetWeight: 0.7
```

### 3. Scheduler
**File**: `pkg/epp/framework/scheduler/scheduler.go`

**Integration**: Scheduler automatically calls `Score()` on all registered scorers

### 4. Request Pipeline
**File**: `pkg/epp/framework/requestcontrol/pipeline.go`

**Integration**: Pipeline automatically calls `PreRequest()` on all registered pre-request hooks

### 5. Response Pipeline
**File**: `pkg/epp/framework/requestcontrol/response.go`

**Integration**: Pipeline automatically calls `ResponseBody()` on all registered response processors

---

## Thread Safety

### Concurrent Access Patterns

1. **Multiple requests scoring simultaneously**:
   - `Score()` uses `budgetTracker.GetAvailable()` with read lock
   - Safe for concurrent reads

2. **Reservation during scoring**:
   - Race condition possible between `Score()` and `PreRequest()`
   - Accepted as rare and self-correcting
   - Can optimize later with custom picker if needed

3. **Multiple requests completing simultaneously**:
   - `ResponseBody()` uses `ReleaseAndDeduct()` with write lock
   - Serialized updates, thread-safe

4. **Cleanup during operations**:
   - Cleanup goroutine uses write lock
   - Serialized with other operations

### Lock Hierarchy
```
BudgetTracker.mu (RWMutex)
  ├─> Read operations: GetAvailable(), IsFirstTimeProgram()
  └─> Write operations: Reserve(), ReleaseAndDeduct(), cleanup
```

---

## Error Handling

### Non-Blocking Errors
- `PreRequest()` errors: Log but allow request to proceed
- `ResponseBody()` errors: Log but don't fail response
- Rationale: Budget tracking is for fairness, not correctness

### Blocking Errors
- `Reserve()` when budget exhausted: Return error, request may be retried
- Constructor errors: Fail plugin initialization

---

## Monitoring & Observability

### Log Levels

**Debug**:
- Successful reservations
- Successful deductions
- Score calculations

**Info**:
- Budget refreshes
- Plugin initialization

**Warn**:
- Missing program IDs
- Missing pod IDs
- Stale reservation cleanup

**Error**:
- Budget exhaustion
- Failed reservations
- Failed deductions

### Metrics (Future)
- Budget utilization per program
- Reservation success/failure rate
- Average budget per pod
- Stale reservation count

---

## Testing Strategy

### Unit Tests (Completed)
- ✅ Budget tracker core functions
- ✅ Concurrent access
- ✅ Edge cases (exhaustion, cleanup)
- ✅ Refresh operations

### Integration Tests (TODO)
- End-to-end request flow
- Multi-program scenarios
- Budget refresh during load
- Pod failure handling

### Load Tests (TODO)
- High concurrency (1000+ req/s)
- Budget exhaustion under load
- Cleanup performance

---

## Future Enhancements

### 1. Token-Based Budgets
- Track tokens instead of requests
- More accurate resource accounting
- Requires token count from responses

### 2. Dynamic Budget Adjustment
- Adjust budgets based on pod capacity
- Scale budgets with pod resources
- Auto-tune based on load

### 3. Custom Picker
- Atomic selection + reservation
- Eliminate race condition
- More complex implementation

### 4. Budget Borrowing
- Allow temporary overdraft
- Repay from future budget
- More flexible fairness

### 5. Priority Levels
- Different budgets for different priorities
- High-priority programs get more budget
- Weighted fairness

---

## Summary

This architecture implements a **request-based budget system** with:
- ✅ **3 core files**: budget_tracker.go, plugin.go, budget_tracker_test.go
- ✅ **Complete request lifecycle**: Score → PreRequest → ResponseBody
- ✅ **Thread-safe operations**: RWMutex for concurrent access
- ✅ **Comprehensive tests**: All passing
- 🔄 **1 pending file**: budget_refresher.go (background service)

**Total Lines of Code**: ~1,200 lines (excluding tests and docs)

**Ready for**: Integration testing and deployment