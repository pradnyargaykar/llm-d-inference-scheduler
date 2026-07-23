package programaware

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Budget represents the budget allocation for a program on a specific pod
type Budget struct {
	TotalRequests    int64     // Total allowed requests (e.g., 1000)
	UsedRequests     int64     // Completed requests
	ReservedRequests int64     // In-flight requests (reserved but not yet completed)
	LastReset        time.Time // Last time budget was reset
	LastRefresh      time.Time // Last time budget was refreshed
}

// InFlightRequest tracks a request that has been reserved but not yet completed
type InFlightRequest struct {
	RequestID string    // Unique request identifier
	ProgramID string    // Program/FairnessID
	PodName   string    // Target pod name
	StartTime time.Time // When the reservation was made
}

// BudgetTracker manages request budgets for programs across pods
// Uses a uniform budget model: all programs get the same budget per pod
type BudgetTracker struct {
	// budgets maps programID -> podName -> Budget
	budgets map[string]map[string]*Budget

	// defaultBudget is the uniform budget value applied to all program-pod pairs
	defaultBudget int64

	// inFlightRequests tracks requests that have been reserved but not completed
	inFlightRequests map[string]*InFlightRequest

	// firstPod maps programID -> podName of the first routed request
	firstPod map[string]string

	// mu protects budgets, inFlightRequests, and firstPod for concurrent access
	mu sync.RWMutex

	// Cleanup configuration
	cleanupInterval    time.Duration // How often to run cleanup (default: 1 minute)
	reservationTimeout time.Duration // How long before a reservation is considered stale (default: 5 minutes)
	stopCleanup        chan struct{} // Signal to stop cleanup goroutine
	cleanupRunning     bool
	cleanupMu          sync.Mutex // Separate mutex for cleanup state
}

// NewBudgetTracker creates a new BudgetTracker with uniform budget allocation
// All programs will get the same defaultBudget per pod
func NewBudgetTracker(defaultBudget int64) *BudgetTracker {
	if defaultBudget <= 0 {
		log.Printf("Warning: defaultBudget=%d is invalid, using default of 1000", defaultBudget)
		defaultBudget = 1000
	}

	bt := &BudgetTracker{
		budgets:            make(map[string]map[string]*Budget),
		defaultBudget:      defaultBudget,
		inFlightRequests:   make(map[string]*InFlightRequest),
		firstPod:           make(map[string]string),
		cleanupInterval:    1 * time.Minute,
		reservationTimeout: 5 * time.Minute,
		stopCleanup:        make(chan struct{}),
	}

	log.Printf("BudgetTracker initialized with uniform budget: %d requests per program per pod", defaultBudget)

	// Start cleanup goroutine
	go bt.cleanupLoop()

	return bt
}

// Reserve tracks an in-flight request for budget accounting purposes.
// Budget exhaustion is handled by Score() assigning zero scores; Reserve() must
// always succeed so that ReleaseAndDeduct() can clean up state correctly.
// Budgets are auto-created on first use with the default budget value.
func (bt *BudgetTracker) Reserve(requestID, programID, podName string) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Auto-create program map if it doesn't exist
	if bt.budgets[programID] == nil {
		bt.budgets[programID] = make(map[string]*Budget)
		log.Printf("Auto-created budget map for program: %s", programID)
	}

	// Auto-create pod budget if it doesn't exist (using default budget)
	if bt.budgets[programID][podName] == nil {
		bt.budgets[programID][podName] = &Budget{
			TotalRequests:    bt.defaultBudget,
			UsedRequests:     0,
			ReservedRequests: 0,
			LastReset:        time.Now(),
		}
		log.Printf("Auto-created budget for program=%s pod=%s with default=%d requests",
			programID, podName, bt.defaultBudget)
	}

	// Record the first pod selected for the program if not already set
	if bt.firstPod[programID] == "" {
		bt.firstPod[programID] = podName
		log.Printf("Recorded first pod for program=%s: %s", programID, podName)
	}

	budget := bt.budgets[programID][podName]

	// Reserve the budget slot. We always track the request even when available
	// budget is zero: Score() already gave this endpoint a zero score so it will
	// be migrated on the next request. Without tracking here, ReleaseAndDeduct()
	// would error and the reservation counter would never be decremented, causing
	// permanent budget exhaustion until the next cleanup cycle.
	budget.ReservedRequests++

	// Track the in-flight request
	bt.inFlightRequests[requestID] = &InFlightRequest{
		RequestID: requestID,
		ProgramID: programID,
		PodName:   podName,
		StartTime: time.Now(),
	}

	available := budget.TotalRequests - budget.UsedRequests - budget.ReservedRequests
	log.Printf("Reserved budget: requestID=%s program=%s pod=%s (total=%d used=%d reserved=%d available=%d)",
		requestID, programID, podName, budget.TotalRequests, budget.UsedRequests,
		budget.ReservedRequests, available)

	return nil
}

// GetAvailable returns the available budget for a program on a pod
// Returns defaultBudget if the program-pod pair doesn't exist yet (will be auto-created on Reserve)
func (bt *BudgetTracker) GetAvailable(programID, podName string) int64 {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	// Check if program exists
	if bt.budgets[programID] == nil {
		// Budget will be auto-created on first Reserve call
		return bt.defaultBudget
	}

	// Check if pod exists for this program
	budget := bt.budgets[programID][podName]
	if budget == nil {
		// Budget will be auto-created on first Reserve call
		return bt.defaultBudget
	}

	// Calculate available budget
	available := budget.TotalRequests - budget.UsedRequests - budget.ReservedRequests

	if available < 0 {
		log.Printf("Warning: negative available budget for program=%s pod=%s (total=%d used=%d reserved=%d)",
			programID, podName, budget.TotalRequests, budget.UsedRequests, budget.ReservedRequests)
		return 0
	}

	return available
}

// ReleaseAndDeduct releases a reservation and deducts the actual usage
// This should be called when a request completes (successfully or with error)
func (bt *BudgetTracker) ReleaseAndDeduct(requestID string, success bool) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Find the in-flight request
	inFlight := bt.inFlightRequests[requestID]
	if inFlight == nil {
		return fmt.Errorf("request not found in in-flight tracking: %s", requestID)
	}

	programID := inFlight.ProgramID
	podName := inFlight.PodName

	// Verify budget exists
	if bt.budgets[programID] == nil || bt.budgets[programID][podName] == nil {
		return fmt.Errorf("budget not found for program=%s pod=%s", programID, podName)
	}

	budget := bt.budgets[programID][podName]

	// Release the reservation
	if budget.ReservedRequests > 0 {
		budget.ReservedRequests--
	} else {
		log.Printf("Warning: attempted to release reservation but ReservedRequests=0 for program=%s pod=%s",
			programID, podName)
	}

	// Deduct actual usage only if request succeeded
	if success {
		budget.UsedRequests++
	}

	// Remove from in-flight tracking
	delete(bt.inFlightRequests, requestID)

	duration := time.Since(inFlight.StartTime)
	log.Printf("Released and deducted budget: requestID=%s program=%s pod=%s success=%v duration=%v (total=%d used=%d reserved=%d)",
		requestID, programID, podName, success, duration, budget.TotalRequests, budget.UsedRequests, budget.ReservedRequests)

	return nil
}

// GetBudgetStats returns current budget statistics for a program on a pod
func (bt *BudgetTracker) GetBudgetStats(programID, podName string) (total, used, reserved, available int64, exists bool) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	if bt.budgets[programID] == nil || bt.budgets[programID][podName] == nil {
		return 0, 0, 0, 0, false
	}

	budget := bt.budgets[programID][podName]
	available = budget.TotalRequests - budget.UsedRequests - budget.ReservedRequests

	return budget.TotalRequests, budget.UsedRequests, budget.ReservedRequests, available, true
}

// ResetBudget resets the used and reserved counters for a program on a pod
// This can be used for periodic budget resets (e.g., daily, hourly)
func (bt *BudgetTracker) ResetBudget(programID, podName string) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.budgets[programID] == nil || bt.budgets[programID][podName] == nil {
		return fmt.Errorf("budget not found for program=%s pod=%s", programID, podName)
	}

	budget := bt.budgets[programID][podName]
	oldUsed := budget.UsedRequests
	oldReserved := budget.ReservedRequests

	budget.UsedRequests = 0
	budget.ReservedRequests = 0
	budget.LastReset = time.Now()
	delete(bt.firstPod, programID)

	log.Printf("Reset budget for program=%s pod=%s (was: used=%d reserved=%d, now: used=0 reserved=0)",
		programID, podName, oldUsed, oldReserved)

	return nil
}

// ResetAllBudgets resets all budgets (useful for periodic resets)
func (bt *BudgetTracker) ResetAllBudgets() {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	bt.firstPod = make(map[string]string)

	count := 0
	for programID, pods := range bt.budgets {
		for podName, budget := range pods {
			budget.UsedRequests = 0
			budget.ReservedRequests = 0
			budget.LastReset = time.Now()
			count++
			log.Printf("Reset budget for program=%s pod=%s", programID, podName)
		}
	}

	log.Printf("Reset all budgets: %d program-pod pairs reset", count)
}

// cleanupLoop runs periodically to clean up stale reservations
func (bt *BudgetTracker) cleanupLoop() {
	bt.cleanupMu.Lock()
	bt.cleanupRunning = true
	bt.cleanupMu.Unlock()

	ticker := time.NewTicker(bt.cleanupInterval)
	defer ticker.Stop()

	log.Printf("Budget cleanup loop started (interval=%v, timeout=%v)",
		bt.cleanupInterval, bt.reservationTimeout)

	for {
		select {
		case <-ticker.C:
			bt.cleanupStaleReservations()
		case <-bt.stopCleanup:
			log.Printf("Budget cleanup loop stopped")
			bt.cleanupMu.Lock()
			bt.cleanupRunning = false
			bt.cleanupMu.Unlock()
			return
		}
	}
}

// cleanupStaleReservations removes reservations that have been in-flight too long
// This prevents budget leaks from failed/timed-out requests
func (bt *BudgetTracker) cleanupStaleReservations() {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	now := time.Now()
	staleCount := 0

	for requestID, inFlight := range bt.inFlightRequests {
		age := now.Sub(inFlight.StartTime)

		if age > bt.reservationTimeout {
			programID := inFlight.ProgramID
			podName := inFlight.PodName

			// Release the stale reservation
			if bt.budgets[programID] != nil && bt.budgets[programID][podName] != nil {
				budget := bt.budgets[programID][podName]
				if budget.ReservedRequests > 0 {
					budget.ReservedRequests--
				}

				log.Printf("Cleaned up stale reservation: requestID=%s program=%s pod=%s age=%v (total=%d used=%d reserved=%d)",
					requestID, programID, podName, age, budget.TotalRequests, budget.UsedRequests, budget.ReservedRequests)
			}

			// Remove from in-flight tracking
			delete(bt.inFlightRequests, requestID)
			staleCount++
		}
	}

	if staleCount > 0 {
		log.Printf("Cleaned up %d stale reservations (timeout=%v)", staleCount, bt.reservationTimeout)
	}
}

// Stop stops the cleanup goroutine
func (bt *BudgetTracker) Stop() {
	bt.cleanupMu.Lock()
	if bt.cleanupRunning {
		close(bt.stopCleanup)
	}
	bt.cleanupMu.Unlock()

	log.Printf("BudgetTracker stopped")
}

// GetInFlightCount returns the number of in-flight requests
func (bt *BudgetTracker) GetInFlightCount() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return len(bt.inFlightRequests)
}

// GetProgramCount returns the number of programs being tracked
func (bt *BudgetTracker) GetProgramCount() int {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	return len(bt.budgets)
}

// Made with Bob

// IsFirstTimeProgram checks if a program has never been used (optimization for routing)
// Returns true if the program has no used requests across all pods
// This allows the scorer to skip expensive cache queries for first-time requests
func (bt *BudgetTracker) IsFirstTimeProgram(programID string) bool {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	podBudgets, exists := bt.budgets[programID]
	if !exists {
		return true // Never seen this program
	}

	// Check if any pod has used or reserved requests
	for _, budget := range podBudgets {
		if budget.UsedRequests > 0 || budget.ReservedRequests > 0 {
			return false // Program has been used or is in-flight
		}
	}

	return true // Program exists but never used or reserved
}

// GetFirstPod returns the name of the pod where the first request was routed
func (bt *BudgetTracker) GetFirstPod(programID string) (string, bool) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()
	podName, exists := bt.firstPod[programID]
	return podName, exists
}

// RefreshPodBudgets adds budget to all programs on a specific pod with max budget cap
// Used by the budget refresher for utilization-based refresh
// Example: Low utilization pod gets 2x refresh, high utilization gets 1x
// CRITICAL: Available budget NEVER exceeds bt.defaultBudget
func (bt *BudgetTracker) RefreshPodBudgets(podName string, refreshAmount int64) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	count := 0
	cappedCount := 0
	for programID, pods := range bt.budgets {
		if budget, exists := pods[podName]; exists {
			oldTotal := budget.TotalRequests
			available := oldTotal - budget.UsedRequests - budget.ReservedRequests
			newAvailable := available + refreshAmount

			// CRITICAL: Cap available budget at defaultBudget - available should NEVER exceed it
			var added int64
			if newAvailable > bt.defaultBudget {
				added = bt.defaultBudget - available
				budget.TotalRequests = bt.defaultBudget + budget.UsedRequests + budget.ReservedRequests
				cappedCount++
				log.Printf("Refreshed budget (capped) for program=%s pod=%s: available was %d, tried to add %d, capped available at max=%d",
					programID, podName, available, refreshAmount, bt.defaultBudget)
			} else {
				added = refreshAmount
				budget.TotalRequests = oldTotal + refreshAmount
				log.Printf("Refreshed budget for program=%s pod=%s: added %d requests (new available=%d, max=%d)",
					programID, podName, refreshAmount, newAvailable, bt.defaultBudget)
			}

			budget.LastRefresh = time.Now()
			count++

			// Emit per-program-pod refresh metrics
			budgetRefreshesTotal.WithLabelValues(programID, podName).Inc()
			if added > 0 {
				budgetRefreshAmount.WithLabelValues(programID, podName).Add(float64(added))
			}
		}
	}

	if count > 0 {
		log.Printf("Refreshed budgets for pod=%s: %d programs updated with +%d requests each (max=%d, %d capped)",
			podName, count, refreshAmount, bt.defaultBudget, cappedCount)
	}
}

// RefreshAllBudgets adds budget to all program-pod pairs with max budget cap
// Used for periodic refresh across the entire cluster
// CRITICAL: Available budget NEVER exceeds bt.defaultBudget
func (bt *BudgetTracker) RefreshAllBudgets(refreshAmount int64) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	count := 0
	cappedCount := 0
	for _, pods := range bt.budgets {
		for _, budget := range pods {
			oldTotal := budget.TotalRequests
			available := oldTotal - budget.UsedRequests - budget.ReservedRequests
			newAvailable := available + refreshAmount
			
			// CRITICAL: Cap available budget at defaultBudget - available should NEVER exceed it
			if newAvailable > bt.defaultBudget {
				budget.TotalRequests = bt.defaultBudget + budget.UsedRequests + budget.ReservedRequests
				cappedCount++
			} else {
				budget.TotalRequests = oldTotal + refreshAmount
			}
			
			budget.LastRefresh = time.Now()
			count++
		}
	}

	log.Printf("Refreshed all budgets: %d program-pod pairs updated with +%d requests each (max=%d, %d capped)",
		count, refreshAmount, bt.defaultBudget, cappedCount)
}

// EmergencyRefreshMultiplePods provides immediate budget boost for a program across multiple pods
// More efficient than calling single-pod refresh multiple times (single lock acquisition)
// Used when all budgets are exhausted and idle pods are available
func (bt *BudgetTracker) EmergencyRefreshMultiplePods(programID string, podNames []string, refreshAmount int64) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Create program map if doesn't exist
	if bt.budgets[programID] == nil {
		bt.budgets[programID] = make(map[string]*Budget)
	}

	count := 0
	for _, podName := range podNames {
		if bt.budgets[programID][podName] == nil {
			// Create new budget with emergency amount
			bt.budgets[programID][podName] = &Budget{
				TotalRequests:    refreshAmount,
				UsedRequests:     0,
				ReservedRequests: 0,
				LastReset:        time.Now(),
				LastRefresh:      time.Now(),
			}
			log.Printf("Emergency refresh: created budget for program=%s pod=%s with %d requests",
				programID, podName, refreshAmount)
		} else {
			// Add to existing budget
			budget := bt.budgets[programID][podName]
			budget.TotalRequests += refreshAmount
			budget.LastRefresh = time.Now()
			log.Printf("Emergency refresh: added %d requests to program=%s pod=%s (new total=%d)",
				refreshAmount, programID, podName, budget.TotalRequests)
		}
		count++
	}

	log.Printf("Emergency refresh completed: updated %d pods for program=%s with +%d requests each",
		count, programID, refreshAmount)

	return nil
}
