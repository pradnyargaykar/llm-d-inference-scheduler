package programaware

import (
	"context"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// BudgetRefresher handles periodic budget refreshment based on pod utilization
type BudgetRefresher struct {
	budgetTracker   *BudgetTracker
	refreshInterval time.Duration
	refreshAmount   int64 // Base amount to add per refresh (before multipliers)

	// Utilization thresholds and multipliers
	lowUtilThreshold     float64
	mediumUtilThreshold  float64
	lowUtilMultiplier    float64
	mediumUtilMultiplier float64
	highUtilMultiplier   float64

	// Cached utilization data (updated by plugin's Score method)
	utilizationCache map[string]float64 // podID -> utilization (0.0 to 1.0)
	cacheMu          sync.RWMutex

	// Control
	stopChan chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
	running  bool
}

// RefresherConfig holds configuration for the budget refresher
type RefresherConfig struct {
	RefreshInterval      time.Duration
	RefreshAmount        int64 // Base amount to add per refresh cycle
	LowUtilThreshold     float64
	MediumUtilThreshold  float64
	LowUtilMultiplier    float64
	MediumUtilMultiplier float64
	HighUtilMultiplier   float64
}

// NewBudgetRefresher creates a new budget refresher
func NewBudgetRefresher(tracker *BudgetTracker, config RefresherConfig) *BudgetRefresher {
	return &BudgetRefresher{
		budgetTracker:        tracker,
		refreshInterval:      config.RefreshInterval,
		refreshAmount:        config.RefreshAmount,
		lowUtilThreshold:     config.LowUtilThreshold,
		mediumUtilThreshold:  config.MediumUtilThreshold,
		lowUtilMultiplier:    config.LowUtilMultiplier,
		mediumUtilMultiplier: config.MediumUtilMultiplier,
		highUtilMultiplier:   config.HighUtilMultiplier,
		utilizationCache:     make(map[string]float64),
		stopChan:             make(chan struct{}),
	}
}

// UpdateUtilization updates the cached utilization for a pod
// This should be called by the plugin's Score method
func (r *BudgetRefresher) UpdateUtilization(podID string, utilization float64) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	r.utilizationCache[podID] = utilization
}

// Start begins the periodic budget refresh loop
func (r *BudgetRefresher) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return nil
	}
	r.running = true
	r.mu.Unlock()

	logger := log.FromContext(ctx)
	logger.Info("[BudgetRefresher] Starting",
		"interval", r.refreshInterval,
		"refreshAmount", r.refreshAmount,
		"maxBudget", r.budgetTracker.defaultBudget)

	r.wg.Add(1)
	go r.refreshLoop(ctx)

	return nil
}

// Stop stops the budget refresher
func (r *BudgetRefresher) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	r.mu.Unlock()

	close(r.stopChan)
	r.wg.Wait()
}

// refreshLoop is the main refresh loop
func (r *BudgetRefresher) refreshLoop(ctx context.Context) {
	defer r.wg.Done()

	ticker := time.NewTicker(r.refreshInterval)
	defer ticker.Stop()

	logger := log.FromContext(ctx)

	for {
		select {
		case <-ticker.C:
			r.performRefresh(ctx)
		case <-r.stopChan:
			logger.Info("[BudgetRefresher] Stopping refresh loop")
			return
		case <-ctx.Done():
			logger.Info("[BudgetRefresher] Context cancelled, stopping")
			return
		}
	}
}

// performRefresh executes a single refresh cycle
func (r *BudgetRefresher) performRefresh(ctx context.Context) {
	logger := log.FromContext(ctx)
	startTime := time.Now()

	// Get cached utilization data (updated by plugin's Score method)
	r.cacheMu.RLock()
	utilizationSnapshot := make(map[string]float64, len(r.utilizationCache))
	for podID, util := range r.utilizationCache {
		utilizationSnapshot[podID] = util
	}
	r.cacheMu.RUnlock()

	if len(utilizationSnapshot) == 0 {
		logger.Info("[BudgetRefresher] No utilization data cached, skipping refresh")
		return
	}

	// Calculate and apply refresh amounts per pod based on utilization
	var lowUtilCount, mediumUtilCount, highUtilCount int
	refreshedPods := 0

	for podID, utilization := range utilizationSnapshot {
		// Determine refresh multiplier based on utilization
		var multiplier float64
		if utilization < r.lowUtilThreshold {
			multiplier = r.lowUtilMultiplier
			lowUtilCount++
		} else if utilization < r.mediumUtilThreshold {
			multiplier = r.mediumUtilMultiplier
			mediumUtilCount++
		} else {
			multiplier = r.highUtilMultiplier
			highUtilCount++
		}

		refreshAmount := int64(float64(r.refreshAmount) * multiplier)

		// Apply refresh to this specific pod (method now has built-in max budget cap)
		r.budgetTracker.RefreshPodBudgets(podID, refreshAmount)
		refreshedPods++

		logger.V(2).Info("[BudgetRefresher] Pod refresh applied",
			"pod", podID,
			"utilization", utilization,
			"multiplier", multiplier,
			"refreshAmount", refreshAmount,
			"maxBudget", r.budgetTracker.defaultBudget)
	}

	duration := time.Since(startTime)
	logger.Info("[BudgetRefresher] Refresh completed",
		"duration", duration,
		"refreshedPods", refreshedPods,
		"numPods", len(utilizationSnapshot),
		"lowUtilPods", lowUtilCount,
		"mediumUtilPods", mediumUtilCount,
		"highUtilPods", highUtilCount)
}

// GetStatus returns the current status of the refresher
func (r *BudgetRefresher) GetStatus() map[string]interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()

	return map[string]interface{}{
		"running":                r.running,
		"refresh_interval":       r.refreshInterval.String(),
		"refresh_amount":         r.refreshAmount,
		"max_budget":             r.budgetTracker.defaultBudget,
		"low_util_threshold":     r.lowUtilThreshold,
		"medium_util_threshold":  r.mediumUtilThreshold,
		"low_util_multiplier":    r.lowUtilMultiplier,
		"medium_util_multiplier": r.mediumUtilMultiplier,
		"high_util_multiplier":   r.highUtilMultiplier,
	}
}

// Made with Bob