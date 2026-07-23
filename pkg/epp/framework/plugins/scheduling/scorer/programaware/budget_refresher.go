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

	// Cached queue size data (updated by plugin's Score method)
	queueSizeCache map[string]int // podID -> waiting queue size
	cacheMu        sync.RWMutex

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
		queueSizeCache:       make(map[string]int),
		stopChan:             make(chan struct{}),
	}
}

// UpdateQueueSize updates the cached waiting queue size for a pod
func (r *BudgetRefresher) UpdateQueueSize(podID string, queueSize int) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	r.queueSizeCache[podID] = queueSize
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

	// Get cached queue size data (updated by plugin's Score method)
	r.cacheMu.RLock()
	queueSizeSnapshot := make(map[string]int, len(r.queueSizeCache))
	for podID, qSize := range r.queueSizeCache {
		queueSizeSnapshot[podID] = qSize
	}
	r.cacheMu.RUnlock()

	if len(queueSizeSnapshot) == 0 {
		logger.Info("[BudgetRefresher] No queue size data cached, skipping refresh")
		return
	}

	// Calculate and apply refresh amounts per pod based on replica queue size
	var lowQueueCount, mediumQueueCount, highQueueCount int
	refreshedPods := 0

	for podID, queueSize := range queueSizeSnapshot {
		// Determine refresh multiplier based on replica waiting queue size
		var multiplier float64
		if queueSize <= 1 {
			multiplier = r.lowUtilMultiplier
			lowQueueCount++
		} else if queueSize <= 3 {
			multiplier = r.mediumUtilMultiplier
			mediumQueueCount++
		} else {
			multiplier = r.highUtilMultiplier
			highQueueCount++
		}

		refreshAmount := int64(float64(r.refreshAmount) * multiplier)

		// Apply refresh to this specific pod (method now has built-in max budget cap)
		r.budgetTracker.RefreshPodBudgets(podID, refreshAmount)
		refreshedPods++

		logger.V(2).Info("[BudgetRefresher] Pod refresh applied based on queue size",
			"pod", podID,
			"queueSize", queueSize,
			"multiplier", multiplier,
			"refreshAmount", refreshAmount,
			"maxBudget", r.budgetTracker.defaultBudget)
	}

	duration := time.Since(startTime)
	logger.Info("[BudgetRefresher] Refresh completed based on queue size",
		"duration", duration,
		"refreshedPods", refreshedPods,
		"numPods", len(queueSizeSnapshot),
		"lowQueuePods", lowQueueCount,
		"mediumQueuePods", mediumQueueCount,
		"highQueuePods", highQueueCount)
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