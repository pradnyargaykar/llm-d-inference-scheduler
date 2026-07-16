/*
Copyright 2025 The Kubernetes Authors.

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

package programaware

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewBudgetTracker tests the initialization of BudgetTracker
func TestNewBudgetTracker(t *testing.T) {
	tests := []struct {
		name           string
		defaultBudget  int64
		expectedBudget int64
	}{
		{
			name:           "valid budget",
			defaultBudget:  1000,
			expectedBudget: 1000,
		},
		{
			name:           "zero budget defaults to 1000",
			defaultBudget:  0,
			expectedBudget: 1000,
		},
		{
			name:           "negative budget defaults to 1000",
			defaultBudget:  -100,
			expectedBudget: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewBudgetTracker(tt.defaultBudget)
			defer tracker.Stop()

			assert.NotNil(t, tracker)
			assert.Equal(t, tt.expectedBudget, tracker.defaultBudget)
			assert.NotNil(t, tracker.budgets)
			assert.NotNil(t, tracker.inFlightRequests)
			assert.Equal(t, 0, len(tracker.budgets))
			assert.Equal(t, 0, len(tracker.inFlightRequests))
		})
	}
}

// TestReserveAutoCreation tests that budgets are auto-created on first use
func TestReserveAutoCreation(t *testing.T) {
	tracker := NewBudgetTracker(1000)
	defer tracker.Stop()

	// Verify no budget exists initially
	total, used, reserved, available, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.False(t, exists, "Budget should not exist before first request")

	// First request - should auto-create budget
	err := tracker.Reserve("req-001", "program-a", "pod-1")
	assert.NoError(t, err)

	// Verify budget was auto-created with default value
	total, used, reserved, available, exists = tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists, "Budget should exist after first request")
	assert.Equal(t, int64(1000), total, "Total should be default budget")
	assert.Equal(t, int64(0), used, "Used should be 0")
	assert.Equal(t, int64(1), reserved, "Reserved should be 1")
	assert.Equal(t, int64(999), available, "Available should be 999")
}

// TestFirstRequestDetection tests detecting first-time requests
func TestFirstRequestDetection(t *testing.T) {
	tracker := NewBudgetTracker(1000)
	defer tracker.Stop()

	// Check if program-a has any budget (first request detection)
	_, _, _, _, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.False(t, exists, "No budget = first request")

	// Reserve first request
	err := tracker.Reserve("req-001", "program-a", "pod-1")
	require.NoError(t, err)

	// Check again - now budget exists (not first request)
	_, used, reserved, _, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists, "Budget exists = not first request")
	assert.True(t, used > 0 || reserved > 0, "Has activity = not first request")
}

// TestReserve tests the Reserve method
func TestReserve(t *testing.T) {
	tracker := NewBudgetTracker(100)
	defer tracker.Stop()

	// Test successful reservation
	err := tracker.Reserve("req-001", "program-a", "pod-1")
	assert.NoError(t, err)

	// Verify in-flight tracking
	assert.Equal(t, 1, tracker.GetInFlightCount())

	// Test multiple reservations
	err = tracker.Reserve("req-002", "program-a", "pod-1")
	assert.NoError(t, err)
	err = tracker.Reserve("req-003", "program-a", "pod-1")
	assert.NoError(t, err)

	_, _, reserved, available, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists)
	assert.Equal(t, int64(3), reserved)
	assert.Equal(t, int64(97), available)
	assert.Equal(t, 3, tracker.GetInFlightCount())
}

// TestReserveInsufficientBudget tests reservation failure when budget is exhausted
func TestReserveInsufficientBudget(t *testing.T) {
	tracker := NewBudgetTracker(2)
	defer tracker.Stop()

	// Reserve all available budget
	err := tracker.Reserve("req-001", "program-a", "pod-1")
	assert.NoError(t, err)
	err = tracker.Reserve("req-002", "program-a", "pod-1")
	assert.NoError(t, err)

	// Try to reserve when budget is exhausted
	err = tracker.Reserve("req-003", "program-a", "pod-1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient budget")

	// Verify only 2 requests are in-flight
	assert.Equal(t, 2, tracker.GetInFlightCount())
}

// TestGetAvailable tests the GetAvailable method
func TestGetAvailable(t *testing.T) {
	tracker := NewBudgetTracker(100)
	defer tracker.Stop()

	// Test before any budget is created (should return default)
	available := tracker.GetAvailable("program-a", "pod-1")
	assert.Equal(t, int64(100), available)

	// Reserve some budget
	err := tracker.Reserve("req-001", "program-a", "pod-1")
	require.NoError(t, err)

	// Test after reservation
	available = tracker.GetAvailable("program-a", "pod-1")
	assert.Equal(t, int64(99), available)

	// Test different program (should return default)
	available = tracker.GetAvailable("program-b", "pod-1")
	assert.Equal(t, int64(100), available)

	// Test different pod (should return default)
	available = tracker.GetAvailable("program-a", "pod-2")
	assert.Equal(t, int64(100), available)
}

// TestReleaseAndDeduct tests the ReleaseAndDeduct method
func TestReleaseAndDeduct(t *testing.T) {
	tracker := NewBudgetTracker(100)
	defer tracker.Stop()

	// Reserve budget
	err := tracker.Reserve("req-001", "program-a", "pod-1")
	require.NoError(t, err)

	// Verify reservation
	_, used, reserved, available, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists)
	assert.Equal(t, int64(0), used)
	assert.Equal(t, int64(1), reserved)
	assert.Equal(t, int64(99), available)

	// Release and deduct (success = true)
	err = tracker.ReleaseAndDeduct("req-001", true)
	assert.NoError(t, err)

	// Verify budget updated
	_, used2, reserved2, available2, exists2 := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists2)
	assert.Equal(t, int64(1), used2)
	assert.Equal(t, int64(0), reserved2)
	assert.Equal(t, int64(99), available2) // Available stays same

	// Reserve another and release with success = false
	err = tracker.Reserve("req-002", "program-a", "pod-1")
	require.NoError(t, err)

	err = tracker.ReleaseAndDeduct("req-002", false)
	assert.NoError(t, err)

	// Verify budget updated (used requests stays 1, available goes back to 99)
	_, used3, reserved3, available3, exists3 := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists3)
	assert.Equal(t, int64(1), used3)
	assert.Equal(t, int64(0), reserved3)
	assert.Equal(t, int64(99), available3)

	// Verify in-flight tracking cleared
	assert.Equal(t, 0, tracker.GetInFlightCount())
}

// TestMultipleProgramsAndPods tests budget tracking across multiple programs and pods
func TestMultipleProgramsAndPods(t *testing.T) {
	tracker := NewBudgetTracker(100)
	defer tracker.Stop()

	// Reserve for different program-pod combinations
	err := tracker.Reserve("req-001", "program-a", "pod-1")
	require.NoError(t, err)
	err = tracker.Reserve("req-002", "program-a", "pod-2")
	require.NoError(t, err)
	err = tracker.Reserve("req-003", "program-b", "pod-1")
	require.NoError(t, err)
	err = tracker.Reserve("req-004", "program-b", "pod-2")
	require.NoError(t, err)

	// Verify each program-pod pair has independent budget
	available := tracker.GetAvailable("program-a", "pod-1")
	assert.Equal(t, int64(99), available)
	available = tracker.GetAvailable("program-a", "pod-2")
	assert.Equal(t, int64(99), available)
	available = tracker.GetAvailable("program-b", "pod-1")
	assert.Equal(t, int64(99), available)
	available = tracker.GetAvailable("program-b", "pod-2")
	assert.Equal(t, int64(99), available)

	// Verify program count
	assert.Equal(t, 2, tracker.GetProgramCount())
}

// TestConcurrentReserve tests concurrent Reserve calls for race conditions
func TestConcurrentReserve(t *testing.T) {
	tracker := NewBudgetTracker(1000)
	defer tracker.Stop()

	numGoroutines := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch concurrent Reserve calls
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			requestID := "req-" + string(rune(id))
			err := tracker.Reserve(requestID, "program-a", "pod-1")
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify all reservations succeeded
	total, used, reserved, available, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists)
	assert.Equal(t, int64(1000), total)
	assert.Equal(t, int64(0), used)
	assert.Equal(t, int64(numGoroutines), reserved)
	assert.Equal(t, int64(1000-numGoroutines), available)
	assert.Equal(t, numGoroutines, tracker.GetInFlightCount())
}

// TestConcurrentReserveAndRelease tests concurrent Reserve and ReleaseAndDeduct calls
func TestConcurrentReserveAndRelease(t *testing.T) {
	tracker := NewBudgetTracker(1000)
	defer tracker.Stop()

	numOperations := 50
	var wg sync.WaitGroup
	wg.Add(numOperations * 2)

	// Launch concurrent Reserve calls
	for i := 0; i < numOperations; i++ {
		go func(id int) {
			defer wg.Done()
			requestID := "req-" + string(rune(id))
			err := tracker.Reserve(requestID, "program-a", "pod-1")
			assert.NoError(t, err)
		}(i)
	}

	// Give reserves time to complete
	time.Sleep(100 * time.Millisecond)

	// Launch concurrent ReleaseAndDeduct calls
	for i := 0; i < numOperations; i++ {
		go func(id int) {
			defer wg.Done()
			requestID := "req-" + string(rune(id))
			err := tracker.ReleaseAndDeduct(requestID, true)
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Verify final state
	total, used, reserved, available, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists)
	assert.Equal(t, int64(1000), total)
	assert.Equal(t, int64(numOperations), used)
	assert.Equal(t, int64(0), reserved)
	assert.Equal(t, int64(1000-numOperations), available)
	assert.Equal(t, 0, tracker.GetInFlightCount())
}

// TestCleanupStaleReservations tests the cleanup of stale reservations
func TestCleanupStaleReservations(t *testing.T) {
	tracker := NewBudgetTracker(100)
	tracker.reservationTimeout = 100 * time.Millisecond
	defer tracker.Stop()

	// Reserve budget
	err := tracker.Reserve("req-001", "program-a", "pod-1")
	require.NoError(t, err)

	// Verify reservation exists
	assert.Equal(t, 1, tracker.GetInFlightCount())
	_, _, reserved, _, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists)
	assert.Equal(t, int64(1), reserved)

	// Wait for reservation to become stale
	time.Sleep(200 * time.Millisecond)

	// Manually trigger cleanup
	tracker.cleanupStaleReservations()

	// Verify stale reservation was cleaned up
	assert.Equal(t, 0, tracker.GetInFlightCount())
	_, used2, reserved2, _, exists2 := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists2)
	assert.Equal(t, int64(0), reserved2)
	assert.Equal(t, int64(0), used2)
}

// TestCompleteRequestLifecycle tests the complete lifecycle of a request
func TestCompleteRequestLifecycle(t *testing.T) {
	tracker := NewBudgetTracker(100)
	defer tracker.Stop()

	// Initial state - no budget exists (first request)
	_, _, _, _, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.False(t, exists, "No budget = first request")

	available := tracker.GetAvailable("program-a", "pod-1")
	assert.Equal(t, int64(100), available)

	// Reserve
	err := tracker.Reserve("req-001", "program-a", "pod-1")
	require.NoError(t, err)

	// Check state after reserve
	total, used, reserved, available, exists := tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists, "Budget exists = not first request anymore")
	assert.Equal(t, int64(100), total)
	assert.Equal(t, int64(0), used)
	assert.Equal(t, int64(1), reserved)
	assert.Equal(t, int64(99), available)
	assert.Equal(t, 1, tracker.GetInFlightCount())

	// Release and deduct
	err = tracker.ReleaseAndDeduct("req-001", true)
	require.NoError(t, err)

	// Check final state
	total, used, reserved, available, exists = tracker.GetBudgetStats("program-a", "pod-1")
	assert.True(t, exists)
	assert.Equal(t, int64(100), total)
	assert.Equal(t, int64(1), used)
	assert.Equal(t, int64(0), reserved)
	assert.Equal(t, int64(99), available)
	assert.Equal(t, 0, tracker.GetInFlightCount())
}

// Made with Bob
