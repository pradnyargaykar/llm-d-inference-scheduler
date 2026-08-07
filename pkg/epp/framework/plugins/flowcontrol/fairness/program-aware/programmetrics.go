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

package programaware

import (
	"sync"
	"sync/atomic"
	"time"
)

const ewmaAlpha = 0.5

// ProgramMetrics holds aggregated metrics for a single program (identified by its fairness ID).
// All methods are goroutine-safe.
type ProgramMetrics struct {
	mu                 sync.Mutex
	averageWaitTime    float64 // cumulative mean of wait time in milliseconds
	waitCount          int64   // number of wait time observations
	averageTokens      float64 // EWMA of per-request token usage (input+output)
	serviceRate        float64 // EWMA of weighted tokens per second, updated on each completion.
	lastCompletionTime time.Time

	totalRequests   atomic.Int64
	dispatchedCount atomic.Int64
	inFlight        atomic.Int64
}

func (m *ProgramMetrics) IncrementRequests() {
	m.totalRequests.Add(1)
}

func (m *ProgramMetrics) IncrementDispatched() {
	m.dispatchedCount.Add(1)
}

func (m *ProgramMetrics) IncrementInFlight() {
	m.inFlight.Add(1)
}

func (m *ProgramMetrics) DecrementInFlight() {
	m.inFlight.Add(-1)
}

func (m *ProgramMetrics) InFlight() int64 {
	return m.inFlight.Load()
}

func (m *ProgramMetrics) RecordWaitTime(waitMs float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitCount++
	m.averageWaitTime += (waitMs - m.averageWaitTime) / float64(m.waitCount)
}

func (m *ProgramMetrics) RecordDispatched(enqueueTime time.Time) {
	m.inFlight.Add(1)
	m.dispatchedCount.Add(1)
	if enqueueTime.IsZero() {
		return
	}
	waitMs := float64(time.Since(enqueueTime).Milliseconds())
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitCount++
	m.averageWaitTime += (waitMs - m.averageWaitTime) / float64(m.waitCount)
}

func (m *ProgramMetrics) RecordCompletion(now time.Time) {
	m.inFlight.Add(-1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastCompletionTime = now
}

func (m *ProgramMetrics) AverageWaitTime() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.averageWaitTime
}

func (m *ProgramMetrics) WaitCount() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.waitCount
}

func (m *ProgramMetrics) RecordTokens(input, output int64) {
	cost := weightInputToken*float64(input) + weightOutputToken*float64(output)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.averageTokens == 0 {
		m.averageTokens = cost
	} else {
		m.averageTokens = ewmaAlpha*cost + (1-ewmaAlpha)*m.averageTokens
	}
}

func (m *ProgramMetrics) AverageTokens() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.averageTokens
}

func (m *ProgramMetrics) RecordServiceRate(weightedTokens float64, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastCompletionTime.IsZero() {
		m.lastCompletionTime = now
		return
	}
	elapsed := now.Sub(m.lastCompletionTime).Seconds()
	if elapsed <= 0 {
		return
	}
	instantRate := weightedTokens / elapsed
	if m.serviceRate == 0 {
		m.serviceRate = instantRate
	} else {
		m.serviceRate = ewmaAlpha*instantRate + (1-ewmaAlpha)*m.serviceRate
	}
	m.lastCompletionTime = now
}

func (m *ProgramMetrics) ServiceRate() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.serviceRate
}

func (m *ProgramMetrics) LastCompletionTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastCompletionTime
}

func (m *ProgramMetrics) TotalRequests() int64 {
	return m.totalRequests.Load()
}

func (m *ProgramMetrics) DispatchedCount() int64 {
	return m.dispatchedCount.Load()
}
