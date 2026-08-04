package service

import (
	"math/rand"
	"sync"
	"time"
)

// parseSlotRegistry tracks, per knowledge base, how many document-parse
// tasks are currently executing inside this app instance. It backs the
// per-KB "max concurrent parse" setting (ChunkingConfig.MaxConcurrentParse):
// ProcessDocument tries to acquire a slot before doing any heavy work and,
// when the KB is already at its cap, re-schedules the task with a short
// delay instead of blocking an asynq worker.
//
// Scope note: the counter is in-process. With a single app instance (the
// standard docker-compose deployment) it is exact; with N replicas the
// effective cap becomes cap×N per KB, which is still a sane throttle.
type parseSlotRegistry struct {
	mu     sync.Mutex
	counts map[string]int
}

var kbParseSlots = &parseSlotRegistry{counts: make(map[string]int)}

// tryAcquire reserves one parse slot for kbID when fewer than limit are in
// use. Returns true when the slot was acquired.
func (r *parseSlotRegistry) tryAcquire(kbID string, limit int) bool {
	if limit <= 0 {
		return true // no cap
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.counts[kbID] >= limit {
		return false
	}
	r.counts[kbID]++
	return true
}

// release returns a previously acquired slot for kbID.
func (r *parseSlotRegistry) release(kbID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := r.counts[kbID]; n <= 1 {
		delete(r.counts, kbID)
	} else {
		r.counts[kbID] = n - 1
	}
}

// inUse reports how many parse slots kbID currently holds.
func (r *parseSlotRegistry) inUse(kbID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[kbID]
}

// parseSlotRetryDelay returns a jittered, exponentially backed-off delay used
// when re-scheduling a parse task that exceeded its KB's concurrency cap.
// deferCount is how many times the task has already been deferred: the base
// delay doubles per defer (10s, 20s, 40s, 80s) and is capped at 2 minutes, so
// a 1000-file batch upload settles into ~2min polling instead of hammering
// Redis/logs with fixed 10-20s retries for hours. Jitter avoids all deferred
// tasks waking up (and colliding) at the same instant.
func parseSlotRetryDelay(deferCount int) time.Duration {
	base := 10 * time.Second
	for i := 0; i < deferCount && base < 2*time.Minute; i++ {
		base *= 2
	}
	if base > 2*time.Minute {
		base = 2 * time.Minute
	}
	return base + time.Duration(rand.Intn(10_000))*time.Millisecond
}
