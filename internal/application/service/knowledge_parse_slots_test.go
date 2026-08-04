package service

import (
	"sync"
	"testing"
	"time"
)

// TestParseSlotRegistry_CapEnforced verifies the per-KB cap: exactly `limit`
// acquisitions succeed, further ones fail until a slot is released.
func TestParseSlotRegistry_CapEnforced(t *testing.T) {
	r := &parseSlotRegistry{counts: make(map[string]int)}
	const kb = "kb-1"
	const limit = 5

	for i := 0; i < limit; i++ {
		if !r.tryAcquire(kb, limit) {
			t.Fatalf("acquire %d/%d should succeed", i+1, limit)
		}
	}
	if r.tryAcquire(kb, limit) {
		t.Fatalf("acquire beyond cap should fail")
	}
	r.release(kb)
	if !r.tryAcquire(kb, limit) {
		t.Fatalf("acquire after release should succeed")
	}
	if got := r.inUse(kb); got != limit {
		t.Fatalf("inUse = %d, want %d", got, limit)
	}
}

// TestParseSlotRegistry_PerKBIsolation verifies caps are tracked per KB.
func TestParseSlotRegistry_PerKBIsolation(t *testing.T) {
	r := &parseSlotRegistry{counts: make(map[string]int)}
	if !r.tryAcquire("a", 1) {
		t.Fatal("kb a acquire should succeed")
	}
	if r.tryAcquire("a", 1) {
		t.Fatal("kb a second acquire should fail")
	}
	if !r.tryAcquire("b", 1) {
		t.Fatal("kb b acquire should be independent of kb a")
	}
}

// TestParseSlotRegistry_NoCapWhenZero verifies limit<=0 means unlimited.
func TestParseSlotRegistry_NoCapWhenZero(t *testing.T) {
	r := &parseSlotRegistry{counts: make(map[string]int)}
	for i := 0; i < 100; i++ {
		if !r.tryAcquire("kb", 0) {
			t.Fatalf("acquire %d with no cap should succeed", i)
		}
	}
}

// TestParseSlotRegistry_ConcurrentSafety hammers the registry from many
// goroutines and checks the cap is never exceeded and counts return to zero.
func TestParseSlotRegistry_ConcurrentSafety(t *testing.T) {
	r := &parseSlotRegistry{counts: make(map[string]int)}
	const kb = "kb-c"
	const limit = 5
	var wg sync.WaitGroup
	var mu sync.Mutex
	maxSeen := 0

	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if r.tryAcquire(kb, limit) {
					n := r.inUse(kb)
					mu.Lock()
					if n > maxSeen {
						maxSeen = n
					}
					mu.Unlock()
					r.release(kb)
				}
			}
		}()
	}
	wg.Wait()

	if maxSeen > limit {
		t.Fatalf("cap exceeded: saw %d concurrent slots, limit %d", maxSeen, limit)
	}
	if got := r.inUse(kb); got != 0 {
		t.Fatalf("slots leaked: inUse = %d, want 0", got)
	}
}

// TestParseSlotRetryDelay_Backoff verifies the deferred-parse re-schedule
// delay grows exponentially with the defer count and is capped at 2 minutes
// (plus up to 10s jitter).
func TestParseSlotRetryDelay_Backoff(t *testing.T) {
	cases := []struct {
		deferCount int
		minBase    time.Duration
	}{
		{0, 10 * time.Second},
		{1, 20 * time.Second},
		{2, 40 * time.Second},
		{3, 80 * time.Second},
		{4, 2 * time.Minute}, // capped
		{10, 2 * time.Minute},
		{100, 2 * time.Minute},
	}
	const jitterMax = 10 * time.Second
	for _, c := range cases {
		for i := 0; i < 20; i++ {
			d := parseSlotRetryDelay(c.deferCount)
			if d < c.minBase || d > c.minBase+jitterMax {
				t.Fatalf("deferCount=%d: delay %s outside [%s, %s]",
					c.deferCount, d, c.minBase, c.minBase+jitterMax)
			}
		}
	}
}
