package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGraphBatchCount verifies the graph batch-count formula matches the
// enqueue loop exactly. If the two drift, the seeded pending_subtasks_count
// in knowledge_post_process.go will not reach zero (row stuck in "finalizing")
// or will drain prematurely (row "completed" while graph work is pending).
// graphGenChunkBatchSize is 5; the formula is ceil(chunks / 5).
func TestGraphBatchCount(t *testing.T) {
	cases := []struct {
		name   string
		chunks int
		want   int
	}{
		{"zero chunks", 0, 0},
		{"one chunk", 1, 1},
		{"just under one batch", 4, 1},
		{"exactly one batch", 5, 1},
		{"one over", 6, 2},
		{"two batches", 10, 2},
		{"two over", 11, 3},
		{"ten batches", 50, 10},
		{"forty batches", 200, 40},
		{"forty over", 201, 41},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := graphBatchCount(c.chunks)
			assert.Equal(t, c.want, got, "graphBatchCount(%d)", c.chunks)
		})
	}
}

// TestGraphBatchCountMatchesEnqueueLoop asserts the count the orchestrator
// seeds (graphBatchCount) equals the number of batch tasks the spawn loop
// would enqueue for an arbitrary chunk count. The spawn loop iterates
// start := 0; start < chunkCount; start += graphGenChunkBatchSize, so the
// iteration count is exactly ceil(chunkCount / graphGenChunkBatchSize).
func TestGraphBatchCountMatchesEnqueueLoop(t *testing.T) {
	for _, chunkCount := range []int{0, 1, 19, 20, 21, 55, 100, 333, 1000} {
		seed := graphBatchCount(chunkCount)

		// Simulate the spawn loop's iteration count.
		enqueued := 0
		for start := 0; start < chunkCount; start += graphGenChunkBatchSize {
			_ = start
			enqueued++
		}

		assert.Equal(t, seed, enqueued,
			"seeded slots (%d) must equal enqueued batch tasks (%d) for %d chunks",
			seed, enqueued, chunkCount)
	}
}
