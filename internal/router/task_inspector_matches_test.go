package router

import (
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestMatchesByKB_WikiTriggerMatchedByKBID(t *testing.T) {
	// wiki:ingest carries only knowledge_base_id -> matched by kbID
	payload, _ := json.Marshal(map[string]any{"knowledge_base_id": "kb-9", "tenant_id": 1})
	if !matchesByKB(types.TypeWikiIngest, payload, "kb-9", map[string]struct{}{"k1": {}}) {
		t.Fatal("wiki:ingest with matching knowledge_base_id should match")
	}
	if matchesByKB(types.TypeWikiIngest, payload, "kb-other", map[string]struct{}{"k1": {}}) {
		t.Fatal("wiki:ingest with non-matching kbID should not match")
	}
}

func TestMatchesByKB_WikiFinalizeMatchedByKBID(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"knowledge_base_id": "kb-9"})
	if !matchesByKB(types.TypeWikiFinalize, payload, "kb-9", nil) {
		t.Fatal("wiki:finalize with matching knowledge_base_id should match")
	}
}

func TestMatchesByKB_KnowledgeScopedMatchedByMembership(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"knowledge_id": "k-abc"})
	set := map[string]struct{}{"k-abc": {}}
	if !matchesByKB(types.TypeChunkExtract, payload, "kb-9", set) {
		t.Fatal("chunk:extract for a member knowledge should match")
	}
	// not in set -> no match
	if matchesByKB(types.TypeChunkExtract, payload, "kb-9", map[string]struct{}{"other": {}}) {
		t.Fatal("chunk:extract for a non-member knowledge should not match")
	}
	// unrelated task type -> no match even if knowledge id matches
	if matchesByKB(types.TypeKBClone, payload, "kb-9", set) {
		t.Fatal("kb:clone should not be matched by knowledge membership")
	}
}
