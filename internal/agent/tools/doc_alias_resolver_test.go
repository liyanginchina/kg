package tools

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// fakeKnowledgeService implements only the two methods resolveDocumentAlias
// touches. The remaining interface methods are promoted from the embedded nil
// interface and are never invoked by this test.
type fakeKnowledgeService struct {
	interfaces.KnowledgeService
	docsByKB map[string][]*types.Knowledge
}

func (f *fakeKnowledgeService) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	for _, docs := range f.docsByKB {
		for _, d := range docs {
			if d.ID == id {
				return d, nil
			}
		}
	}
	return nil, nil
}

func (f *fakeKnowledgeService) ListPagedKnowledgeByKnowledgeBaseID(
	_ context.Context,
	kbID string,
	page *types.Pagination,
	filter types.KnowledgeListFilter,
) (*types.PageResult, error) {
	docs := f.docsByKB[kbID]
	if filter.ParseStatus != "" {
		filtered := make([]*types.Knowledge, 0, len(docs))
		for _, d := range docs {
			if d.ParseStatus == filter.ParseStatus {
				filtered = append(filtered, d)
			}
		}
		docs = filtered
	}
	// docs are stored in created_at DESC order (oldest last) to mirror RecentDocs.
	start := (page.GetPage() - 1) * page.GetPageSize()
	if start > len(docs) {
		start = len(docs)
	}
	end := start + page.GetPageSize()
	if end > len(docs) {
		end = len(docs)
	}
	slice := make([]*types.Knowledge, len(docs[start:end]))
	copy(slice, docs[start:end])
	return &types.PageResult{
		Total:    int64(len(docs)),
		Page:     page.GetPage(),
		PageSize: page.GetPageSize(),
		Data:     slice,
	}, nil
}

func doc(id string, completed bool) *types.Knowledge {
	status := types.ParseStatusCompleted
	if !completed {
		status = types.ParseStatusDeleting
	}
	return &types.Knowledge{ID: id, ParseStatus: status, KnowledgeBaseID: "kb-1"}
}

func TestResolveDocumentAlias_SingleKB(t *testing.T) {
	svc := &fakeKnowledgeService{docsByKB: map[string][]*types.Knowledge{
		"kb-1": {doc("doc-1", true), doc("doc-2", true), doc("doc-3", true), doc("doc-4", true)},
	}}
	targets := types.SearchTargets{{KnowledgeBaseID: "kb-1", TenantID: 1}}

	got, err := resolveDocumentAlias(context.Background(), "d3", svc, targets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "doc-3" {
		t.Fatalf("d3 should resolve to doc-3, got %q", got)
	}
}

func TestResolveDocumentAlias_SkipsNonCompleted(t *testing.T) {
	// doc-2 is mid-deletion, so d2 must map to the next completed document.
	svc := &fakeKnowledgeService{docsByKB: map[string][]*types.Knowledge{
		"kb-1": {doc("doc-1", true), doc("doc-2", false), doc("doc-3", true)},
	}}
	targets := types.SearchTargets{{KnowledgeBaseID: "kb-1", TenantID: 1}}

	got, err := resolveDocumentAlias(context.Background(), "d2", svc, targets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "doc-3" {
		t.Fatalf("d2 should skip the deleting doc and resolve to doc-3, got %q", got)
	}
}

func TestResolveDocumentAlias_MultiKB(t *testing.T) {
	svc := &fakeKnowledgeService{docsByKB: map[string][]*types.Knowledge{
		"kb-1": {doc("a1", true), doc("a2", true), doc("a3", true)}, // d1..d3
		"kb-2": {doc("b1", true), doc("b2", true)},                  // d4..d5
	}}
	// targets order defines alias ordering across KBs
	targets := types.SearchTargets{
		{KnowledgeBaseID: "kb-1", TenantID: 1},
		{KnowledgeBaseID: "kb-2", TenantID: 1},
	}

	if got, _ := resolveDocumentAlias(context.Background(), "d1", svc, targets); got != "a1" {
		t.Fatalf("d1 = %q, want a1", got)
	}
	if got, _ := resolveDocumentAlias(context.Background(), "d3", svc, targets); got != "a3" {
		t.Fatalf("d3 = %q, want a3", got)
	}
	if got, _ := resolveDocumentAlias(context.Background(), "d4", svc, targets); got != "b1" {
		t.Fatalf("d4 = %q, want b1 (first doc of second KB)", got)
	}
	if got, _ := resolveDocumentAlias(context.Background(), "d5", svc, targets); got != "b2" {
		t.Fatalf("d5 = %q, want b2", got)
	}
}

func TestResolveDocumentAlias_OutOfRange(t *testing.T) {
	svc := &fakeKnowledgeService{docsByKB: map[string][]*types.Knowledge{
		"kb-1": {doc("doc-1", true), doc("doc-2", true)},
	}}
	targets := types.SearchTargets{{KnowledgeBaseID: "kb-1", TenantID: 1}}

	got, err := resolveDocumentAlias(context.Background(), "d9", svc, targets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Fatalf("out-of-range alias should resolve to empty string, got %q", got)
	}
}

func TestResolveDocumentAlias_NonAliasPassthrough(t *testing.T) {
	svc := &fakeKnowledgeService{}
	targets := types.SearchTargets{{KnowledgeBaseID: "kb-1", TenantID: 1}}

	cases := []string{"", "real-uuid-1234", "c5", "b2"}
	for _, c := range cases {
		got, err := resolveDocumentAlias(context.Background(), c, svc, targets)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", c, err)
		}
		if got != c {
			t.Fatalf("non-alias %q should pass through unchanged, got %q", c, got)
		}
	}
}
