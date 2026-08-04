package tools

import (
	"context"
	"regexp"
	"strconv"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// docAliasRE matches the request-local document alias `dN` that the model may
// pass as a knowledge_id / knowledge_ids argument. Aliases are positional and
// request-local; see internal/llmreference for the full scheme.
var docAliasRE = regexp.MustCompile(`^d[1-9][0-9]*$`)

// IsDocumentAlias reports whether id is a request-local document alias (dN).
func IsDocumentAlias(id string) bool {
	return docAliasRE.MatchString(id)
}

// resolveDocumentAlias resolves a request-local document alias (dN) to a real
// knowledge UUID in the cases where the llmreference registry did not register
// it.
//
// Background: the model is instructed to reference documents as dN, but
// registerRuntimeReferences (internal/agent/observe.go) only registers each
// bound KB's RecentDocs (top 10, created_at DESC) as d1..d10. When the model
// references d11+ — a document it saw in an earlier agent turn, or one beyond
// the RecentDocs window — the alias is absent from the registry,
// DecodeToolCalls leaves the raw dN in the tool arguments, and the tool would
// otherwise fail with "knowledge not found".
//
// The alias dN is positional: it is the Nth document (created_at DESC,
// completed) across the bound knowledge bases in searchTargets order — exactly
// the ordering registerRuntimeReferences uses. For N within a KB's RecentDocs
// this matches the registry byte-for-byte; for larger N it extends the same
// "Nth most recent document" semantics so the tool still resolves instead of
// erroring. Only documents inside searchTargets are enumerated, so the resolved
// id is always in-scope (the caller's existing permission checks still run).
//
// Returns:
//   - the resolved real UUID when the alias maps to an in-scope document
//   - the input unchanged when it is not a dN alias (already a real id, or empty)
//   - ("", nil) when the id is a dN alias but the index is out of range; the
//     caller should report a clear "out of range" error instead of a generic
//     "knowledge not found"
func resolveDocumentAlias(
	ctx context.Context,
	id string,
	knowledgeService interfaces.KnowledgeService,
	searchTargets types.SearchTargets,
) (string, error) {
	if id == "" || knowledgeService == nil {
		return id, nil
	}
	if !docAliasRE.MatchString(id) {
		// Not an alias — already a real identifier (UUIDs never match this shape).
		return id, nil
	}

	index, err := strconv.Atoi(id[1:])
	if err != nil || index <= 0 {
		return id, nil
	}

	const pageSize = 50
	for _, target := range searchTargets {
		if target == nil || target.KnowledgeBaseID == "" {
			continue
		}
		page := 0
		for {
			page++
			pageResult, err := knowledgeService.ListPagedKnowledgeByKnowledgeBaseID(ctx, target.KnowledgeBaseID, &types.Pagination{
				Page:     page,
				PageSize: pageSize,
			}, types.KnowledgeListFilter{ParseStatus: types.ParseStatusCompleted})
			if err != nil {
				return "", err
			}
			docs, ok := pageResult.Data.([]*types.Knowledge)
			if !ok || len(docs) == 0 {
				break
			}
			for _, k := range docs {
				index--
				if index == 0 {
					return k.ID, nil
				}
			}
			if int64(len(docs)) < int64(pageSize) {
				break
			}
		}
	}
	// Alias index exceeds the total number of in-scope completed documents.
	return "", nil
}
