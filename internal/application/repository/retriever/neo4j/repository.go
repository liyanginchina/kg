package neo4j

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

// Graph-write batching knobs.
//
// The previous implementation opened a brand-new Neo4j session + write
// transaction for EVERY chunk's graph (one UNWIND of just that chunk's few
// nodes/rels). Under a bulk import that is tens of thousands of tiny
// transactions contending on the same kg write lock — the dominant cause of
// graph-extract slowness and intermittent failures. We now buffer incoming
// graphs across chunk tasks and flush them in bulk: a single session and a
// single transaction per flush, with the node/rel rows UNWIND in sub-batches.
const (
	// graphFlushEvery is the number of buffered graphs that triggers a
	// synchronous flush. The chunk task that crosses the threshold pays the
	// (amortized) flush cost and sees any error, so failures still surface
	// through asynq retry.
	graphFlushEvery = 512
	// graphFlushInterval bounds how long the tail of a burst can sit
	// un-flushed. The background ticker drains the buffer even when no single
	// task crosses graphFlushEvery.
	graphFlushInterval = 2 * time.Second
	// graphSubBatchRows caps the rows per UNWIND statement so a very large
	// flush does not build one gigantic Cypher parameter blob.
	graphSubBatchRows = 5000
	// graphMaxBuffer hard-caps buffered graphs to bound memory if Neo4j is
	// persistently unavailable: once exceeded the oldest graphs are dropped
	// (logged) rather than growing without limit.
	graphMaxBuffer = 4096
)

// bufferedGraph pairs a graph with the namespace it belongs to.
type bufferedGraph struct {
	ns    types.NameSpace
	graph *types.GraphData
}

// graphNodeImportQuery merges node rows idempotently. node.chunks is unioned
// so the same entity cited by multiple chunks accumulates every source chunk.
const graphNodeImportQuery = `
	UNWIND $data AS row
	CALL apoc.merge.node(row.labels, {name: row.name, kg: row.knowledge_id}, row.props, {}) YIELD node
	SET node.chunks = apoc.coll.union(node.chunks, row.chunks)
	RETURN distinct 'done' AS result`

// graphRelImportQuery merges relationship rows idempotently. apoc.merge on an
// empty property map deduplicates identical (source,type,target) triples.
const graphRelImportQuery = `
	UNWIND $data AS row
	CALL apoc.merge.node(row.source_labels, {name: row.source, kg: row.knowledge_id}, {}, {}) YIELD node as source
	CALL apoc.merge.node(row.target_labels, {name: row.target, kg: row.knowledge_id}, {}, {}) YIELD node as target
	CALL apoc.merge.relationship(source, row.type, {}, row.attributes, target) YIELD rel
	RETURN distinct 'done'`

// Neo4jRepository is a repository for Neo4j
type Neo4jRepository struct {
	driver     neo4j.Driver
	nodePrefix string

	// buf + mu implement cross-task graph-write batching (see graphFlush*
	// constants above). When driver is nil (Neo4j disabled) AddGraph is a
	// no-op and the buffer is never used.
	mu         sync.Mutex
	buf        []bufferedGraph
	flushEvery int
	stopCh     chan struct{}
	stopOnce   sync.Once
}

// NewNeo4jRepository creates a new Neo4j repository
func NewNeo4jRepository(driver neo4j.Driver) interfaces.RetrieveGraphRepository {
	repo := &Neo4jRepository{
		driver:     driver,
		nodePrefix: "ENTITY",
		flushEvery: graphFlushEvery,
		stopCh:     make(chan struct{}),
	}
	if driver != nil {
		go repo.flushLoop()
	}
	return repo
}

// Stop terminates the background flush goroutine, performing a final drain so
// no buffered graphs are lost on shutdown. Safe to call multiple times.
func (n *Neo4jRepository) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

// flushLoop periodically drains the buffer so a burst's tail is not left
// un-flushed indefinitely.
func (n *Neo4jRepository) flushLoop() {
	ticker := time.NewTicker(graphFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			n.mu.Lock()
			_ = n.flushLocked(context.Background())
			n.mu.Unlock()
			return
		case <-ticker.C:
			n.mu.Lock()
			if len(n.buf) > 0 {
				_ = n.flushLocked(context.Background())
			}
			n.mu.Unlock()
		}
	}
}

// _remove_hyphen removes hyphens from a string
func _remove_hyphen(s string) string {
	return strings.ReplaceAll(s, "-", "_")
}

// Labels returns the labels for a namespace
func (n *Neo4jRepository) Labels(namespace types.NameSpace) []string {
	res := make([]string, 0)
	for _, label := range namespace.Labels() {
		res = append(res, n.nodePrefix+_remove_hyphen(label))
	}
	return res
}

// Label returns the label for a namespace
func (n *Neo4jRepository) Label(namespace types.NameSpace) string {
	labels := n.Labels(namespace)
	return strings.Join(labels, ":")
}

// AddGraph buffers graphs and flushes them in bulk. Callers (one per chunk
// task) append a single graph; the buffer is flushed when it reaches
// graphFlushEvery graphs (synchronously, returning any error to the caller so
// asynq retries) or by the background ticker every graphFlushInterval.
//
// This collapses tens of thousands of per-chunk sessions/transactions into a
// handful of bulk writes — the key fix for graph-extract slowness and the
// write-lock contention that surfaced as GRAPH_EXTRACT_FAILED. Writes stay
// idempotent (apoc.merge.*), so replaying a buffered graph is safe.
func (n *Neo4jRepository) AddGraph(ctx context.Context, namespace types.NameSpace, graphs []*types.GraphData) error {
	if n.driver == nil {
		return nil
	}
	n.mu.Lock()
	for _, graph := range graphs {
		if graph == nil {
			continue
		}
		n.buf = append(n.buf, bufferedGraph{ns: namespace, graph: graph})
		if len(n.buf) > graphMaxBuffer {
			over := len(n.buf) - graphMaxBuffer
			n.buf = n.buf[over:]
			logger.Warnf(ctx, "graph buffer overflow, dropped %d oldest graphs to bound memory", over)
		}
	}
	if len(n.buf) >= n.flushEvery {
		err := n.flushLocked(ctx)
		n.mu.Unlock()
		return err
	}
	n.mu.Unlock()
	return nil
}

// flushLocked drains the buffer and writes all pending graphs in one session
// and one transaction. Caller must hold n.mu. Returns nil if the buffer is
// empty.
func (n *Neo4jRepository) flushLocked(ctx context.Context) error {
	if len(n.buf) == 0 {
		return nil
	}
	pending := n.buf
	n.buf = nil

	// Accumulate node/rel rows across all buffered graphs. Nodes/rels carry
	// their kg in the merge key, so mixing namespaces within one statement is
	// safe.
	nodeData := make([]map[string]interface{}, 0, len(pending)*4)
	relData := make([]map[string]interface{}, 0, len(pending)*4)
	for _, bg := range pending {
		labels := n.Labels(bg.ns)
		for _, node := range bg.graph.Node {
			nodeData = append(nodeData, map[string]interface{}{
				"name":         node.Name,
				"knowledge_id": bg.ns.Knowledge,
				"props":        map[string][]string{"attributes": node.Attributes},
				"chunks":       node.Chunks,
				"labels":       labels,
			})
		}
		for _, rel := range bg.graph.Relation {
			relData = append(relData, map[string]interface{}{
				"source":        rel.Node1,
				"target":        rel.Node2,
				"knowledge_id":  bg.ns.Knowledge,
				"type":          rel.Type,
				"source_labels": labels,
				"target_labels": labels,
			})
		}
	}

	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		if err := runGraphSubBatches(ctx, tx, graphNodeImportQuery, nodeData); err != nil {
			return nil, err
		}
		if err := runGraphSubBatches(ctx, tx, graphRelImportQuery, relData); err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		logger.Errorf(ctx, "failed to flush graph buffer (%d graphs): %v", len(pending), err)
		return err
	}
	logger.Debugf(ctx, "flushed graph buffer: %d graphs (%d nodes, %d rels)", len(pending), len(nodeData), len(relData))
	return nil
}

// runGraphSubBatches splits data into graphSubBatchRows-sized UNWIND
// statements so a single flush never builds one oversized Cypher parameter.
func runGraphSubBatches(ctx context.Context, tx neo4j.ManagedTransaction, query string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return nil
	}
	for start := 0; start < len(data); start += graphSubBatchRows {
		end := start + graphSubBatchRows
		if end > len(data) {
			end = len(data)
		}
		if _, err := tx.Run(ctx, query, map[string]interface{}{"data": data[start:end]}); err != nil {
			return fmt.Errorf("graph batch import failed: %v", err)
		}
	}
	return nil
}

// DelGraph deletes a graph from the Neo4j repository
func (n *Neo4jRepository) DelGraph(ctx context.Context, namespaces []types.NameSpace) error {
	if n.driver == nil {
		logger.Warnf(ctx, "NOT SUPPORT RETRIEVE GRAPH")
		return nil
	}
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	result, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		for _, namespace := range namespaces {
			labelExpr := n.Label(namespace)

			deleteRelsQuery := `
				CALL apoc.periodic.iterate(
					"MATCH (n:` + labelExpr + ` {kg: $knowledge_id})-[r]-(m:` + labelExpr + ` {kg: $knowledge_id}) RETURN r",
					"DELETE r",
					{batchSize: 1000, parallel: true, params: {knowledge_id: $knowledge_id}}
				) YIELD batches, total
				RETURN total
        	`
			if _, err := tx.Run(ctx, deleteRelsQuery, map[string]interface{}{"knowledge_id": namespace.Knowledge}); err != nil {
				return nil, fmt.Errorf("failed to delete relationships: %v", err)
			}

			deleteNodesQuery := `
				CALL apoc.periodic.iterate(
					"MATCH (n:` + labelExpr + ` {kg: $knowledge_id}) RETURN n",
					"DELETE n",
					{batchSize: 1000, parallel: true, params: {knowledge_id: $knowledge_id}}
				) YIELD batches, total
				RETURN total
        	`
			if _, err := tx.Run(ctx, deleteNodesQuery, map[string]interface{}{"knowledge_id": namespace.Knowledge}); err != nil {
				return nil, fmt.Errorf("failed to delete nodes: %v", err)
			}
		}
		return nil, nil
	})
	if err != nil {
		return err
	}
	logger.Infof(ctx, "delete graph result: %v", result)
	return nil
}

// SearchNode searches for nodes in the Neo4j repository
func (n *Neo4jRepository) SearchNode(
	ctx context.Context,
	namespace types.NameSpace,
	nodes []string,
) (*types.GraphData, error) {
	if n.driver == nil {
		logger.Warnf(ctx, "NOT SUPPORT RETRIEVE GRAPH")
		return nil, nil
	}
	session := n.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	result, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		labelExpr := n.Label(namespace)
		query := `
			MATCH (n:` + labelExpr + `)-[r]-(m:` + labelExpr + `)
			WHERE ANY(nodeText IN $nodes WHERE n.name CONTAINS nodeText)
			RETURN n, r, m
		`
		params := map[string]interface{}{"nodes": nodes}
		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, fmt.Errorf("failed to run query: %v", err)
		}

		graphData := &types.GraphData{}
		nodeSeen := make(map[string]bool)
		for result.Next(ctx) {
			record := result.Record()
			node, _ := record.Get("n")
			rel, _ := record.Get("r")
			targetNode, _ := record.Get("m")

			nodeData := node.(neo4j.Node)
			targetNodeData := targetNode.(neo4j.Node)

			// Convert node to types.Node
			for _, n := range []neo4j.Node{nodeData, targetNodeData} {
				nameStr := n.Props["name"].(string)
				if _, ok := nodeSeen[nameStr]; !ok {
					nodeSeen[nameStr] = true
					graphData.Node = append(graphData.Node, &types.GraphNode{
						Name:       nameStr,
						Chunks:     listI2listS(n.Props["chunks"].([]interface{})),
						Attributes: listI2listS(n.Props["attributes"].([]interface{})),
					})
				}
			}

			// Convert relationship to types.Relation
			relData := rel.(neo4j.Relationship)
			graphData.Relation = append(graphData.Relation, &types.GraphRelation{
				Node1: nodeData.Props["name"].(string),
				Node2: targetNodeData.Props["name"].(string),
				Type:  relData.Type,
			})
		}
		return graphData, nil
	})
	if err != nil {
		logger.Errorf(ctx, "search node failed: %v", err)
		return nil, err
	}
	return result.(*types.GraphData), nil
}

func listI2listS(list []any) []string {
	result := make([]string, len(list))
	for i, v := range list {
		result[i] = fmt.Sprintf("%v", v)
	}
	return result
}
