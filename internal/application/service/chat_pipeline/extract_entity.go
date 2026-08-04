package chatpipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// PluginExtractEntity is a plugin for extracting entities from user queries
// It uses historical dialog context and large language models to identify key entities in the user's original query
type PluginExtractEntity struct {
	modelService      interfaces.ModelService         // Model service for calling large language models
	template          *types.PromptTemplateStructured // Template for generating prompts
	knowledgeBaseRepo interfaces.KnowledgeBaseRepository
	knowledgeService  interfaces.KnowledgeService // For shared KB document resolution
	knowledgeRepo     interfaces.KnowledgeRepository
}

// NewPluginExtractEntity creates a new extract-entity plugin instance
// Also registers the plugin with the event manager
func NewPluginExtractEntity(
	eventManager *EventManager,
	modelService interfaces.ModelService,
	knowledgeBaseRepo interfaces.KnowledgeBaseRepository,
	knowledgeService interfaces.KnowledgeService,
	knowledgeRepo interfaces.KnowledgeRepository,
	config *config.Config,
) *PluginExtractEntity {
	res := &PluginExtractEntity{
		modelService:      modelService,
		template:          config.ExtractManager.ExtractEntity,
		knowledgeBaseRepo: knowledgeBaseRepo,
		knowledgeService:  knowledgeService,
		knowledgeRepo:     knowledgeRepo,
	}
	eventManager.Register(res)
	return res
}

// ActivationEvents returns the list of event types this plugin responds to.
func (p *PluginExtractEntity) ActivationEvents() []types.EventType {
	return []types.EventType{types.QUERY_UNDERSTAND}
}

// OnEvent processes triggered events
// When receiving a QUERY_UNDERSTAND event, it extracts entities from the query
func (p *PluginExtractEntity) OnEvent(ctx context.Context,
	eventType types.EventType, chatManage *types.ChatManage, next func() *PluginError,
) *PluginError {
	if strings.ToLower(os.Getenv("NEO4J_ENABLE")) != "true" {
		logger.Debugf(ctx, "skipping extract entity, neo4j is disabled")
		return next()
	}

	query := chatManage.Query

	model, err := p.modelService.GetChatModel(ctx, chatManage.ChatModelID)
	if err != nil {
		logger.Errorf(ctx, "Failed to get model, session_id: %s, error: %v", chatManage.SessionID, err)
		return next()
	}

	// Collect all knowledge base IDs to query
	kbIDSet := make(map[string]struct{})
	for _, id := range chatManage.KnowledgeBaseIDs {
		kbIDSet[id] = struct{}{}
	}

	// If KnowledgeIDs is specified, retrieve them and collect their knowledge base IDs (include shared KB docs)
	// Also build a mapping from KnowledgeID to KnowledgeBaseID
	knowledgeToKBMap := make(map[string]string)
	if len(chatManage.KnowledgeIDs) > 0 {
		knowledges, err := p.knowledgeService.GetKnowledgeBatchWithSharedAccess(ctx, chatManage.TenantID, chatManage.KnowledgeIDs)
		if err != nil {
			logger.Errorf(ctx, "failed to get knowledges: %v", err)
			return next()
		}
		for _, k := range knowledges {
			kbIDSet[k.KnowledgeBaseID] = struct{}{}
			knowledgeToKBMap[k.ID] = k.KnowledgeBaseID
		}
	}

	// Convert set to slice
	allKBIDs := make([]string, 0, len(kbIDSet))
	for id := range kbIDSet {
		allKBIDs = append(allKBIDs, id)
	}

	// Batch retrieve all knowledge bases
	kbs, err := p.knowledgeBaseRepo.GetKnowledgeBaseByIDs(ctx, allKBIDs)
	if err != nil {
		logger.Errorf(ctx, "failed to get knowledge bases: %v", err)
		return next()
	}

	// Check if any knowledge base has ExtractConfig enabled and collect their IDs
	enabledKBSet := make(map[string]struct{})
	for _, kb := range kbs {
		if kb.ExtractConfig != nil && kb.ExtractConfig.Enabled {
			enabledKBSet[kb.ID] = struct{}{}
		}
	}
	if len(enabledKBSet) == 0 {
		logger.Debugf(ctx, "no knowledge base has extract config enabled")
		return next()
	}

	// Save enabled knowledge base IDs for later use in search_entity
	enabledKBIDs := make([]string, 0, len(enabledKBSet))
	for id := range enabledKBSet {
		enabledKBIDs = append(enabledKBIDs, id)
	}
	chatManage.EntityKBIDs = enabledKBIDs

	// Filter knowledgeToKBMap to only include files from enabled knowledge bases
	entityKnowledge := make(map[string]string)
	for knowledgeID, kbID := range knowledgeToKBMap {
		if _, ok := enabledKBSet[kbID]; ok {
			entityKnowledge[knowledgeID] = kbID
		}
	}
	chatManage.EntityKnowledge = entityKnowledge

	template := &types.PromptTemplateStructured{
		Description: p.template.Description,
		Examples:    p.template.Examples,
	}
	extractor := NewExtractor(model, template)
	graph, err := extractor.Extract(ctx, query)
	if err != nil {
		logger.Errorf(ctx, "Failed to extract entities, session_id: %s, error: %v", chatManage.SessionID, err)
		return next()
	}
	nodes := []string{}
	for _, node := range graph.Node {
		nodes = append(nodes, node.Name)
	}
	logger.Debugf(ctx, "extracted node: %v", nodes)
	chatManage.Entity = nodes
	return next()
}

// Extractor is a struct for extracting entities
type Extractor struct {
	chat     chat.Chat
	formater *Formater
	template *types.PromptTemplateStructured
	chatOpt  *chat.ChatOptions
}

// NewExtractor creates a new extractor
func NewExtractor(
	chatModel chat.Chat,
	template *types.PromptTemplateStructured,
) Extractor {
	think := false
	return Extractor{
		chat:     chatModel,
		formater: NewFormater(),
		template: template,
		chatOpt: &chat.ChatOptions{
			Temperature: 0.3,
			MaxTokens:   4096,
			Thinking:    &think,
		},
	}
}

// ErrGraphParseFailed marks an Extract failure that happened AFTER the LLM
// call succeeded — the model returned content but it could not be parsed into
// GraphData even after repairJSON/salvageFragments. Unlike transport errors,
// retrying the same prompt against the same model is very likely to produce
// the same malformed output, so callers should treat this as deterministic:
// degrade (e.g. empty graph for the chunk) instead of burning asynq retries.
// Detect it with errors.Is(err, ErrGraphParseFailed).
var ErrGraphParseFailed = errors.New("graph output parse failed")

// Extract extracts entities from content
func (e *Extractor) Extract(ctx context.Context, content string) (*types.GraphData, error) {
	generator := NewQAPromptGenerator(e.formater, e.template)

	// logger.Debugf(ctx, "chat system: %s", generator.System(ctx))
	// logger.Debugf(ctx, "chat user: %s", generator.User(ctx, content))

	chatResponse, err := e.chat.Chat(ctx, generator.Render(ctx, content), e.chatOpt)
	if err != nil {
		logger.Errorf(ctx, "failed to chat: %v", err)
		return nil, err
	}

	graph, err := e.formater.ParseGraph(ctx, chatResponse.Content)
	if err != nil {
		logger.Errorf(ctx, "failed to parse graph: %v", err)
		return nil, fmt.Errorf("%w: %v", ErrGraphParseFailed, err)
	}
	// e.RemoveUnknownRelation(ctx, graph)
	return graph, nil
}

// RemoveUnknownRelation removes unknown relations from graph
func (e *Extractor) RemoveUnknownRelation(ctx context.Context, graph *types.GraphData) {
	relationType := make(map[string]bool)
	for _, tag := range e.template.Tags {
		relationType[tag] = true
	}

	relationNew := make([]*types.GraphRelation, 0)
	for _, relation := range graph.Relation {
		if _, ok := relationType[relation.Type]; ok {
			relationNew = append(relationNew, relation)
		} else {
			logger.Infof(ctx, "Unknown relation type %s with %v, ignore it", relation.Type, e.template.Tags)
		}
	}
	graph.Relation = relationNew
}

// QAPromptGenerator is a struct for generating QA prompts
type QAPromptGenerator struct {
	Formater        *Formater
	Template        *types.PromptTemplateStructured
	ExamplesHeading string
	QuestionHeading string
	QuestionPrefix  string
	AnswerPrefix    string
}

// NewQAPromptGenerator creates a new QA prompt generator
func NewQAPromptGenerator(formater *Formater, template *types.PromptTemplateStructured) *QAPromptGenerator {
	return &QAPromptGenerator{
		Formater:        formater,
		Template:        template,
		ExamplesHeading: "# Examples",
		QuestionHeading: "# Question",
		QuestionPrefix:  "Q: ",
		AnswerPrefix:    "A: ",
	}
}

// System generates a system prompt
func (qa *QAPromptGenerator) System(ctx context.Context) string {
	promptLines := []string{}

	if len(qa.Template.Tags) == 0 {
		promptLines = append(promptLines, qa.Template.Description)
	} else {
		tags, _ := json.Marshal(qa.Template.Tags)
		promptLines = append(promptLines, fmt.Sprintf(qa.Template.Description, string(tags)))
	}
	if len(qa.Template.Examples) > 0 {
		promptLines = append(promptLines, qa.ExamplesHeading)
		for _, example := range qa.Template.Examples {
			// Question
			promptLines = append(promptLines, fmt.Sprintf("%s%s", qa.QuestionPrefix, strings.TrimSpace(example.Text)))

			// Answer
			answer, err := qa.Formater.formatExtraction(example.Node, example.Relation)
			if err != nil {
				return ""
			}
			promptLines = append(promptLines, fmt.Sprintf("%s%s", qa.AnswerPrefix, answer))

			// new line
			promptLines = append(promptLines, "")
		}
	}
	return strings.Join(promptLines, "\n")
}

// User generates a user prompt
func (qa *QAPromptGenerator) User(ctx context.Context, question string) string {
	promptLines := []string{}
	promptLines = append(promptLines, qa.QuestionHeading)
	promptLines = append(promptLines, fmt.Sprintf("%s%s", qa.QuestionPrefix, question))
	promptLines = append(promptLines, qa.AnswerPrefix)
	return strings.Join(promptLines, "\n")
}

// Render renders a prompt
func (qa *QAPromptGenerator) Render(ctx context.Context, question string) []chat.Message {
	return []chat.Message{
		{
			Role:    "system",
			Content: qa.System(ctx),
		},
		{
			Role:    "user",
			Content: qa.User(ctx, question),
		},
	}
}

// FormatType is a type for format types
type FormatType string

const (
	// FormatTypeJSON is a format type for JSON
	FormatTypeJSON FormatType = "json"
	// FormatTypeYAML is a format type for YAML
	FormatTypeYAML FormatType = "yaml"
)

const (
	_FENCE_START   = "```"
	_LANGUAGE_TAG  = `(?P<lang>[A-Za-z0-9_+-]+)?`
	_FENCE_NEWLINE = `(?:\s*\n)?`
	_FENCE_BODY    = `(?P<body>[\s\S]*?)`
	_FENCE_END     = "```"
)

var _FENCE_RE = regexp.MustCompile(
	_FENCE_START + _LANGUAGE_TAG + _FENCE_NEWLINE + _FENCE_BODY + _FENCE_END,
)

// Formater is a struct for formatting entities
type Formater struct {
	attributeSuffix string
	formatType      FormatType
	useFences       bool
	nodePrefix      string

	relationSource string
	relationTarget string
	relationPrefix string
}

// NewFormater creates a new formater
func NewFormater() *Formater {
	return &Formater{
		attributeSuffix: "_attributes",
		formatType:      FormatTypeJSON,
		useFences:       true,
		nodePrefix:      "entity",
		relationSource:  "entity1",
		relationTarget:  "entity2",
		relationPrefix:  "relation",
	}
}

// formatExtraction formats extraction
func (f *Formater) formatExtraction(nodes []*types.GraphNode, relations []*types.GraphRelation) (string, error) {
	items := make([]map[string]interface{}, 0)
	for _, node := range nodes {
		item := map[string]interface{}{
			f.nodePrefix: node.Name,
		}
		if len(node.Attributes) > 0 {
			item[fmt.Sprintf("%s%s", f.nodePrefix, f.attributeSuffix)] = node.Attributes
		}
		items = append(items, item)
	}
	for _, relation := range relations {
		item := map[string]interface{}{
			f.relationSource: relation.Node1,
			f.relationTarget: relation.Node2,
			f.relationPrefix: relation.Type,
		}
		items = append(items, item)
	}
	formatted := ""
	switch f.formatType {
	default:
		formattedBytes, err := json.MarshalIndent(items, "", "  ")
		if err != nil {
			return "", err
		}
		formatted = string(formattedBytes)
	}
	if f.useFences {
		formatted = f.addFences(formatted)
	}
	return formatted, nil
}

func (f *Formater) parseOutput(ctx context.Context, text string) ([]map[string]interface{}, error) {
	if text == "" {
		return nil, errors.New("empty or invalid input string")
	}
	content := f.extractContent(ctx, text)
	// logger.Debugf(ctx, "Extracted content: %s", content)
	if content == "" {
		return nil, errors.New("empty or invalid input string")
	}

	if f.formatType == FormatTypeJSON {
		// The graph-extraction model (e.g. DeepSeek via the chinalco gateway)
		// frequently emits non-strict JSON: unescaped newlines / tabs inside
		// string values, stray trailing characters after the closing bracket
		// (e.g. ')'), leading prose before the first bracket, trailing commas,
		// several concatenated JSON objects without a wrapping array, or
		// arbitrary special characters / mojibake ('<', '>', '(', non-ASCII,
		// markdown/HTML fragments, etc.) leaked into structural positions.
		// Normalize every one of those defects before the strict unmarshal so
		// a single malformed chunk does not fail the whole graph extraction
		// task.
		if repaired := repairJSON(content); repaired != "" {
			content = repaired
		}
	}

	var parsed interface{}
	var err error
	if f.formatType == FormatTypeJSON {
		err = json.Unmarshal([]byte(content), &parsed)
		if err != nil {
			// The strict unmarshal failed even after repairJSON normalized the
			// common defects. The graph-extraction model (e.g. DeepSeek via the
			// chinalco gateway) sometimes emits a payload where ONE fragment is
			// malformed — an unterminated string, a stray quote, or a special
			// character that corrupts the structural state — while the rest of
			// the payload is perfectly good. Salvage the well-formed fragments
			// instead of failing the whole chunk; a single bad item must not
			// discard an entire document's graph extraction.
			if salvaged, sErr := salvageFragments(content); sErr == nil && len(salvaged) > 0 {
				logger.Warnf(ctx, "graph JSON salvage recovered %d fragment(s) after parse error: %v", len(salvaged), err)
				parsed = salvaged
				err = nil
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s content: %s", strings.ToUpper(string(f.formatType)), err.Error())
	}
	if parsed == nil {
		return nil, fmt.Errorf("content must be a list of extractions or a dict")
	}

	var items []interface{}
	if parsedMap, ok := parsed.(map[string]interface{}); ok {
		items = []interface{}{parsedMap}
	} else if parsedList, ok := parsed.([]interface{}); ok {
		items = parsedList
	} else {
		return nil, fmt.Errorf("expected list or dict, got %T", parsed)
	}

	itemsList := make([]map[string]interface{}, 0)
	for _, item := range items {
		if itemMap, ok := item.(map[string]interface{}); ok {
			itemsList = append(itemsList, itemMap)
		} else {
			// Best-effort: skip a non-mapping element (e.g. a bare string or
			// number the model emitted inside the list) instead of failing
			// the entire extraction. The well-formed entries are still
			// usable for graph construction.
			logger.Warnf(ctx, "skipping non-mapping item in extraction sequence: %T", item)
		}
	}
	return itemsList, nil
}

// trailingCommaRE matches a comma immediately preceding a closing brace or
// bracket. JSON forbids trailing commas, and LLMs emit them often; stripping
// them is safe because a comma can never be valid immediately before '}'/']'.
var trailingCommaRE = regexp.MustCompile(`,\s*([}\]])`)

// repairJSON normalizes the most common LLM-produced JSON defects that the
// strict encoding/json package rejects. It first trims the payload to its
// outermost balanced object/array (dropping any leading prose and trailing
// junk such as ')', stray braces, or explanatory text), then escapes
// unescaped control characters inside JSON string literals and removes
// trailing commas. It returns an empty string when nothing parseable can be
// recovered, so callers can keep their original error path.
//
// This is intentionally conservative: it only rewrites content inside string
// literals and trims to a balanced container — valid JSON passes through
// untouched.
func repairJSON(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	trimmed := extractTopLevelJSON(s)
	if trimmed == "" {
		// No balanced JSON container found; nothing to repair.
		return ""
	}
	s = trimmed
	s = sanitizeJSONStrings(s)
	// Drop non-ASCII runes that leaked into structural positions (between
	// JSON tokens). In valid JSON every non-ASCII rune lives inside a string
	// value (UTF-8), so a non-ASCII rune outside a string literal is either
	// encoding corruption (mojibake, e.g. the chinalco gateway returning
	// Latin-1 bytes) or stray prose the model emitted between array/object
	// elements. Discarding it lets json.Unmarshal proceed instead of failing
	// with "invalid character 'ä' after array element".
	s = stripStructuralGarbage(s)
	// LLMs frequently concatenate values without the separating comma (e.g.
	// several objects inside an array with prose in between, or a trailing
	// comma that was already stripped). After garbage removal, re-insert a
	// comma between any two adjacent values so the payload is valid JSON.
	s = insertMissingCommas(s)
	s = trailingCommaRE.ReplaceAllString(s, "$1")
	return s
}

// extractTopLevelJSON returns the outermost balanced JSON value(s) from s,
// respecting string literals so braces/brackets inside strings do not
// unbalance the scan. When several top-level values are concatenated (e.g.
// the model emitted multiple JSON objects without a wrapping array), they are
// joined into a single JSON array so the result stays parseable as a list.
// It returns an empty string when no balanced value can be found.
func extractTopLevelJSON(s string) string {
	var values []string
	inString := false
	escaped := false
	depth := 0
	start := -1
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			if depth == 0 {
				start = i
			}
			depth++
		case '}', ']':
			if depth > 0 {
				depth--
				if depth == 0 && start >= 0 {
					values = append(values, s[start:i+1])
					start = -1
				}
			}
		}
	}
	switch len(values) {
	case 0:
		return ""
	case 1:
		return strings.TrimSpace(values[0])
	default:
		// Concatenated top-level values -> wrap them in an array.
		return "[" + strings.Join(values, ",") + "]"
	}
}

// extractTopLevelFragments returns every top-level balanced JSON value
// (object or array) found in s, respecting string literals so that braces and
// brackets inside strings do not unbalance the scan. Unlike extractTopLevelJSON
// it does NOT join the fragments into a single array — the caller decides how
// to interpret each one independently. It returns an empty slice when no
// balanced value can be found.
func extractTopLevelFragments(s string) []string {
	var frags []string
	inString := false
	escaped := false
	depth := 0
	start := -1
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			if depth == 0 {
				start = i
			}
			depth++
		case '}', ']':
			if depth > 0 {
				depth--
				if depth == 0 && start >= 0 {
					frags = append(frags, s[start:i+1])
					start = -1
				}
			}
		}
	}
	return frags
}

// salvageFragments attempts to recover usable graph data from a payload that
// failed the strict JSON unmarshal. It splits the payload into top-level
// balanced fragments and unmarshals each one; objects are kept as-is, arrays
// contribute their (mapping) elements. When an array fragment fails to unmarshal
// as a whole — e.g. because one of its elements is malformed — it falls back to
// salvaging each array element individually. The result is a flat list of
// mapping items, exactly what parseOutput expects downstream. It returns an
// error (and no items) only when nothing usable could be recovered.
func salvageFragments(content string) ([]interface{}, error) {
	frags := extractTopLevelFragments(content)
	collected := make([]interface{}, 0)
	for _, frag := range frags {
		trimmed := strings.TrimSpace(frag)
		var v interface{}
		if err := json.Unmarshal([]byte(trimmed), &v); err == nil {
			switch val := v.(type) {
			case map[string]interface{}:
				collected = append(collected, val)
			case []interface{}:
				collected = append(collected, val...)
			}
			continue
		}
		// A top-level array that fails as a whole may still contain good
		// elements — try to salvage them one by one.
		if strings.HasPrefix(trimmed, "[") {
			if elems, e2 := salvageArrayElements(trimmed); e2 == nil {
				collected = append(collected, elems...)
			}
		}
	}
	if len(collected) == 0 {
		return nil, errors.New("no salvageable JSON fragments")
	}
	return collected, nil
}

// salvageArrayElements salvages the individual elements of a top-level JSON
// array whose whole-array unmarshal failed. It scans the array body for
// top-level balanced elements (string-aware) and unmarshals each; malformed
// elements are skipped. It returns an error when no element could be recovered.
func salvageArrayElements(frag string) ([]interface{}, error) {
	inner := strings.TrimSpace(frag)
	if len(inner) < 2 || inner[0] != '[' || inner[len(inner)-1] != ']' {
		return nil, errors.New("not an array fragment")
	}
	body := inner[1 : len(inner)-1]
	var elems []interface{}
	inString := false
	escaped := false
	depth := 0
	start := -1
	flush := func(end int) {
		if start < 0 {
			return
		}
		elem := strings.TrimSpace(body[start:end])
		start = -1
		if elem == "" {
			return
		}
		var v interface{}
		if json.Unmarshal([]byte(elem), &v) == nil {
			elems = append(elems, v)
		}
	}
	for i := 0; i < len(body); i++ {
		c := body[i]
		if inString {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			if depth == 0 {
				start = i
			}
			depth++
		case '}', ']':
			if depth > 0 {
				depth--
				if depth == 0 && start >= 0 {
					flush(i + 1)
				}
			}
		case ',':
			if depth == 0 {
				flush(i)
			}
		}
	}
	flush(len(body))
	if len(elems) == 0 {
		return nil, errors.New("no salvageable array elements")
	}
	return elems, nil
}

// sanitizeJSONStrings walks the input character by character and escapes
// control characters that are illegal inside JSON string literals but which
// LLMs frequently emit verbatim (raw newlines, tabs, carriage returns, and
// other sub-0x20 bytes). Tokens outside of string literals are left
// untouched, so legitimate whitespace between JSON elements is preserved.
func sanitizeJSONStrings(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inString := false
	escaped := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString {
			if escaped {
				b.WriteByte(c)
				escaped = false
				continue
			}
			switch c {
			case '\\':
				b.WriteByte(c)
				escaped = true
			case '"':
				b.WriteByte(c)
				inString = false
			case '\n':
				b.WriteString(`\n`)
			case '\r':
				b.WriteString(`\r`)
			case '\t':
				b.WriteString(`\t`)
			default:
				if c < 0x20 {
					b.WriteString(fmt.Sprintf(`\u%04x`, c))
				} else {
					b.WriteByte(c)
				}
			}
			continue
		}
		switch c {
		case '"':
			b.WriteByte(c)
			inString = true
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// stripStructuralGarbage removes every character that is NOT part of valid
// JSON structure when it appears OUTSIDE a string literal. In well-formed
// JSON the only things allowed outside strings are: whitespace, the
// structural punctuation {}[]:, the value delimiters ("), number characters,
// and the literals true/false/null. Anything else outside a string —
// stray prose, markdown/HTML tags such as '<' or '>', parentheses, slashes,
// asterisks, non-ASCII mojibake like 'ä'/'é'/'å', etc. — is corruption or
// commentary the model emitted between JSON elements and is safe to drop.
//
// Characters INSIDE string literals are preserved untouched, so legitimate
// UTF-8 text and punctuation in entity/relation names (e.g. an entity whose
// name contains '<' or a Chinese character) survive intact.
//
// This generalizes the earlier "non-ASCII only" filter so that the full
// alphabet of special characters the graph model has been observed to leak
// into structural positions is tolerated.
func stripStructuralGarbage(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inString := false
	escaped := false
	i := 0
	n := len(s)
	for i < n {
		c := s[i]
		if inString {
			if escaped {
				b.WriteByte(c)
				escaped = false
				i++
				continue
			}
			if c == '\\' {
				b.WriteByte(c)
				escaped = true
				i++
				continue
			}
			if c == '"' {
				b.WriteByte(c)
				inString = false
				i++
				continue
			}
			b.WriteByte(c)
			i++
			continue
		}
		switch {
		case c == '"':
			b.WriteByte(c)
			inString = true
			i++
		case c == '{' || c == '}' || c == '[' || c == ']' || c == ':' || c == ',':
			b.WriteByte(c)
			i++
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			b.WriteByte(c)
			i++
		case isJSONNumberByte(c):
			// Keep a whole number run (digits and . + - e E).
			j := i
			for j < n && isJSONNumberByte(s[j]) {
				j++
			}
			b.WriteString(s[i:j])
			i = j
		case isASCIILetter(c):
			// Could be a true/false/null literal, or stray prose. Keep only
			// the exact literals; drop everything else.
			j := i
			for j < n && isASCIILetter(s[j]) {
				j++
			}
			word := s[i:j]
			if word == "true" || word == "false" || word == "null" {
				b.WriteString(word)
			}
			i = j
		default:
			// Any other byte outside a string literal: drop it. This covers
			// '<', '>', '(', ')', '/', '*', '#', '@', '=', non-ASCII multibyte
			// starts (>= 0x80), and any other junk the model produced.
			i++
		}
	}
	return b.String()
}

// RepairJSON exposes the internal repairJSON normalization to other packages
// (notably the wiki postprocess module) so a single, battle-tested LLM-JSON
// hardener is shared across every graph/entity extraction path instead of
// being re-implemented per caller. It returns an empty string when nothing
// parseable can be recovered.
func RepairJSON(s string) string {
	return repairJSON(s)
}

// SalvageJSONToMaps exposes the internal salvageFragments recovery to other
// packages. It splits a payload that failed a strict unmarshal into its
// top-level balanced JSON object/array fragments and returns only the object
// fragments as maps — exactly what downstream callers need to reconstruct a
// typed struct via json.Marshal/Unmarshal. It returns an error (and no maps)
// only when nothing usable could be recovered.
func SalvageJSONToMaps(s string) ([]map[string]interface{}, error) {
	items, err := salvageFragments(s)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(items))
	for _, it := range items {
		if m, ok := it.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no salvageable JSON fragments")
	}
	return out, nil
}

// isJSONNumberByte reports whether b may appear inside a JSON number token.
func isJSONNumberByte(b byte) bool {
	switch {
	case b >= '0' && b <= '9':
		return true
	case b == '.' || b == '-' || b == '+' || b == 'e' || b == 'E':
		return true
	}
	return false
}

// isASCIILetter reports whether b is an ASCII letter (a-z, A-Z).
func isASCIILetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// insertMissingCommas re-inserts the comma that separates two adjacent JSON
// values when the model omitted it — a common defect when several objects or
// strings are concatenated inside an array (or several top-level values) with
// prose in between. It only acts in structural positions: when the previous
// non-whitespace token closed a value ('}', ']' or a string) and the next
// token opens a new value ('{', '[', '"', a digit, or a true/false/null
// literal), a comma is inserted. Valid JSON never places a value directly
// after another value without a comma, so this is a no-op on well-formed
// input and cannot produce invalid output.
func insertMissingCommas(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inString := false
	escaped := false
	// prev holds the last structural token seen outside a string: '}' or ']'
	// (a value just closed) or '"' (a string value just closed). The zero
	// value means "no value-closing token pending".
	prev := rune(0)
	i := 0
	n := len(s)
	for i < n {
		c := s[i]
		if inString {
			if escaped {
				b.WriteByte(c)
				escaped = false
				i++
				continue
			}
			if c == '\\' {
				b.WriteByte(c)
				escaped = true
				i++
				continue
			}
			if c == '"' {
				b.WriteByte(c)
				inString = false
				prev = '"'
				i++
				continue
			}
			b.WriteByte(c)
			i++
			continue
		}
		switch {
		case c == '"':
			if prev == '}' || prev == ']' || prev == '"' {
				b.WriteByte(',')
			}
			b.WriteByte(c)
			inString = true
			prev = 0
			i++
		case c == '{' || c == '[':
			if prev == '}' || prev == ']' || prev == '"' {
				b.WriteByte(',')
			}
			b.WriteByte(c)
			prev = 0
			i++
		case c >= '0' && c <= '9':
			if prev == '}' || prev == ']' || prev == '"' {
				b.WriteByte(',')
			}
			// Keep a whole number run (digits and . + - e E).
			j := i
			for j < n && isJSONNumberByte(s[j]) {
				j++
			}
			b.WriteString(s[i:j])
			i = j
			prev = 0
		case isASCIILetter(c):
			// Whole word run. stripStructuralGarbage already dropped every
			// letter run that is not a literal, so the only surviving runs
			// here are true/false/null. Preserve the whole word (the rune-by-
			// rune path would truncate them to their first letter).
			j := i
			for j < n && isASCIILetter(s[j]) {
				j++
			}
			word := s[i:j]
			if word == "true" || word == "false" || word == "null" {
				if prev == '}' || prev == ']' || prev == '"' {
					b.WriteByte(',')
				}
				b.WriteString(word)
			}
			i = j
			prev = 0
		case c == '}' || c == ']':
			b.WriteByte(c)
			prev = rune(c)
			i++
		case c == ',' || c == ':':
			b.WriteByte(c)
			prev = 0
			i++
		default:
			// Whitespace: preserve but do not reset the pending value-closer
			// (it still applies once the next value-start token appears).
			if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
				b.WriteByte(c)
			}
			// Any other non-ASCII/structural junk was already removed by
			// stripStructuralGarbage; other ASCII punctuation here is dropped.
			i++
		}
	}
	return b.String()
}

func (f *Formater) ParseGraph(ctx context.Context, text string) (*types.GraphData, error) {
	matchData, err := f.parseOutput(ctx, text)
	if err != nil {
		return nil, err
	}
	if len(matchData) == 0 {
		logger.Debugf(ctx, "received empty extraction data.")
		return &types.GraphData{}, nil
	}
	// mm, _ := json.Marshal(matchData)
	// logger.Debugf(ctx, "Parsed graph data: %s", string(mm))

	var nodes []*types.GraphNode
	var relations []*types.GraphRelation

	for _, group := range matchData {
		switch {
		case group[f.nodePrefix] != nil:
			attributes := make([]string, 0)
			attributesKey := f.nodePrefix + f.attributeSuffix
			if attr, ok := group[attributesKey].([]interface{}); ok {
				for _, v := range attr {
					attributes = append(attributes, fmt.Sprintf("%v", v))
				}
			}
			nodes = append(nodes, &types.GraphNode{
				Name:       fmt.Sprintf("%v", group[f.nodePrefix]),
				Attributes: attributes,
			})
		case group[f.relationSource] != nil && group[f.relationTarget] != nil:
			relations = append(relations, &types.GraphRelation{
				Node1: fmt.Sprintf("%v", group[f.relationSource]),
				Node2: fmt.Sprintf("%v", group[f.relationTarget]),
				Type:  fmt.Sprintf("%v", group[f.relationPrefix]),
			})
		default:
			logger.Warnf(ctx, "Unsupported graph group: %v", group)
			continue
		}
	}
	graph := &types.GraphData{
		Node:     nodes,
		Relation: relations,
	}
	f.rebuildGraph(ctx, graph)
	return graph, nil
}

func (f *Formater) rebuildGraph(ctx context.Context, graph *types.GraphData) {
	nodeMap := make(map[string]*types.GraphNode)
	nodes := make([]*types.GraphNode, 0, len(graph.Node))
	for _, node := range graph.Node {
		if prenode, ok := nodeMap[node.Name]; ok {
			logger.Infof(ctx, "Duplicate node ID: %s, merge attribute", node.Name)
			// 修复panic：检查Attributes是否为nil
			if node.Attributes == nil {
				node.Attributes = make([]string, 0)
			}
			if prenode.Attributes != nil {
				node.Attributes = append(node.Attributes, prenode.Attributes...)
			}
			continue
		}
		nodeMap[node.Name] = node
		nodes = append(nodes, node)
	}

	relations := make([]*types.GraphRelation, 0, len(graph.Relation))
	for _, relation := range graph.Relation {
		if relation.Node1 == relation.Node2 {
			logger.Infof(ctx, "Duplicate relation, ignore it")
			continue
		}

		if _, ok := nodeMap[relation.Node1]; !ok {
			node := &types.GraphNode{Name: relation.Node1}
			nodes = append(nodes, node)
			nodeMap[relation.Node1] = node
			logger.Infof(ctx, "Add unknown source node ID: %s", relation.Node1)
		}
		if _, ok := nodeMap[relation.Node2]; !ok {
			node := &types.GraphNode{Name: relation.Node2}
			nodes = append(nodes, node)
			nodeMap[relation.Node2] = node
			logger.Infof(ctx, "Add unknown target node ID: %s", relation.Node2)
		}

		relations = append(relations, relation)
	}
	*graph = types.GraphData{
		Node:     nodes,
		Relation: relations,
	}
}

func (f *Formater) extractContent(ctx context.Context, text string) string {
	if !f.useFences {
		return strings.TrimSpace(text)
	}
	validTags := map[FormatType]map[string]struct{}{
		FormatTypeYAML: {"yaml": {}, "yml": {}},
		FormatTypeJSON: {"json": {}},
	}
	matches := _FENCE_RE.FindAllStringSubmatch(text, -1)
	var candidates []string
	for _, match := range matches {
		lang := match[1]
		body := match[2]
		if f.isValidLanguageTag(lang, validTags) {
			candidates = append(candidates, body)
		}
	}
	switch {
	case len(candidates) == 1:
		return strings.TrimSpace(candidates[0])

	case len(candidates) > 1:
		logger.Warnf(ctx, "multiple candidates found: %d", len(candidates))
		return strings.TrimSpace(candidates[0])

	case len(matches) == 1:
		logger.Debugf(ctx, "no candidate found, use first match without language tag: %s", matches[0][1])
		return strings.TrimSpace(matches[0][2])

	case len(matches) > 1:
		logger.Warnf(ctx, "multiple matches found: %d", len(matches))
		return strings.TrimSpace(matches[0][2])

	default:
		// Fallback strategies for cases where the fence regex fails to match.
		// This commonly happens when:
		//   1. The LLM output is truncated (no closing fence) — issue #1113 Pattern 3.
		//   2. The opening fence is malformed or surrounded by unexpected content,
		//      so the non-greedy regex falls back to the raw text — issue #1113 Pattern 1.
		// Without these fallbacks, the raw text (including backticks) is passed to
		// json.Unmarshal and fails with `invalid character '`'`.
		if extracted := stripFencesAndExtract(text, f.formatType); extracted != "" {
			logger.Debugf(ctx, "no fence match, recovered content via fallback (%d bytes)", len(extracted))
			return extracted
		}
		logger.Warnf(ctx, "no match found")
		return strings.TrimSpace(text)
	}
}

// stripFencesAndExtract attempts to recover a parseable payload from an LLM
// response when the strict fence regex fails. It handles three common cases:
//
//  1. Truncated responses with an opening ```lang fence but no closing fence
//     (LLM hit max_tokens mid-output).
//  2. Responses where the JSON/YAML body is preceded or followed by prose
//     and the fences are present but malformed.
//  3. Responses with no fences at all but a recognizable JSON object/array
//     embedded in surrounding text.
//
// It returns an empty string when no plausible payload can be recovered, so
// callers can fall back to their own behavior.
func stripFencesAndExtract(text string, format FormatType) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}

	// Case 1: opening fence present (with or without language tag) but no
	// matching closing fence. Take everything after the first fence and
	// strip any trailing backticks.
	if idx := strings.Index(trimmed, "```"); idx >= 0 {
		rest := trimmed[idx+3:]
		// Drop optional language tag on the same line.
		if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
			firstLine := strings.TrimSpace(rest[:nl])
			// A pure language tag is short and alphanumeric-ish.
			if firstLine == "" || isLikelyLanguageTag(firstLine) {
				rest = rest[nl+1:]
			}
		}
		// If there is a closing fence somewhere, cut at it.
		if end := strings.Index(rest, "```"); end >= 0 {
			rest = rest[:end]
		}
		rest = strings.TrimSpace(rest)
		rest = strings.Trim(rest, "`")
		rest = strings.TrimSpace(rest)
		if rest != "" {
			return rest
		}
	}

	// Case 2: no usable fence found, but the payload may still contain a
	// JSON object/array. Extract the outermost {...} or [...] substring.
	if format == FormatTypeJSON {
		if extracted := extractJSONLike(trimmed); extracted != "" {
			return extracted
		}
	}

	return ""
}

// isLikelyLanguageTag reports whether s looks like a markdown fence language
// tag (e.g. "json", "yaml", "yml", "go"). It must be short and contain only
// characters typical for a language identifier.
func isLikelyLanguageTag(s string) bool {
	if s == "" || len(s) > 16 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '+':
		default:
			return false
		}
	}
	return true
}

// extractJSONLike returns the outermost JSON object or array substring from s,
// or an empty string if none is found. It picks whichever bracket type appears
// first in the input, which mirrors what the LLM is most likely to have
// produced. The returned slice is not validated as JSON; callers must still
// json.Unmarshal it.
func extractJSONLike(s string) string {
	objStart := strings.IndexByte(s, '{')
	arrStart := strings.IndexByte(s, '[')
	var open, closeCh byte
	var start int
	switch {
	case objStart < 0 && arrStart < 0:
		return ""
	case objStart < 0:
		open, closeCh, start = '[', ']', arrStart
	case arrStart < 0:
		open, closeCh, start = '{', '}', objStart
	case objStart < arrStart:
		open, closeCh, start = '{', '}', objStart
	default:
		open, closeCh, start = '[', ']', arrStart
	}
	// Find matching close, respecting string literals so braces/brackets
	// inside JSON strings don't unbalance the count.
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch c {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case open:
			depth++
		case closeCh:
			depth--
			if depth == 0 {
				return strings.TrimSpace(s[start : i+1])
			}
		}
	}
	return ""
}

func (f *Formater) addFences(content string) string {
	content = strings.TrimSpace(content)
	return fmt.Sprintf("```%s\n%s\n```", f.formatType, content)
}

func (f *Formater) isValidLanguageTag(lang string, validTags map[FormatType]map[string]struct{}) bool {
	if lang == "" {
		return true
	}
	tag := strings.TrimSpace(strings.ToLower(lang))
	validSet, ok := validTags[f.formatType]
	if !ok {
		return false
	}
	_, exists := validSet[tag]
	return exists
}
