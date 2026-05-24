package main

// cmd/tools/mcp-server — MCP stdio server exposing ERC-8004 repository queries as Claude Code tools.
// Protocol: JSON-RPC 2.0 over stdin/stdout (newline-delimited).
// Start: go run ./cmd/tools/mcp-server  (reads .env from project root)

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/classifier"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ─── JSON-RPC types ───────────────────────────────────────────────────────────

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ─── MCP types ────────────────────────────────────────────────────────────────

type toolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type toolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type toolResult struct {
	Content []toolContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// ─── Server ───────────────────────────────────────────────────────────────────

// server routes each allowed collection name to the database that actually owns it.
// Raw indexer data (events, contracts, crawlers, offchain_data, config) lives in the
// primary DB; analyzed/scored data (agents, *_history) lives in the analyzed DB.
type server struct {
	viewDB           *mongo.Database
	allowCollections map[string]*mongo.Database
}

func main() {
	// MCP servers must not write to stdout before the protocol starts.
	// All logs go to stderr.
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime | log.Lshortfile)

	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("mcp: load config: %v", err)
	}

	mc, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("mcp: mongo connect: %v", err)
	}
	defer func() { _ = mc.Disconnect(context.Background()) }()

	// Stderr only (stdout is MCP JSON-RPC). Helps debug wrong host/port vs Docker.
	log.Printf("mcp: mongo ping ok primaryDB=%q analyzedDB=%q events=%q agents=%q",
		cfg.MongoDatabase, cfg.AnalyzedDatabase, cfg.EventsColl, cfg.AgentsColl)

	primaryDB := mc.Database(cfg.MongoDatabase)
	analyzedDB := mc.Database(cfg.AnalyzedDatabase)
	viewDB := mc.Database("view")

	srv := &server{
		viewDB: viewDB,
		allowCollections: map[string]*mongo.Database{
			cfg.ContractsColl:    primaryDB,
			cfg.CrawlersColl:     primaryDB,
			cfg.EventsColl:       primaryDB,
			cfg.OffchainColl:     primaryDB,
			cfg.ConfigColl:       primaryDB,
			cfg.AgentsColl:       analyzedDB,
			cfg.IdentityHistColl: analyzedDB,
			cfg.ScoreHistColl:    analyzedDB,
			cfg.FeedbackHistColl: analyzedDB,
			cfg.WalletColl:       analyzedDB,
		},
	}

	log.Printf("mcp: server started, listening on stdin")

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1 MB max line
	enc := json.NewEncoder(os.Stdout)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req rpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			writeError(enc, nil, -32700, "parse error")
			continue
		}

		// Notifications (no id) — no response needed.
		if req.ID == nil {
			continue
		}

		resp := srv.dispatch(ctx, req)
		if err := enc.Encode(resp); err != nil {
			log.Printf("mcp: encode response: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("mcp: stdin read error: %v", err)
	}
}

func (s *server) dispatch(ctx context.Context, req rpcRequest) rpcResponse {
	switch req.Method {
	case "initialize":
		return s.handleInitialize(req)
	case "tools/list":
		return s.handleToolsList(req)
	case "tools/call":
		return s.handleToolsCall(ctx, req)
	default:
		return errorResp(req.ID, -32601, "method not found: "+req.Method)
	}
}

// ─── initialize ──────────────────────────────────────────────────────────────

func (s *server) handleInitialize(req rpcRequest) rpcResponse {
	return rpcResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]any{
				"tools": map[string]any{},
			},
			"serverInfo": map[string]any{
				"name":    "erc8004-mcp",
				"version": "1.0.0",
			},
		},
	}
}

// ─── tools/list ──────────────────────────────────────────────────────────────

func (s *server) handleToolsList(req rpcRequest) rpcResponse {
	return rpcResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"tools": allTools(),
		},
	}
}

func allTools() []toolDef {
	return []toolDef{
		{
			Name:        "classify_feedback",
			Description: "Classify ERC-8004 feedback tags using the rule-based classifier. Returns category, confidence, and source.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"tag1":           map[string]any{"type": "string", "description": "tag1 from NewFeedback event (required)"},
					"tag2":           map[string]any{"type": "string", "description": "tag2 from NewFeedback event (optional)"},
					"value_raw":      map[string]any{"type": "string", "description": "raw uint256 value string (optional)"},
					"value_decimals": map[string]any{"type": "integer", "description": "decimal places 0–18 (optional, default 0)"},
				},
				"required": []string{"tag1"},
			},
		},
		{
			Name:        "query_collection_readonly",
			Description: "Generic read-only MongoDB query for allowed collections. Supports filter/projection/sort with pagination.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"collection": map[string]any{"type": "string", "description": "target collection name (must be allowed by server config)"},
					"filter":     map[string]any{"type": "object", "description": "MongoDB filter (read-only operators only)"},
					"projection": map[string]any{"type": "object", "description": "MongoDB projection fields (1 include, 0 exclude)"},
					"sort":       map[string]any{"type": "object", "description": "MongoDB sort document, e.g. {\"createdAt\":-1}"},
					"limit":      map[string]any{"type": "integer", "description": "max results (default 20, max 100)"},
					"skip":       map[string]any{"type": "integer", "description": "pagination offset (default 0, max 5000)"},
					"count":      map[string]any{"type": "boolean", "description": "include total document count for the filter"},
				},
				"required": []string{"collection"},
			},
		},
		{
			Name:        "aggregate_readonly",
			Description: "Read-only MongoDB aggregation on allowed collections. Pipeline limited to read stages: $match, $project, $group, $sort, $limit, $skip, $count, $unwind, $lookup, $addFields, $set, $facet, $sample, $bucket, $bucketAuto.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"collection": map[string]any{"type": "string", "description": "target collection (must be allowed)"},
					"pipeline":   map[string]any{"type": "array", "description": "aggregation pipeline (read-only stages only)"},
					"limit":      map[string]any{"type": "integer", "description": "max returned docs (default 50, max 500)"},
				},
				"required": []string{"collection", "pipeline"},
			},
		},
		{
			Name:        "tag_feedback_statistics_readonly",
			Description: "Read tag feedback statistics from view.TagFeedbackStatistics with count-based sorting and limit.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"sort_by_count": map[string]any{
						"type":        "string",
						"description": "sort direction by count: desc (default) or asc",
						"enum":        []string{"asc", "desc"},
					},
					"limit": map[string]any{
						"type":        "integer",
						"description": "max results (default 20, max 100)",
					},
				},
			},
		},
	}
}

// ─── tools/call ──────────────────────────────────────────────────────────────

type callParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

func (s *server) handleToolsCall(ctx context.Context, req rpcRequest) rpcResponse {
	var p callParams
	if err := json.Unmarshal(req.Params, &p); err != nil {
		return errorResp(req.ID, -32602, "invalid params: "+err.Error())
	}

	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var result toolResult
	var callErr error

	switch p.Name {
	case "classify_feedback":
		result, callErr = s.toolClassify(p.Arguments)
	case "query_collection_readonly":
		result, callErr = s.toolQueryCollectionReadonly(callCtx, p.Arguments)
	case "aggregate_readonly":
		result, callErr = s.toolAggregateReadonly(callCtx, p.Arguments)
	case "tag_feedback_statistics_readonly":
		result, callErr = s.toolTagFeedbackStatisticsReadonly(callCtx, p.Arguments)
	default:
		return errorResp(req.ID, -32601, "unknown tool: "+p.Name)
	}

	if callErr != nil {
		result = toolResult{
			IsError: true,
			Content: []toolContent{{Type: "text", Text: callErr.Error()}},
		}
	}

	return rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
}

// ─── Tool: classify_feedback ─────────────────────────────────────────────────

func (s *server) toolClassify(args map[string]any) (toolResult, error) {
	tag1 := strArg(args, "tag1")
	tag2 := strArg(args, "tag2")
	valueRaw := strArg(args, "value_raw")
	valueDecimals := intArg(args, "value_decimals")

	cls := classifier.Classify(tag1, tag2)
	isAnomalous := classifier.IsAnomalousValue(valueRaw, valueDecimals)
	vi := classifier.NormalizeValue(valueRaw, valueDecimals)

	out := fmt.Sprintf(
		"category:     %s\nconfidence:   %.2f\nsource:       %s\nnorm_tag:     %s\nvi (norm):    %.4f\nanomalous:    %v",
		cls.Category, cls.Confidence, cls.Source, cls.NormalizedTag, vi, isAnomalous,
	)
	return textResult(out), nil
}

// ─── Tool: query_collection_readonly ─────────────────────────────────────────

func (s *server) toolQueryCollectionReadonly(ctx context.Context, args map[string]any) (toolResult, error) {
	collection := strArg(args, "collection")
	if collection == "" {
		return toolResult{}, fmt.Errorf("collection is required")
	}
	if strings.HasPrefix(collection, "$") {
		return toolResult{}, fmt.Errorf("invalid collection name")
	}
	db, ok := s.allowCollections[collection]
	if !ok || db == nil {
		return toolResult{}, fmt.Errorf("collection %q is not allowed", collection)
	}

	filter, err := sanitizeFilter(docArg(args, "filter"))
	if err != nil {
		return toolResult{}, fmt.Errorf("invalid filter: %w", err)
	}
	projection, err := sanitizeProjection(docArg(args, "projection"))
	if err != nil {
		return toolResult{}, fmt.Errorf("invalid projection: %w", err)
	}
	sortDoc, err := sanitizeSort(docArg(args, "sort"))
	if err != nil {
		return toolResult{}, fmt.Errorf("invalid sort: %w", err)
	}

	limit := clampInt(intArg(args, "limit"), 1, 100, 20)
	skip := clampInt(intArg(args, "skip"), 0, 5000, 0)
	includeCount := boolArg(args, "count", false)

	findOptions := options.Find().SetLimit(int64(limit)).SetSkip(int64(skip))
	if len(projection) > 0 {
		findOptions.SetProjection(projection)
	}
	if len(sortDoc) > 0 {
		findOptions.SetSort(sortDoc)
	}

	coll := db.Collection(collection)
	cur, err := coll.Find(ctx, filter, findOptions)
	if err != nil {
		return toolResult{}, fmt.Errorf("query_collection_readonly find: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	var docs []bson.M
	if err := cur.All(ctx, &docs); err != nil {
		return toolResult{}, fmt.Errorf("query_collection_readonly decode: %w", err)
	}

	out := map[string]any{
		"database":   db.Name(),
		"collection": collection,
		"limit":      limit,
		"skip":       skip,
		"count":      len(docs),
		"documents":  docs,
	}
	if includeCount {
		total, err := coll.CountDocuments(ctx, filter)
		if err != nil {
			return toolResult{}, fmt.Errorf("query_collection_readonly count: %w", err)
		}
		out["total"] = total
	}

	b, _ := json.MarshalIndent(out, "", "  ")
	return textResult(string(b)), nil
}

// ─── Tool: aggregate_readonly ────────────────────────────────────────────────

var allowedAggregationStages = map[string]struct{}{
	"$match":      {},
	"$project":    {},
	"$group":      {},
	"$sort":       {},
	"$limit":      {},
	"$skip":       {},
	"$count":      {},
	"$unwind":     {},
	"$lookup":     {},
	"$addFields":  {},
	"$set":        {},
	"$facet":      {},
	"$sample":     {},
	"$bucket":     {},
	"$bucketAuto": {},
	"$replaceRoot": {},
	"$replaceWith": {},
	"$sortByCount": {},
	"$redact":      {},
}

func (s *server) toolAggregateReadonly(ctx context.Context, args map[string]any) (toolResult, error) {
	collection := strArg(args, "collection")
	if collection == "" {
		return toolResult{}, fmt.Errorf("collection is required")
	}
	if strings.HasPrefix(collection, "$") {
		return toolResult{}, fmt.Errorf("invalid collection name")
	}
	db, ok := s.allowCollections[collection]
	if !ok || db == nil {
		return toolResult{}, fmt.Errorf("collection %q is not allowed", collection)
	}

	rawPipeline, ok := args["pipeline"].([]any)
	if !ok {
		return toolResult{}, fmt.Errorf("pipeline must be an array")
	}
	if len(rawPipeline) == 0 {
		return toolResult{}, fmt.Errorf("pipeline must contain at least one stage")
	}
	if len(rawPipeline) > 20 {
		return toolResult{}, fmt.Errorf("pipeline too long: max 20 stages")
	}

	pipeline := make([]bson.M, 0, len(rawPipeline))
	for i, item := range rawPipeline {
		stage, ok := item.(map[string]any)
		if !ok {
			return toolResult{}, fmt.Errorf("pipeline stage %d must be an object", i)
		}
		if len(stage) != 1 {
			return toolResult{}, fmt.Errorf("pipeline stage %d must have exactly one operator", i)
		}
		for k := range stage {
			if _, allow := allowedAggregationStages[k]; !allow {
				return toolResult{}, fmt.Errorf("pipeline stage %d operator %q is not allowed", i, k)
			}
		}
		pipeline = append(pipeline, bson.M(stage))
	}

	limit := clampInt(intArg(args, "limit"), 1, 500, 50)
	pipeline = append(pipeline, bson.M{"$limit": int64(limit)})

	coll := db.Collection(collection)
	cur, err := coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return toolResult{}, fmt.Errorf("aggregate_readonly: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	var docs []bson.M
	if err := cur.All(ctx, &docs); err != nil {
		return toolResult{}, fmt.Errorf("aggregate_readonly decode: %w", err)
	}

	out := map[string]any{
		"database":   db.Name(),
		"collection": collection,
		"limit":      limit,
		"count":      len(docs),
		"documents":  docs,
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	return textResult(string(b)), nil
}

// ─── Tool: tag_feedback_statistics_readonly ──────────────────────────────────

func (s *server) toolTagFeedbackStatisticsReadonly(ctx context.Context, args map[string]any) (toolResult, error) {
	sortByCount := strings.ToLower(strings.TrimSpace(strArg(args, "sort_by_count")))
	sortDir := -1
	switch sortByCount {
	case "", "desc":
		sortDir = -1
	case "asc":
		sortDir = 1
	default:
		return toolResult{}, fmt.Errorf("sort_by_count must be 'asc' or 'desc'")
	}

	limit := clampInt(intArg(args, "limit"), 1, 100, 20)
	findOptions := options.Find().
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "count", Value: sortDir}})

	cur, err := s.viewDB.Collection("TagFeedbackStatistics").Find(ctx, bson.M{}, findOptions)
	if err != nil {
		return toolResult{}, fmt.Errorf("tag_feedback_statistics_readonly find: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	var docs []bson.M
	if err := cur.All(ctx, &docs); err != nil {
		return toolResult{}, fmt.Errorf("tag_feedback_statistics_readonly decode: %w", err)
	}

	out := map[string]any{
		"database":   "view",
		"collection": "TagFeedbackStatistics",
		"sort": map[string]any{
			"count": sortDir,
		},
		"limit":     limit,
		"count":     len(docs),
		"documents": docs,
	}

	b, _ := json.MarshalIndent(out, "", "  ")
	return textResult(string(b)), nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func writeError(enc *json.Encoder, id json.RawMessage, code int, msg string) {
	_ = enc.Encode(errorResp(id, code, msg))
}

func errorResp(id json.RawMessage, code int, msg string) rpcResponse {
	return rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &rpcError{Code: code, Message: msg},
	}
}

func textResult(text string) toolResult {
	return toolResult{Content: []toolContent{{Type: "text", Text: text}}}
}

func strArg(args map[string]any, key string) string {
	v, ok := args[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func intArg(args map[string]any, key string) int {
	v, ok := args[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

func boolArg(args map[string]any, key string, defaultVal bool) bool {
	v, ok := args[key]
	if !ok {
		return defaultVal
	}
	b, ok := v.(bool)
	if !ok {
		return defaultVal
	}
	return b
}

func strSliceArg(args map[string]any, key string) []string {
	v, ok := args[key]
	if !ok {
		return nil
	}
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func docArg(args map[string]any, key string) map[string]any {
	v, ok := args[key]
	if !ok {
		return nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	return m
}

func clampInt(v, min, max, def int) int {
	if v <= 0 {
		return def
	}
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func max0(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

var allowedReadOperators = map[string]struct{}{
	"$and":       {},
	"$or":        {},
	"$nor":       {},
	"$not":       {},
	"$in":        {},
	"$nin":       {},
	"$eq":        {},
	"$ne":        {},
	"$gt":        {},
	"$gte":       {},
	"$lt":        {},
	"$lte":       {},
	"$exists":    {},
	"$regex":     {},
	"$options":   {},
	"$size":      {},
	"$type":      {},
	"$all":       {},
	"$elemMatch": {},
	"$mod":       {},
}

func sanitizeFilter(in map[string]any) (bson.M, error) {
	if len(in) == 0 {
		return bson.M{}, nil
	}
	out := bson.M{}
	for k, v := range in {
		cleanV, err := sanitizeFilterValue(k, v)
		if err != nil {
			return nil, err
		}
		out[k] = cleanV
	}
	return out, nil
}

func sanitizeFilterValue(key string, v any) (any, error) {
	if strings.HasPrefix(key, "$") {
		if _, ok := allowedReadOperators[key]; !ok {
			return nil, fmt.Errorf("operator %q is not allowed", key)
		}
	}

	switch t := v.(type) {
	case map[string]any:
		nested := bson.M{}
		for nk, nv := range t {
			cv, err := sanitizeFilterValue(nk, nv)
			if err != nil {
				return nil, err
			}
			nested[nk] = cv
		}
		return nested, nil
	case []any:
		arr := make([]any, 0, len(t))
		for _, item := range t {
			cv, err := sanitizeFilterArrayValue(item)
			if err != nil {
				return nil, err
			}
			arr = append(arr, cv)
		}
		return arr, nil
	default:
		return v, nil
	}
}

func sanitizeFilterArrayValue(v any) (any, error) {
	switch t := v.(type) {
	case map[string]any:
		nested := bson.M{}
		for nk, nv := range t {
			cv, err := sanitizeFilterValue(nk, nv)
			if err != nil {
				return nil, err
			}
			nested[nk] = cv
		}
		return nested, nil
	case []any:
		out := make([]any, 0, len(t))
		for _, item := range t {
			cv, err := sanitizeFilterArrayValue(item)
			if err != nil {
				return nil, err
			}
			out = append(out, cv)
		}
		return out, nil
	default:
		return v, nil
	}
}

func sanitizeProjection(in map[string]any) (bson.M, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := bson.M{}
	for key, value := range in {
		if strings.HasPrefix(key, "$") {
			return nil, fmt.Errorf("projection key %q is not allowed", key)
		}
		switch t := value.(type) {
		case float64:
			if t != 0 && t != 1 {
				return nil, fmt.Errorf("projection value for %q must be 0 or 1", key)
			}
			out[key] = int(t)
		case int:
			if t != 0 && t != 1 {
				return nil, fmt.Errorf("projection value for %q must be 0 or 1", key)
			}
			out[key] = t
		case bool:
			if t {
				out[key] = 1
			} else {
				out[key] = 0
			}
		default:
			return nil, fmt.Errorf("projection value for %q must be 0/1 or boolean", key)
		}
	}
	return out, nil
}

func sanitizeSort(in map[string]any) (bson.D, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := bson.D{}
	for key, value := range in {
		if strings.HasPrefix(key, "$") {
			return nil, fmt.Errorf("sort key %q is not allowed", key)
		}
		dir, err := normalizeSortDirection(value)
		if err != nil {
			return nil, fmt.Errorf("sort %q: %w", key, err)
		}
		out = append(out, bson.E{Key: key, Value: dir})
	}
	return out, nil
}

func normalizeSortDirection(v any) (int, error) {
	switch t := v.(type) {
	case float64:
		if t == 1 || t == -1 {
			return int(t), nil
		}
	case int:
		if t == 1 || t == -1 {
			return t, nil
		}
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "asc", "1", "+1":
			return 1, nil
		case "desc", "-1":
			return -1, nil
		}
	}
	return 0, fmt.Errorf("direction must be 1/-1 or asc/desc")
}
