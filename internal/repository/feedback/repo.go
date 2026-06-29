package feedback

// repo.go — FeedbackHistoryRepository for the "feedback_history" MongoDB collection.
// Stores all feedback data (NewFeedback, FeedbackRevoked, ResponseAppended).

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// FeedbackDocumentID returns the feedback_history _id.
func FeedbackDocumentID(chainID int64, agentID, clientAddress string, feedbackIndex uint64) string {
	client := strings.ToLower(strings.TrimSpace(clientAddress))
	return fmt.Sprintf("%d:%s:%s:%d", chainID, agentID, client, feedbackIndex)
}

// NewRepository returns a Repository bound to the named collection.
func NewRepository(db *mongodrv.Database, collectionName string) *Repository {
	m := mongorepo.NewMongoRepo[FeedbackRecord](db, collectionName)
	return &Repository{MongoRepoImpl: *m}
}

// EnsureIndexes creates the required indexes on the feedback_history collection.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateMany(ctx, []mongodrv.IndexModel{
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "agentId", Value: 1},
				{Key: "clientAddress", Value: 1},
				{Key: "feedbackIndex", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("ux_chain_agent_client_fbidx"),
		},
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "agentId", Value: 1},
				{Key: "blockNumber", Value: 1},
				{Key: "logIndex", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_agent_block_log"),
		},
		// Allows category-aware queries and aggregations without collection scan.
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "category", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_category"),
		},
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "agentId", Value: 1},
				{Key: "category", Value: 1},
				{Key: "blockNumber", Value: 1},
				{Key: "logIndex", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_agent_category_block"),
		},
		// Partial index on revoked feedback for revocation timeline queries.
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "revokedAt", Value: -1},
			},
			Options: options.Index().
				SetName("idx_chain_revoked_at").
				SetPartialFilterExpression(bson.M{"isRevoked": true}),
		},
		// Index for wallet-centric queries (/wallet/:address/feedbacks).
		{
			Keys:    bson.D{{Key: "clientAddress", Value: 1}},
			Options: options.Index().SetName("idx_client_address"),
		},
		// Supports rescale worker Phase 1: fetch service feedbacks by tag1 before a timestamp.
		{
			Keys: bson.D{
				{Key: "tag1", Value: 1},
				{Key: "category", Value: 1},
				{Key: "isRevoked", Value: 1},
				{Key: "timestamp", Value: 1},
			},
			Options: options.Index().
				SetName("idx_tag1_category_revoked_ts").
				SetPartialFilterExpression(bson.M{
					"category":  "quality",
					"isRevoked": false,
				}),
		},
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "validationVerdict", Value: 1},
				{Key: "timestamp", Value: -1},
			},
			Options: options.Index().SetName("idx_chain_verdict_ts"),
		},
		{
			Keys: bson.D{
				{Key: "clientAddress", Value: 1},
				{Key: "chainId", Value: 1},
				{Key: "timestamp", Value: -1},
			},
			Options: options.Index().SetName("idx_client_chain_ts"),
		},
	})
	return err
}

// Upsert inserts or updates a feedback record using $set (upsert).
func (r *Repository) Upsert(ctx context.Context, doc FeedbackRecord) error {
	doc.ID = FeedbackDocumentID(doc.ChainID, doc.AgentID, doc.ClientAddress, doc.FeedbackIndex)
	filter := bson.M{"_id": doc.ID}
	update := bson.M{"$set": doc}
	_, err := r.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("feedback repo: upsert %s: %w", doc.ID, err)
	}
	return nil
}

// MarkRevoked sets isRevoked, revokedAt, and revokeTxHash on a feedback record.
func (r *Repository) MarkRevoked(ctx context.Context, chainID int64, agentID, clientAddress string, feedbackIndex uint64, revokeTxHash string, revokedAt int64) error {
	id := FeedbackDocumentID(chainID, agentID, clientAddress, feedbackIndex)
	filter := bson.M{"_id": id}
	update := bson.M{"$set": bson.M{
		"revokeTxHash": revokeTxHash,
		"isRevoked":    true,
		"revokedAt":    revokedAt,
	}}
	_, err := r.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("feedback repo: mark revoked %s: %w", id, err)
	}
	return nil
}

// AppendResponse appends a new response entry to the responses array using $push.
func (r *Repository) AppendResponse(ctx context.Context, chainID int64, agentID, clientAddress string, feedbackIndex uint64, response FeedbackResponse) error {
	id := FeedbackDocumentID(chainID, agentID, clientAddress, feedbackIndex)
	filter := bson.M{"_id": id}
	update := bson.M{"$push": bson.M{
		"responses": response,
	}}
	_, err := r.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("feedback repo: append response %s: %w", id, err)
	}
	return nil
}

// FindByAgentAndIndex returns a specific feedback record for one client submission index.
func (r *Repository) FindByAgentAndIndex(ctx context.Context, chainID int64, agentID, clientAddress string, feedbackIndex uint64) (*FeedbackRecord, error) {
	id := FeedbackDocumentID(chainID, agentID, clientAddress, feedbackIndex)
	doc, err := r.FindOne(ctx, bson.M{"_id": id})
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

// FindByIDs returns feedback records for the given document _ids (order not guaranteed).
func (r *Repository) FindByIDs(ctx context.Context, ids []string) ([]FeedbackRecord, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	docs, err := r.Find(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return nil, fmt.Errorf("feedback repo: find by ids: %w", err)
	}
	return docs, nil
}

// BulkUpsert upserts multiple feedback records using an unordered bulk write.
func (r *Repository) BulkUpsert(ctx context.Context, docs []FeedbackRecord) error {
	if len(docs) == 0 {
		return nil
	}
	ops := make([]mongodrv.WriteModel, 0, len(docs))
	for i := range docs {
		docs[i].ID = FeedbackDocumentID(docs[i].ChainID, docs[i].AgentID, docs[i].ClientAddress, docs[i].FeedbackIndex)
		filter := bson.M{"_id": docs[i].ID}
		update := bson.M{"$set": docs[i]}
		ops = append(ops, mongodrv.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true))
	}
	_, err := r.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("feedback repo: bulk upsert: %w", err)
	}
	return nil
}

// BulkUpdate applies partial updates to multiple feedback records using an unordered bulk write.
func (r *Repository) BulkUpdate(ctx context.Context, updates []FeedbackUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	ops := make([]mongodrv.WriteModel, 0, len(updates))
	for _, u := range updates {
		ops = append(ops, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": u.ID}).
			SetUpdate(u.Update))
	}
	_, err := r.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("feedback repo: bulk update: %w", err)
	}
	return nil
}

// ScaleUpdate carries the document ID, the new valueScale, and optionally the
// reclassified category/feature for BulkUpdateValueScale.
// NewCategory == "" means "leave category/feature unchanged".
type ScaleUpdate struct {
	ID          string
	NewScale    string
	NewCategory string // non-empty → also set category, feature, classification.rule.*
	NewFeature  string // paired with NewCategory; empty string is valid (clears feature)
}

// BulkUpdateValueScale sets ValueScale (and, when ScaleUpdate.NewCategory is set,
// category/feature/classification.rule.*) on multiple feedback documents.
// Called by the rescale worker Phase 3. The $set is idempotent — safe to retry.
func (r *Repository) BulkUpdateValueScale(ctx context.Context, updates []ScaleUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	ops := make([]mongodrv.WriteModel, 0, len(updates))
	for _, u := range updates {
		fields := bson.M{"valueScale": u.NewScale}
		if u.NewCategory != "" {
			fields["category"] = u.NewCategory
			fields["feature"] = u.NewFeature
			fields["classification.rule.category"] = u.NewCategory
			fields["classification.rule.feature"] = u.NewFeature
		}
		ops = append(ops, mongodrv.NewUpdateOneModel().
			SetFilter(bson.M{"_id": u.ID}).
			SetUpdate(bson.M{"$set": fields}))
	}
	_, err := r.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("feedback repo: bulk update value scale: %w", err)
	}
	return nil
}

// FindServiceFeedbacksByTagPair returns all non-revoked quality feedbacks for a (tag1, tag2) pair
// whose timestamp is before beforeUnix. Used by the rescale worker Phase 1 and 3.
func (r *Repository) FindServiceFeedbacksByTagPair(ctx context.Context, tag1, tag2 string, beforeUnix int64) ([]FeedbackRecord, error) {
	filter := bson.M{
		"tag1":           tag1,
		"tag2":           tag2,
		"category":       "quality",
		"isRevoked":      false,
		"isSelfFeedback": false,
		"timestamp":      bson.M{"$lt": beforeUnix},
	}
	opts := options.Find().SetSort(bson.D{{Key: "agentId", Value: 1}})
	docs, err := r.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("feedback repo: find service by tag pair %q:%q: %w", tag1, tag2, err)
	}
	return docs, nil
}

// ListByAgent returns all feedback for an agent, ordered by blockNumber ASC.
func (r *Repository) ListByAgent(ctx context.Context, chainID int64, agentID string) ([]FeedbackRecord, error) {
	filter := bson.M{"chainId": chainID, "agentId": agentID}
	opts := options.Find().SetSort(bson.D{
		{Key: "blockNumber", Value: 1},
		{Key: "logIndex", Value: 1},
	})
	return r.Find(ctx, filter, opts)
}

// FindByID fetches a single feedback record by its _id.
func (r *Repository) FindByID(ctx context.Context, id string) (*FeedbackRecord, error) {
	doc, err := r.FindOne(ctx, bson.M{"_id": id})
	if err != nil {
		return nil, fmt.Errorf("feedback repo: find by id %s: %w", id, err)
	}
	return &doc, nil
}

// FeedbackEdge is a lightweight struct for graph edge construction.
type FeedbackEdge struct {
	ChainID       int64   `bson:"chainId"`
	ClientAddress string  `bson:"clientAddress"`
	AgentID       string  `bson:"agentId"`
	Wi            float64 `bson:"wi"`
}

// FeedbackBackfillItem is the minimal projection needed to re-publish a feedback
// into the feedback-others queue for async grading.
type FeedbackBackfillItem struct {
	ID      string `bson:"_id"`
	ChainID int64  `bson:"chainId"`
}

// ListUnprocessedIDs returns feedbacks where validationVerdict is missing or empty
// (i.e. grading by trustrank/feedback-others has not yet completed). Pass chainID=0 for all chains.
// limit caps the batch size; callers should pick a value that the queue can absorb.
// Results are stable-sorted by _id so repeated calls page deterministically.
//
// Intended for manual backfill tools only — no worker should call this on a periodic schedule.
func (r *Repository) ListUnprocessedIDs(ctx context.Context, chainID int64, limit int) ([]FeedbackBackfillItem, error) {
	if limit <= 0 {
		return nil, nil
	}
	filter := bson.M{
		"validationVerdict": bson.M{"$in": bson.A{nil, ""}},
	}
	if chainID > 0 {
		filter["chainId"] = chainID
	}
	cur, err := r.Coll.Find(ctx, filter, options.Find().
		SetProjection(bson.M{"_id": 1, "chainId": 1}).
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(int64(limit)).
		SetBatchSize(int32(limit)),
	)
	if err != nil {
		return nil, fmt.Errorf("feedback: list unprocessed ids: %w", err)
	}
	defer cur.Close(ctx)
	var out []FeedbackBackfillItem
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("feedback: decode unprocessed ids: %w", err)
	}
	return out, nil
}

// ScanAllNonJunkEdges returns feedback edges that are not explicitly rejected (junk/missing/self).
// This includes verdict="valid", verdict="" (unprocessed), and verdict="legacy".
// When wi=0 (not yet graded by trustrank or feedback-others), the caller should supply a default weight.
func (r *Repository) ScanAllNonJunkEdges(ctx context.Context, chainID int64) ([]FeedbackEdge, error) {
	filter := bson.M{
		"validationVerdict": bson.M{"$nin": []string{"junk", "missing_fields", "self"}},
	}
	if chainID > 0 {
		filter["chainId"] = chainID
	}
	cur, err := r.Coll.Find(ctx, filter, options.Find().
		SetProjection(bson.M{
			"chainId": 1, "clientAddress": 1, "agentId": 1, "wi": 1, "_id": 0,
		}).
		SetBatchSize(10000),
	)
	if err != nil {
		return nil, fmt.Errorf("feedback: scan non-junk edges: %w", err)
	}
	defer cur.Close(ctx)
	var out []FeedbackEdge
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("feedback: decode non-junk edges: %w", err)
	}
	return out, nil
}

// ScanValidEdges returns all valid feedback edges (validationVerdict="valid", wi > 0).
// Pass chainID=0 for all chains.
func (r *Repository) ScanValidEdges(ctx context.Context, chainID int64) ([]FeedbackEdge, error) {
	filter := bson.M{
		"validationVerdict": "valid",
		"wi":                bson.M{"$gt": 0},
	}
	if chainID > 0 {
		filter["chainId"] = chainID
	}
	cur, err := r.Coll.Find(ctx, filter, options.Find().
		SetProjection(bson.M{
			"chainId": 1, "clientAddress": 1, "agentId": 1, "wi": 1, "_id": 0,
		}).
		SetBatchSize(10000),
	)
	if err != nil {
		return nil, fmt.Errorf("feedback: scan valid edges: %w", err)
	}
	defer cur.Close(ctx)

	var out []FeedbackEdge
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("feedback: decode valid edges: %w", err)
	}
	return out, nil
}
