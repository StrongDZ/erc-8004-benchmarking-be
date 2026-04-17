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

// MarkRevoked sets revokeTxHash on a feedback record.
func (r *Repository) MarkRevoked(ctx context.Context, chainID int64, agentID, clientAddress string, feedbackIndex uint64, revokeTxHash string) error {
	id := FeedbackDocumentID(chainID, agentID, clientAddress, feedbackIndex)
	filter := bson.M{"_id": id}
	update := bson.M{"$set": bson.M{
		"revokeTxHash": revokeTxHash,
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

// ListByAgent returns all feedback for an agent, ordered by blockNumber ASC.
func (r *Repository) ListByAgent(ctx context.Context, chainID int64, agentID string) ([]FeedbackRecord, error) {
	filter := bson.M{"chainId": chainID, "agentId": agentID}
	opts := options.Find().SetSort(bson.D{
		{Key: "blockNumber", Value: 1},
		{Key: "logIndex", Value: 1},
	})
	return r.Find(ctx, filter, opts)
}
