package event

// repo.go — EventsRepository for the "events" MongoDB collection.
// Stores ABI-decoded on-chain events after the consumer processes raw logs.

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
	"erc-8004-benchmarking-be/internal/utils"
)

// EventDocumentID returns the events collection _id: chainId:txHash:logIndex.
func EventDocumentID(chainID int64, txHash string, logIndex uint) string {
	return fmt.Sprintf("%d:%s:%d", chainID, strings.ToLower(txHash), logIndex)
}

// NewRepository returns a Repository bound to the named collection.
func NewRepository(db *mongodrv.Database, collectionName string) *Repository {
	m := mongorepo.NewMongoRepo[DecodedEvent](db, collectionName)
	return &Repository{MongoRepoImpl: *m}
}

// EnsureIndexes creates the required indexes on the events collection.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateMany(ctx, []mongodrv.IndexModel{
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "blockNumber", Value: 1},
				{Key: "logIndex", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("ux_chain_block_log"),
		},
		{
			Keys:    bson.D{{Key: "decodedAt", Value: 1}},
			Options: options.Index().SetName("idx_decoded_at"),
		},
	})
	return err
}

// BulkUpsert upserts a batch of decoded events using an unordered bulk write.
func (r *Repository) BulkUpsert(ctx context.Context, events []DecodedEvent) error {
	if len(events) == 0 {
		return nil
	}

	ops := make([]mongodrv.WriteModel, 0, len(events))
	for i := range events {
		events[i].ID = EventDocumentID(events[i].ChainID, events[i].TxHash, events[i].LogIndex)
		ev := events[i]
		filter := map[string]any{"_id": ev.ID}
		update := map[string]any{"$set": ev}
		ops = append(ops, mongodrv.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true))
	}

	_, err := r.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}

// ListIdentityURIEventsBeforeDecodedAt returns paginated identity URI events
// (Registered, URIUpdated) with decodedAt <= watermark, sorted ASC by (chainId, blockNumber, logIndex).
func (r *Repository) ListIdentityURIEventsBeforeDecodedAt(
	ctx context.Context,
	watermark int64,
	afterChainID int64,
	afterBlock uint64,
	afterLogIndex uint,
	limit int64,
) ([]DecodedEvent, error) {
	filter := bson.M{
		"contractType": "identity",
		"eventName": bson.M{
			"$in": []string{"Registered", "URIUpdated"},
		},
		"decodedAt": bson.M{"$lte": watermark},
		"$or": bson.A{
			bson.M{"chainId": bson.M{"$gt": afterChainID}},
			bson.M{
				"chainId":     afterChainID,
				"blockNumber": bson.M{"$gt": afterBlock},
			},
			bson.M{
				"chainId":     afterChainID,
				"blockNumber": afterBlock,
				"logIndex":    bson.M{"$gt": afterLogIndex},
			},
		},
	}
	opts := options.Find().
		SetSort(bson.D{
			{Key: "chainId", Value: 1},
			{Key: "blockNumber", Value: 1},
			{Key: "logIndex", Value: 1},
		}).
		SetLimit(limit)
	return r.Find(ctx, filter, opts)
}

// ListByChainAscending returns up to `limit` decoded events for a single chain,
// ordered from oldest to newest by (blockNumber, logIndex).
// If after is nil, the first page starts at the chain's earliest event.
// If after is non-nil, only events strictly after that (block, log) position are returned.
func (r *Repository) ListByChainAscending(
	ctx context.Context,
	chainID int64,
	after *ChainEventCursor,
	limit int64,
) ([]DecodedEvent, error) {
	return r.ListByChainBounded(ctx, chainID, after, nil, limit)
}

// ListByChainBounded returns up to `limit` decoded events for a single chain,
// strictly after `after` and at-or-before `upperBound` (inclusive).
// Either cursor may be nil (no lower / no upper bound).
func (r *Repository) ListByChainBounded(
	ctx context.Context,
	chainID int64,
	after *ChainEventCursor,
	upperBound *ChainEventCursor,
	limit int64,
) ([]DecodedEvent, error) {
	if limit < 1 {
		return nil, nil
	}
	filter := bson.M{"chainId": chainID}

	var conditions bson.A

	if after != nil {
		conditions = append(conditions,
			bson.M{"$or": bson.A{
				bson.M{"blockNumber": bson.M{"$gt": after.BlockNumber}},
				bson.M{
					"blockNumber": after.BlockNumber,
					"logIndex":    bson.M{"$gt": after.LogIndex},
				},
			}},
		)
	}

	if upperBound != nil {
		conditions = append(conditions,
			bson.M{"$or": bson.A{
				bson.M{"blockNumber": bson.M{"$lt": upperBound.BlockNumber}},
				bson.M{
					"blockNumber": upperBound.BlockNumber,
					"logIndex":    bson.M{"$lte": upperBound.LogIndex},
				},
			}},
		)
	}

	if len(conditions) > 0 {
		filter["$and"] = conditions
	}

	opts := options.Find().
		SetSort(bson.D{
			{Key: "blockNumber", Value: 1},
			{Key: "logIndex", Value: 1},
		}).
		SetLimit(limit)
	return r.Find(ctx, filter, opts)
}

// FindByTxHash returns all decoded events for a transaction (matches tx hash with/without 0x).
// Does not filter by chainId or agentId.
func (r *Repository) FindByTxHash(ctx context.Context, txHash string) ([]DecodedEvent, error) {
	alts := utils.TxHashMatchVariants(txHash)
	if len(alts) == 0 {
		return nil, fmt.Errorf("event repo: empty tx hash")
	}
	filter := bson.M{"txHash": bson.M{"$in": alts}}
	opts := options.Find().SetSort(bson.D{
		{Key: "chainId", Value: 1},
		{Key: "blockNumber", Value: 1},
		{Key: "logIndex", Value: 1},
	})
	docs, err := r.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("event repo: find by tx hash: %w", err)
	}
	return docs, nil
}
