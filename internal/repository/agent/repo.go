package agent

// repo.go — AgentRepository for the "agents" MongoDB collection.
// Stores the current snapshot of each agent: on-chain identity + O(1) scoring fields.

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongorepo "erc-8004-benchmarking-be/internal/repository"
)

// AgentDocumentID returns the agents collection _id: {chainId}:{agentId}.
func AgentDocumentID(chainID int64, agentID string) string {
	return fmt.Sprintf("%d:%s", chainID, agentID)
}

// NewRepository returns a Repository bound to the named collection. The statsCollectionName
// is used for queries that join scoring data from agent_score_stats (sort by composite,
// median, top-10 averages, etc).
func NewRepository(db *mongodrv.Database, collectionName, statsCollectionName string) *Repository {
	m := mongorepo.NewMongoRepo[AgentDocument](db, collectionName)
	return &Repository{
		MongoRepoImpl: *m,
		StatsColl:     db.Collection(statsCollectionName),
	}
}

// EnsureIndexes creates the required indexes on the agents collection.
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.Indexes().CreateMany(ctx, []mongodrv.IndexModel{
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "agentId", Value: 1},
			},
			Options: options.Index().SetUnique(true).SetName("ux_chain_agent"),
		},
		// OASF capability indexes (scoring fields now live on agent_score_stats — sort joins happen there).
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "hasOASF", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_has_oasf"),
		},

		// Compound index for skill filtering
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "oasfSkills", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_oasf_skills"),
		},
		// Compound index for domain filtering
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "oasfDomains", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_oasf_domains"),
		},
		// Multikey index on denormalized tags (from onchainMetadata.tags).
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "tags", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_tags"),
		},
		// Multikey index on services.name for service type filtering.
		{
			Keys: bson.D{
				{Key: "chainId", Value: 1},
				{Key: "services.name", Value: 1},
			},
			Options: options.Index().SetName("idx_chain_service_name"),
		},
		// Index for owner-based leaderboard filtering (/leaderboard?owner=).
		{
			Keys:    bson.D{{Key: "owner", Value: 1}},
			Options: options.Index().SetName("idx_owner"),
		},
	})
	return err
}

// Upsert inserts or updates an agent document using $set + $setOnInsert for createdAt.
func (r *Repository) Upsert(ctx context.Context, doc AgentDocument) error {
	doc.ID = AgentDocumentID(doc.ChainID, doc.AgentID)

	filter := bson.M{"_id": doc.ID}
	// Do not $set createdAt alongside $setOnInsert{createdAt}: same path → MongoDB conflict.
	setDoc := doc
	setDoc.CreatedAt = 0
	update := bson.M{
		"$set":         setDoc,
		"$setOnInsert": bson.M{"createdAt": doc.CreatedAt},
	}
	_, err := r.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("agent repo: upsert %s: %w", doc.ID, err)
	}
	return nil
}

// FindByAgentID returns the agent for a given chain + agentId, or ErrNoDocuments.
func (r *Repository) FindByAgentID(ctx context.Context, chainID int64, agentID string) (*AgentDocument, error) {
	id := AgentDocumentID(chainID, agentID)
	doc, err := r.FindOne(ctx, bson.M{"_id": id})
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

// FindAllByChain returns all agents for a chain, ordered by _id ascending.
func (r *Repository) FindAllByChain(ctx context.Context, chainID int64) ([]AgentDocument, error) {
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	return r.Find(ctx, bson.M{"chainId": chainID}, opts)
}

// UpdateIdentity atomically updates denormalized registration file fields on the agent document.
func (r *Repository) UpdateIdentity(ctx context.Context, chainID int64, agentID string, id IdentityFields) error {
	docID := AgentDocumentID(chainID, agentID)
	filter := bson.M{"_id": docID}
	update := bson.M{"$set": bson.M{
		"agentURI":       id.AgentURI,
		"name":           id.Name,
		"type":           id.Type,
		"image":          id.Image,
		"domains":        id.Domains,
		"description":    id.Description,
		"services":       id.Services,
		"active":         id.Active,
		"supportedTrust": id.SupportedTrust,
		"x402Support":    id.X402Support,
	}}
	_, err := r.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("agent repo: update identity %s: %w", docID, err)
	}
	return nil
}

// FindByIDs returns agents for a chain matching any of the given agentIDs (order not guaranteed).
func (r *Repository) FindByIDs(ctx context.Context, chainID int64, agentIDs []string) ([]AgentDocument, error) {
	if len(agentIDs) == 0 {
		return nil, nil
	}
	docs, err := r.Find(ctx, bson.M{"chainId": chainID, "agentId": bson.M{"$in": agentIDs}})
	if err != nil {
		return nil, fmt.Errorf("agent repo: find by ids: %w", err)
	}
	return docs, nil
}

// BulkUpsert upserts multiple agent documents using an unordered bulk write.
func (r *Repository) BulkUpsert(ctx context.Context, docs []*AgentDocument) error {
	if len(docs) == 0 {
		return nil
	}
	ops := make([]mongodrv.WriteModel, 0, len(docs))
	for _, doc := range docs {
		doc.ID = AgentDocumentID(doc.ChainID, doc.AgentID)
		filter := bson.M{"_id": doc.ID}
		// Omit createdAt from $set so it does not conflict with $setOnInsert on insert.
		setDoc := *doc
		setDoc.CreatedAt = 0
		update := bson.M{
			"$set":         setDoc,
			"$setOnInsert": bson.M{"createdAt": doc.CreatedAt},
		}
		ops = append(ops, mongodrv.NewUpdateOneModel().
			SetFilter(filter).
			SetUpdate(update).
			SetUpsert(true))
	}
	_, err := r.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return fmt.Errorf("agent repo: bulk upsert: %w", err)
	}
	return nil
}

// FindAll returns all agents across all chains (for decay cron). Paginated by skip/limit.
func (r *Repository) FindAll(ctx context.Context, skip, limit int64) ([]AgentDocument, error) {
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetSkip(skip).SetLimit(limit)
	return r.Find(ctx, bson.M{}, opts)
}
