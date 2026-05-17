package agent

// oasf.go — OASF capability query methods for agents collection.

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	mongodrv "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// OASFFilter holds filter criteria for OASF capability queries.
type OASFFilter struct {
	ChainID int64
	Skills  []string // normalized skill paths (AND logic if multiple)
	Domains []string // normalized domain paths (AND logic if multiple)
}

// OASFFacetNode represents a single node in the skill/domain tree with count.
type OASFFacetNode struct {
	Key      string `bson:"_id" json:"key"`           // full path like "a/b/c"
	Count    int64  `bson:"count" json:"count"`       // distinct agent count
	Name     string `bson:"-" json:"name"`            // leaf name (computed)
	ParentKey string `bson:"-" json:"parentKey,omitempty"` // parent path (computed)
}

// OASFFacetTree is the response for facet tree queries.
type OASFFacetTree struct {
	AllSkillsCount  int64            `json:"allSkillsCount"`
	AllDomainsCount int64            `json:"allDomainsCount"`
	SkillNodes      []OASFFacetNode  `json:"skillNodes"`
	DomainNodes     []OASFFacetNode  `json:"domainNodes"`
}

// FindByOASFCapabilities returns agents matching OASF skills/domains, sorted by score desc.
func (r *Repository) FindByOASFCapabilities(ctx context.Context, filter OASFFilter, limit, skip int64) ([]AgentDocument, error) {
	query := bson.M{}
	if filter.ChainID > 0 {
		query["chainId"] = filter.ChainID
	}

	// Skills filter (AND logic)
	if len(filter.Skills) > 0 {
		query["oasfSkills"] = bson.M{"$all": filter.Skills}
	}

	// Domains filter (AND logic)
	if len(filter.Domains) > 0 {
		query["oasfDomains"] = bson.M{"$all": filter.Domains}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "reputationScore", Value: -1}}).
		SetSkip(skip).
		SetLimit(limit)

	docs, err := r.Find(ctx, query, opts)
	if err != nil {
		return nil, fmt.Errorf("find by oasf capabilities: %w", err)
	}
	return docs, nil
}

// CountByOASFCapabilities counts agents matching OASF filter.
func (r *Repository) CountByOASFCapabilities(ctx context.Context, filter OASFFilter) (int64, error) {
	query := bson.M{}
	if filter.ChainID > 0 {
		query["chainId"] = filter.ChainID
	}

	if len(filter.Skills) > 0 {
		query["oasfSkills"] = bson.M{"$all": filter.Skills}
	}
	if len(filter.Domains) > 0 {
		query["oasfDomains"] = bson.M{"$all": filter.Domains}
	}

	count, err := r.Count(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("count by oasf capabilities: %w", err)
	}
	return count, nil
}

// GetOASFFacetTree returns the full facet tree for skills and domains with counts.
// This uses aggregation to build prefix nodes from leaf paths and count distinct agents.
func (r *Repository) GetOASFFacetTree(ctx context.Context, chainID int64, currentFilter OASFFilter) (*OASFFacetTree, error) {
	result := &OASFFacetTree{}

	// Count all agents with OASF
	allSkillsQuery := bson.M{"oasfSkills": bson.M{"$exists": true, "$ne": []any{}}}
	if chainID > 0 {
		allSkillsQuery["chainId"] = chainID
	}
	allSkillsCount, err := r.Count(ctx, allSkillsQuery)
	if err != nil {
		return nil, fmt.Errorf("count all skills: %w", err)
	}
	result.AllSkillsCount = allSkillsCount

	allDomainsQuery := bson.M{"oasfDomains": bson.M{"$exists": true, "$ne": []any{}}}
	if chainID > 0 {
		allDomainsQuery["chainId"] = chainID
	}
	allDomainsCount, err := r.Count(ctx, allDomainsQuery)
	if err != nil {
		return nil, fmt.Errorf("count all domains: %w", err)
	}
	result.AllDomainsCount = allDomainsCount

	// Build facet nodes for skills
	skillNodes, err := r.buildOASFFacetNodes(ctx, chainID, "oasfSkills", currentFilter.Domains)
	if err != nil {
		return nil, fmt.Errorf("build skill facet nodes: %w", err)
	}
	result.SkillNodes = enrichFacetNodes(skillNodes)

	// Build facet nodes for domains
	domainNodes, err := r.buildOASFFacetNodes(ctx, chainID, "oasfDomains", currentFilter.Skills)
	if err != nil {
		return nil, fmt.Errorf("build domain facet nodes: %w", err)
	}
	result.DomainNodes = enrichFacetNodes(domainNodes)

	return result, nil
}

// buildOASFFacetNodes builds facet nodes with counts for a given field (oasfSkills or oasfDomains).
// otherFieldFilter applies context filter from the opposite dimension (e.g., filter by domains when building skills tree).
func (r *Repository) buildOASFFacetNodes(ctx context.Context, chainID int64, fieldName string, otherFieldFilter []string) ([]OASFFacetNode, error) {
	matchStage := bson.M{
		fieldName: bson.M{"$exists": true, "$ne": []any{}},
	}
	if chainID > 0 {
		matchStage["chainId"] = chainID
	}

	// Apply context filter from other dimension
	var otherFieldName string
	if fieldName == "oasfSkills" {
		otherFieldName = "oasfDomains"
	} else {
		otherFieldName = "oasfSkills"
	}
	if len(otherFieldFilter) > 0 {
		matchStage[otherFieldName] = bson.M{"$all": otherFieldFilter}
	}

	pipeline := mongodrv.Pipeline{
		// Match agents with the field
		{{Key: "$match", Value: matchStage}},

		// Project agentId and leaf paths
		{{Key: "$project", Value: bson.M{
			"agentId": "$_id",
			"leaves":  "$" + fieldName,
		}}},

		// Unwind leaf paths
		{{Key: "$unwind", Value: "$leaves"}},

		// Generate all prefix nodes from each leaf
		{{Key: "$project", Value: bson.M{
			"agentId": 1,
			"prefixes": bson.M{
				"$function": bson.M{
					"body": `function(leafPath) {
						if (!leafPath) return [];
						const segments = leafPath.split('/');
						const prefixes = [];
						let current = '';
						for (let i = 0; i < segments.length; i++) {
							current = i === 0 ? segments[i] : current + '/' + segments[i];
							prefixes.push(current);
						}
						return prefixes;
					}`,
					"args": []any{"$leaves"},
					"lang": "js",
				},
			},
		}}},

		// Unwind prefixes
		{{Key: "$unwind", Value: "$prefixes"}},

		// Deduplicate (nodePath, agentId) pairs
		{{Key: "$group", Value: bson.M{
			"_id": bson.M{
				"node":  "$prefixes",
				"agent": "$agentId",
			},
		}}},

		// Count distinct agents per node
		{{Key: "$group", Value: bson.M{
			"_id":   "$_id.node",
			"count": bson.M{"$sum": 1},
		}}},

		// Sort by path
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	}

	cursor, err := r.Coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate facet nodes: %w", err)
	}
	defer cursor.Close(ctx)

	var nodes []OASFFacetNode
	if err := cursor.All(ctx, &nodes); err != nil {
		return nil, fmt.Errorf("decode facet nodes: %w", err)
	}

	return nodes, nil
}

// enrichFacetNodes adds name and parentKey to each node.
func enrichFacetNodes(nodes []OASFFacetNode) []OASFFacetNode {
	for i := range nodes {
		node := &nodes[i]
		segments := strings.Split(node.Key, "/")
		if len(segments) > 0 {
			node.Name = segments[len(segments)-1]
		}
		if len(segments) > 1 {
			node.ParentKey = strings.Join(segments[:len(segments)-1], "/")
		}
	}
	return nodes
}
