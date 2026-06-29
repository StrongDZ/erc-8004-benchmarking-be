package wallet

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

// RatedRankingRow is one wallet address ranked by trust score (deduplicated across chains).
type RatedRankingRow struct {
	Address            string
	TrustScore         float64
	FeedbackTotalCount int64
}

// ListRatedRanking returns rated wallets sorted by trustScore descending.
// When chainIDs is non-empty, only wallet documents on those chains are considered.
// Addresses appearing on multiple chains are deduplicated by highest trustScore.
func (r *Repository) ListRatedRanking(ctx context.Context, chainIDs []int64, skip, limit int64) ([]RatedRankingRow, int64, error) {
	if skip < 0 {
		skip = 0
	}
	if limit <= 0 {
		limit = 50
	}

	match := bson.M{"trustRated": true}
	if len(chainIDs) > 0 {
		match["chainId"] = bson.M{"$in": chainIDs}
	}

	pipeline := bson.A{
		bson.M{"$match": match},
		// Normalize the address before grouping so legacy mixed-case rows dedupe
		// against their lowercase counterparts (addresses are persisted lowercase).
		bson.M{"$addFields": bson.M{"addrLower": bson.M{"$toLower": "$address"}}},
		bson.M{"$sort": bson.D{
			{Key: "addrLower", Value: 1},
			{Key: "trustScore", Value: -1},
		}},
		bson.M{"$group": bson.M{
			"_id":                "$addrLower",
			"trustScore":         bson.M{"$first": "$trustScore"},
			"feedbackTotalCount": bson.M{"$first": "$feedbackTotalCount"},
		}},
		bson.M{"$sort": bson.D{
			{Key: "trustScore", Value: -1},
			{Key: "_id", Value: 1},
		}},
		bson.M{"$facet": bson.M{
			"total": bson.A{bson.M{"$count": "n"}},
			"rows": bson.A{
				bson.M{"$skip": skip},
				bson.M{"$limit": limit},
			},
		}},
	}

	cur, err := r.Coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, 0, fmt.Errorf("wallet repo: list rated ranking: %w", err)
	}
	defer cur.Close(ctx)

	var facet []struct {
		Total []struct {
			N int64 `bson:"n"`
		} `bson:"total"`
		Rows []struct {
			Address            string  `bson:"_id"`
			TrustScore         float64 `bson:"trustScore"`
			FeedbackTotalCount int64   `bson:"feedbackTotalCount"`
		} `bson:"rows"`
	}
	if err := cur.All(ctx, &facet); err != nil {
		return nil, 0, fmt.Errorf("wallet repo: list rated ranking decode: %w", err)
	}
	if len(facet) == 0 {
		return nil, 0, nil
	}

	var total int64
	if len(facet[0].Total) > 0 {
		total = facet[0].Total[0].N
	}

	out := make([]RatedRankingRow, 0, len(facet[0].Rows))
	for _, row := range facet[0].Rows {
		out = append(out, RatedRankingRow{
			Address:            row.Address,
			TrustScore:         row.TrustScore,
			FeedbackTotalCount: row.FeedbackTotalCount,
		})
	}
	return out, total, nil
}
