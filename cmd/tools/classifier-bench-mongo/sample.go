package main

// sample.go — Sampling strategies for the classifier benchmark.
//
//   sampleByCategory:        N samples per classification.rule.category — used by
//                            Bench 1 (rule-as-gold over junk/quality/quantity).
//   sampleOthersStratified:  stratified pull from classification.rule.category="others",
//                            grouped client-side by (tag1Prefix, tag2Prefix) so the
//                            others gold-label set covers as many tag-shape clusters
//                            as possible instead of over-sampling a few hot patterns.

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// categoryMongoAliases maps canonical labels to Mongo classification.rule.category
// values. Runtime writes junk/quality/quantity/others; legacy rows may still use
// the 5-class keys — query with $in so Bench 1 and corpus sampling stay balanced.
var categoryMongoAliases = map[string][]string{
	"junk":     {"junk", "spam", "noise"},
	"quality":  {"quality", "service_feedback", "config_feedback"},
	"quantity": {"quantity", "app_specific"},
	"others":   {"others"},
}

// sampleByCategory uses $match + $sample to pull n random feedback records
// for a given classification.rule.category. Used by Bench 1 (rule-as-gold).
func sampleByCategory(ctx context.Context, repo *feedbackrepo.Repository, category string, n int) ([]feedbackrepo.FeedbackRecord, error) {
	if n <= 0 {
		return nil, nil
	}
	aliases, ok := categoryMongoAliases[category]
	if !ok {
		aliases = []string{category}
	}
	pipeline := []bson.D{
		{{Key: "$match", Value: bson.M{"classification.rule.category": bson.M{"$in": aliases}}}},
		{{Key: "$sample", Value: bson.M{"size": int64(n)}}},
	}
	cur, err := repo.Coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, fmt.Errorf("aggregate: %w", err)
	}
	defer cur.Close(ctx)

	var docs []feedbackrepo.FeedbackRecord
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return docs, nil
}

// sampleOthersStratified pulls all records with classification.rule.category="others",
// clusters them by (tag1Prefix, tag2Prefix), and takes up to perCluster random
// samples per cluster, capped at totalBudget overall. The seed is fixed so the
// same others gold-label set can be reproduced across runs.
//
// Rationale: a naive $sample would over-represent the most frequent others
// shapes (e.g. one agent's microtasking suffix). Stratified sampling guarantees
// rare shapes (validation+crypto-economic, miner-vouch, Ichi vault, dated period…)
// appear in the gold set so Bench 2 exercises every category fairly.
func sampleOthersStratified(
	ctx context.Context,
	repo *feedbackrepo.Repository,
	perCluster int,
	totalBudget int,
	seed int64,
) ([]feedbackrepo.FeedbackRecord, error) {
	if perCluster <= 0 {
		perCluster = 5
	}
	if totalBudget <= 0 {
		totalBudget = 200
	}

	cur, err := repo.Coll.Find(ctx, bson.M{"classification.rule.category": "others"})
	if err != nil {
		return nil, fmt.Errorf("find others: %w", err)
	}
	defer cur.Close(ctx)

	var all []feedbackrepo.FeedbackRecord
	if err := cur.All(ctx, &all); err != nil {
		return nil, fmt.Errorf("decode others: %w", err)
	}

	// Cluster by (tag1Prefix, tag2Prefix); empty prefix → "<EMPTY>".
	clusters := make(map[string][]int)
	for i, doc := range all {
		key := clusterKey(doc.Tag1, doc.Tag2)
		clusters[key] = append(clusters[key], i)
	}

	// Stable ordering of cluster keys so the rng draws the same indices each
	// time given the same seed.
	keys := make([]string, 0, len(clusters))
	for k := range clusters {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rng := rand.New(rand.NewSource(seed))
	picked := make(map[int]struct{}, totalBudget)
	out := make([]feedbackrepo.FeedbackRecord, 0, totalBudget)

	// Round-robin: ≤ perCluster per cluster, until budget hit.
	for round := 0; round < perCluster && len(out) < totalBudget; round++ {
		for _, k := range keys {
			if len(out) >= totalBudget {
				break
			}
			idxs := clusters[k]
			rng.Shuffle(len(idxs), func(i, j int) { idxs[i], idxs[j] = idxs[j], idxs[i] })
			for _, idx := range idxs {
				if _, seen := picked[idx]; seen {
					continue
				}
				picked[idx] = struct{}{}
				out = append(out, all[idx])
				break
			}
		}
	}

	return out, nil
}

// clusterKey builds a short signature for stratified sampling. Truncated to 12
// chars per tag and lowercased so case variants (e.g. CADm / cadm) land in the
// same bucket and casing alone does not inflate cluster count.
func clusterKey(tag1, tag2 string) string {
	return tagPrefix(tag1) + "::" + tagPrefix(tag2)
}

func tagPrefix(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "<EMPTY>"
	}
	r := []rune(s)
	if len(r) > 12 {
		r = r[:12]
	}
	return string(r)
}
