package scorerefresh

// services_health.go — Bulk offchain status fetch for a batch of service endpoints.
// Performs a single Mongo query per agent batch rather than one per agent.

import (
	"context"

	"erc-8004-benchmarking-be/internal/repository/offchain"
)

// BulkFetchOffchainStatus returns a map[endpoint]offchainStatus for the
// given list of endpoint URIs. Uses one FindByURIs query keyed by SHA-256 of URI.
// Endpoints with no record return 0 (treat as unknown / not yet probed).
func BulkFetchOffchainStatus(
	ctx context.Context,
	offchainRepo *offchain.Repository,
	endpoints []string,
) (map[string]int, error) {
	result := make(map[string]int, len(endpoints))
	if len(endpoints) == 0 {
		return result, nil
	}

	// Deduplicate non-empty endpoints for the query.
	seen := make(map[string]struct{}, len(endpoints))
	unique := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		if ep == "" {
			continue
		}
		if _, ok := seen[ep]; ok {
			continue
		}
		seen[ep] = struct{}{}
		unique = append(unique, ep)
	}
	if len(unique) == 0 {
		return result, nil
	}

	// FindByURIs internally hashes to _id and runs a single $in query.
	docs, err := offchainRepo.FindByURIs(ctx, unique)
	if err != nil {
		return nil, err
	}

	// Build a map from URI → status using the URI field stored in each document.
	statusByURI := make(map[string]int, len(docs))
	for _, doc := range docs {
		statusByURI[doc.URI] = doc.Status
	}
	for _, ep := range unique {
		if s, ok := statusByURI[ep]; ok {
			result[ep] = s
		}
	}
	return result, nil
}
