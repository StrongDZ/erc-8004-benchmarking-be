package scoring

import "context"

// PublisherScoreProvider yields a normalized [0, 100] reputation score
// for a publisher (owner wallet address). implementations are pluggable
// so the wallet-reputation collection can be wired in later without
// touching callers.
type PublisherScoreProvider interface {
	// Score returns the publisher reputation and whether it is present.
	// present=false means "no publisher signal" — the composite drops the term.
	Score(ctx context.Context, owner string, chainID int64) (float64, bool)
}

// NeutralPublisherProvider always returns a fixed score (default 50).
// used as a placeholder until wallet reputation is implemented.
type NeutralPublisherProvider struct {
	Default float64
}

func (n NeutralPublisherProvider) Score(_ context.Context, _ string, _ int64) (float64, bool) {
	return n.Default, true
}
