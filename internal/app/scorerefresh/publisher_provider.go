package scorerefresh

import (
	"context"
	"fmt"

	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
	"erc-8004-benchmarking-be/internal/utils"
)

// WalletTrustPublisherProvider maps an agent's owner to that owner wallet's WalletTrust score.
// Falls back to 50 (neutral default) for owners not yet in the rated snapshot.
type WalletTrustPublisherProvider struct {
	byID map[string]float64 // key = wallet _id "<chainID>:<lower(owner)>"
}

// NewWalletTrustPublisherProvider builds the provider from a rated-trust snapshot.
func NewWalletTrustPublisherProvider(rated []walletrepo.RatedTrust) WalletTrustPublisherProvider {
	m := make(map[string]float64, len(rated))
	for _, rt := range rated {
		m[rt.ID] = rt.TrustScore
	}
	return WalletTrustPublisherProvider{byID: m}
}

func (p WalletTrustPublisherProvider) Score(_ context.Context, owner string, chainID int64) (float64, bool) {
	score, ok := p.byID[fmt.Sprintf("%d:%s", chainID, utils.NormalizeAddress(owner))]
	if !ok {
		return 50.0, true // neutral default for owners not yet rated
	}
	return score, true
}
