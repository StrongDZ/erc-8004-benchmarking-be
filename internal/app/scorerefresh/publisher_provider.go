package scorerefresh

import (
	"context"
	"fmt"

	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
	"erc-8004-benchmarking-be/internal/utils"
)

// WalletTrustPublisherProvider maps an agent's owner to that owner wallet's propagated
// trustScore. present=false when the owner is unrated (not in the rated snapshot).
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
		return 0, false
	}
	return score, true
}
