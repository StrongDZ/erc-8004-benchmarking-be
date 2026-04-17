package service

// chain.go — Services for /chains and /chains/:chainId/contracts.

import (
	"context"
	"errors"
	"fmt"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/api/dto"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"

	"go.mongodb.org/mongo-driver/bson"
)

// ChainDeps bundles the repos used by the chain service.
type ChainDeps struct {
	Contracts *configrepo.ContractsRepository
	Agents    *agentrepo.Repository
}

// Chain encapsulates chain metadata logic.
type Chain struct {
	deps ChainDeps
}

// NewChain returns a new Chain service.
func NewChain(deps ChainDeps) *Chain { return &Chain{deps: deps} }

// knownChainMeta covers the chains we officially support. Adding a new chain
// requires either extending this map or relying on the default "Chain <id>" fallback.
var knownChainMeta = map[int64]struct {
	Name, ShortName, NativeCurrency, BlockExplorer string
}{
	1:        {"Ethereum", "eth", "ETH", "https://etherscan.io"},
	8453:     {"Base", "base", "ETH", "https://basescan.org"},
	42161:    {"Arbitrum One", "arb1", "ETH", "https://arbiscan.io"},
	10:       {"Optimism", "oeth", "ETH", "https://optimistic.etherscan.io"},
	137:      {"Polygon", "matic", "MATIC", "https://polygonscan.com"},
	11155111: {"Sepolia", "sep", "ETH", "https://sepolia.etherscan.io"},
}

// List returns /chains payload (§5.1).
func (s *Chain) List(ctx context.Context) ([]dto.ChainInfo, error) {
	configs, err := s.deps.Contracts.FindActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("chains list: %w", err)
	}
	out := make([]dto.ChainInfo, 0, len(configs))
	for _, c := range configs {
		count, err := s.deps.Agents.Count(ctx, bson.M{"chainId": c.ChainID})
		if err != nil {
			return nil, fmt.Errorf("chains count agents: %w", err)
		}
		meta := knownChainMeta[c.ChainID]
		if meta.Name == "" {
			meta.Name = fmt.Sprintf("Chain %d", c.ChainID)
			meta.ShortName = fmt.Sprintf("chain-%d", c.ChainID)
		}
		out = append(out, dto.ChainInfo{
			ChainID:        c.ChainID,
			Name:           meta.Name,
			ShortName:      meta.ShortName,
			NativeCurrency: meta.NativeCurrency,
			BlockExplorer:  meta.BlockExplorer,
			AgentCount:     count,
		})
	}
	return out, nil
}

// Contracts returns /chains/:chainId/contracts payload (§5.2).
func (s *Chain) Contracts(ctx context.Context, chainID int64) ([]dto.ContractInfo, error) {
	cfg, err := s.deps.Contracts.FindOne(ctx, bson.M{"_id": configrepo.ContractsDocumentID(chainID)})
	if err != nil {
		if errors.Is(err, mongodrv.ErrNoDocuments) {
			return []dto.ContractInfo{}, nil
		}
		return nil, fmt.Errorf("chain contracts: %w", err)
	}
	out := make([]dto.ContractInfo, 0, len(cfg.Contracts))
	for _, c := range cfg.Contracts {
		out = append(out, dto.ContractInfo{
			Type:       string(c.Type),
			Address:    c.Address,
			StartBlock: c.StartBlock,
		})
	}
	return out, nil
}
