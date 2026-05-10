package service

// chain.go — Services for /chains and /chains/:chainId/contracts.

import (
	"context"
	"errors"
	"fmt"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	"erc-8004-benchmarking-be/internal/api/dto"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"

	"go.mongodb.org/mongo-driver/bson"
)

// ChainDeps bundles the repos used by the chain service.
type ChainDeps struct {
	Contracts *contractsrepo.ContractsRepository
	Agents    *agentrepo.Repository
}

// Chain encapsulates chain listing and contracts logic.
type Chain struct {
	deps ChainDeps
}

// NewChain returns a new Chain service.
func NewChain(deps ChainDeps) *Chain { return &Chain{deps: deps} }

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
		out = append(out, dto.ChainInfo{
			ChainID:    c.ChainID,
			AgentCount: count,
		})
	}
	return out, nil
}

// Contracts returns /chains/:chainId/contracts payload (§5.2).
func (s *Chain) Contracts(ctx context.Context, chainID int64) ([]dto.ContractInfo, error) {
	cfg, err := s.deps.Contracts.FindOne(ctx, bson.M{"_id": contractsrepo.ContractsDocumentID(chainID)})
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
