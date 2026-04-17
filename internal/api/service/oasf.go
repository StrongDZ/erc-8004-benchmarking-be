package service

// oasf.go — Services for /oasf/* endpoints.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
)

// OASFDeps bundles the repos used by the OASF service.
type OASFDeps struct {
	Agents  *agentrepo.Repository
	Formula scoring.FormulaConfig
}

// OASF encapsulates OASF discovery logic.
type OASF struct {
	deps OASFDeps
}

// NewOASF returns a new OASF service.
func NewOASF(deps OASFDeps) *OASF { return &OASF{deps: deps} }

// Facets implements /oasf/facets (§4.1).
func (s *OASF) Facets(ctx context.Context, chainID int64, skills, domains []string) (*dto.OASFFacetTree, error) {
	tree, err := s.deps.Agents.GetOASFFacetTree(ctx, chainID, agentrepo.OASFFilter{
		ChainID: chainID,
		Skills:  skills,
		Domains: domains,
	})
	if err != nil {
		return nil, fmt.Errorf("facets: %w", err)
	}
	out := &dto.OASFFacetTree{
		AllSkillsCount:  tree.AllSkillsCount,
		AllDomainsCount: tree.AllDomainsCount,
		SkillNodes:      buildFacetTree(mapFacetNodes(tree.SkillNodes)),
		DomainNodes:     buildFacetTree(mapFacetNodes(tree.DomainNodes)),
	}
	return out, nil
}

// BrowseParams are the inputs for /oasf/skills and /oasf/domains.
type BrowseParams struct {
	ChainID int64
	Skills  []string
	Domains []string
	Page    int
	Limit   int
	Skip    int64
}

// BrowseResult is the paginated response of top agents for an OASF filter.
type BrowseResult struct {
	Rows  []dto.AgentRow
	Total int64
	Page  int
	Limit int
}

// Browse implements /oasf/skills and /oasf/domains (§4.2, §4.3).
func (s *OASF) Browse(ctx context.Context, p BrowseParams) (*BrowseResult, error) {
	filter := agentrepo.OASFFilter{
		ChainID: p.ChainID,
		Skills:  p.Skills,
		Domains: p.Domains,
	}
	total, err := s.deps.Agents.CountByOASFCapabilities(ctx, filter)
	if err != nil {
		return nil, err
	}
	docs, err := s.deps.Agents.FindByOASFCapabilities(ctx, filter, int64(p.Limit), p.Skip)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	rows := make([]dto.AgentRow, 0, len(docs))
	for i, d := range docs {
		r := toAgentRow(d, s.deps.Formula, now)
		r.Rank = int(p.Skip) + i + 1
		rows = append(rows, r)
	}
	// Stable re-sort by decayed score so rank is monotonic with trustScore.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].TrustScore > rows[j].TrustScore })
	for i := range rows {
		rows[i].Rank = int(p.Skip) + i + 1
	}
	return &BrowseResult{Rows: rows, Total: total, Page: p.Page, Limit: p.Limit}, nil
}

func mapFacetNodes(in []agentrepo.OASFFacetNode) []dto.OASFFacetNode {
	out := make([]dto.OASFFacetNode, 0, len(in))
	for _, n := range in {
		key := strings.TrimSpace(n.Key)
		if key == "" {
			continue
		}
		out = append(out, dto.OASFFacetNode{
			Key:       key,
			Count:     n.Count,
			Name:      n.Name,
			ParentKey: n.ParentKey,
		})
	}
	return out
}

func buildFacetTree(nodes []dto.OASFFacetNode) []dto.OASFFacetNode {
	if len(nodes) == 0 {
		return nil
	}

	byParent := make(map[string][]dto.OASFFacetNode, len(nodes))
	nodeByKey := make(map[string]dto.OASFFacetNode, len(nodes))
	for _, n := range nodes {
		nodeByKey[n.Key] = n
		byParent[n.ParentKey] = append(byParent[n.ParentKey], n)
	}

	var attach func(parentKey string) []dto.OASFFacetNode
	attach = func(parentKey string) []dto.OASFFacetNode {
		children := byParent[parentKey]
		sort.SliceStable(children, func(i, j int) bool {
			return children[i].Key < children[j].Key
		})
		out := make([]dto.OASFFacetNode, 0, len(children))
		for _, child := range children {
			// Use the canonical node instance so we always attach children by key.
			node := nodeByKey[child.Key]
			node.Children = attach(node.Key)
			out = append(out, node)
		}
		return out
	}

	return attach("")
}
