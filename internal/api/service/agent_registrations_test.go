package service

import (
	"context"
	"errors"
	"testing"

	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	scorestats "erc-8004-benchmarking-be/internal/repository/scorestats"

	mongodrv "go.mongodb.org/mongo-driver/mongo"
)

// fakeAgentRepo satisfies agentRepo for Registrations tests.
type fakeAgentRepo struct {
	byID     map[string]*agentrepo.AgentDocument // key: "{chainId}:{agentId}"
	byWallet map[string][]agentrepo.AgentDocument
	byOwner  map[string][]agentrepo.AgentDocument
}

func newFakeRepo() *fakeAgentRepo {
	return &fakeAgentRepo{
		byID:     map[string]*agentrepo.AgentDocument{},
		byWallet: map[string][]agentrepo.AgentDocument{},
		byOwner:  map[string][]agentrepo.AgentDocument{},
	}
}

func (r *fakeAgentRepo) add(doc agentrepo.AgentDocument) {
	key := agentrepo.AgentDocumentID(doc.ChainID, doc.AgentID)
	r.byID[key] = &doc
	if doc.AgentWallet != "" {
		r.byWallet[doc.AgentWallet] = append(r.byWallet[doc.AgentWallet], doc)
	}
	if doc.Owner != "" {
		r.byOwner[doc.Owner] = append(r.byOwner[doc.Owner], doc)
	}
}

func (r *fakeAgentRepo) FindByAgentID(_ context.Context, chainID int64, agentID string) (*agentrepo.AgentDocument, error) {
	key := agentrepo.AgentDocumentID(chainID, agentID)
	if d, ok := r.byID[key]; ok {
		return d, nil
	}
	return nil, mongodrv.ErrNoDocuments
}

func (r *fakeAgentRepo) FindByIDs(_ context.Context, _ int64, _ []string) ([]agentrepo.AgentDocument, error) {
	return nil, nil
}

func (r *fakeAgentRepo) FindRelated(_ context.Context, _ int64, _ string, _, _ []string, _ int64) ([]agentrepo.AgentDocument, error) {
	return nil, nil
}

func (r *fakeAgentRepo) FindByAgentWallet(_ context.Context, wallet string) ([]agentrepo.AgentDocument, error) {
	return r.byWallet[wallet], nil
}

func (r *fakeAgentRepo) FindByOwner(_ context.Context, owner string) ([]agentrepo.AgentDocument, error) {
	return r.byOwner[owner], nil
}

// fakeEmptyScoreStatsRepo satisfies agentScoreStatsRepo with no-op responses.
type fakeEmptyScoreStatsRepo struct{}

func (r *fakeEmptyScoreStatsRepo) FindByAgentID(_ context.Context, _ int64, _ string) (*scorestats.AgentScoreStats, error) {
	return nil, mongodrv.ErrNoDocuments
}

func newRegistrationsAgent(repo *fakeAgentRepo) *Agent {
	return &Agent{deps: AgentDeps{
		Agents:     repo,
		ScoreStats: &fakeEmptyScoreStatsRepo{},
	}}
}

// --- Tests ---

func TestRegistrations_NotFound(t *testing.T) {
	svc := newRegistrationsAgent(newFakeRepo())
	_, err := svc.Registrations(context.Background(), 1, "999")
	if !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("want ErrAgentNotFound, got %v", err)
	}
}

func TestRegistrations_MatchByAgentWallet(t *testing.T) {
	repo := newFakeRepo()
	wallet := "0xaaaa"
	repo.add(agentrepo.AgentDocument{ChainID: 1, AgentID: "1", Name: "A", AgentWallet: wallet, Active: true})
	repo.add(agentrepo.AgentDocument{ChainID: 10, AgentID: "2", Name: "B", AgentWallet: wallet, Active: false})

	svc := newRegistrationsAgent(repo)
	result, err := svc.Registrations(context.Background(), 1, "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.MatchedBy != "agentWallet" {
		t.Errorf("want matchedBy=agentWallet, got %q", result.MatchedBy)
	}
	if len(result.Registrations) != 2 {
		t.Fatalf("want 2 registrations, got %d", len(result.Registrations))
	}
	// First must be current.
	if !result.Registrations[0].IsCurrent {
		t.Error("first registration must be isCurrent=true")
	}
	if result.Registrations[0].ChainID != 1 {
		t.Errorf("want first chainId=1, got %d", result.Registrations[0].ChainID)
	}
	// Second is the other chain.
	if result.Registrations[1].ChainID != 10 {
		t.Errorf("want second chainId=10, got %d", result.Registrations[1].ChainID)
	}
	if result.Registrations[1].IsCurrent {
		t.Error("non-current registration must have isCurrent=false")
	}
}

func TestRegistrations_MatchByOwnerContentHash(t *testing.T) {
	repo := newFakeRepo()
	owner := "0xbbbb"
	// Same identity fields → same content hash.
	doc1 := agentrepo.AgentDocument{ChainID: 1, AgentID: "10", Name: "Bot", Owner: owner, Active: true}
	doc2 := agentrepo.AgentDocument{ChainID: 42161, AgentID: "20", Name: "Bot", Owner: owner, Active: true}
	// Different name → different hash → should NOT be included.
	doc3 := agentrepo.AgentDocument{ChainID: 137, AgentID: "30", Name: "OtherBot", Owner: owner, Active: true}
	repo.add(doc1)
	repo.add(doc2)
	repo.add(doc3)

	svc := newRegistrationsAgent(repo)
	result, err := svc.Registrations(context.Background(), 1, "10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.MatchedBy != "owner+contentHash" {
		t.Errorf("want matchedBy=owner+contentHash, got %q", result.MatchedBy)
	}
	if len(result.Registrations) != 2 {
		t.Fatalf("want 2 registrations (doc3 excluded by hash), got %d", len(result.Registrations))
	}
	// Should be sorted: current (chainId=1) first, then chainId=42161.
	if result.Registrations[0].ChainID != 1 || !result.Registrations[0].IsCurrent {
		t.Error("first must be current chain=1")
	}
	if result.Registrations[1].ChainID != 42161 {
		t.Errorf("want chainId=42161, got %d", result.Registrations[1].ChainID)
	}
}

func TestRegistrations_SelfOnly(t *testing.T) {
	repo := newFakeRepo()
	// Empty agentWallet AND empty owner → self path.
	repo.add(agentrepo.AgentDocument{ChainID: 8453, AgentID: "5", Name: "Solo", Active: true})

	svc := newRegistrationsAgent(repo)
	result, err := svc.Registrations(context.Background(), 8453, "5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.MatchedBy != "self" {
		t.Errorf("want matchedBy=self, got %q", result.MatchedBy)
	}
	if len(result.Registrations) != 1 {
		t.Fatalf("want 1 registration, got %d", len(result.Registrations))
	}
	if !result.Registrations[0].IsCurrent {
		t.Error("self-only registration must be isCurrent=true")
	}
}

func TestRegistrations_NormalizesAddress(t *testing.T) {
	repo := newFakeRepo()
	// Store wallet in lowercase (as normalizer does).
	wallet := "0xcccc"
	repo.add(agentrepo.AgentDocument{ChainID: 1, AgentID: "7", AgentWallet: wallet, Active: true})
	repo.add(agentrepo.AgentDocument{ChainID: 10, AgentID: "8", AgentWallet: wallet, Active: true})

	svc := newRegistrationsAgent(repo)
	result, err := svc.Registrations(context.Background(), 1, "7")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Registrations) != 2 {
		t.Fatalf("want 2 registrations, got %d", len(result.Registrations))
	}
}

func TestRegistrations_SortOrder(t *testing.T) {
	repo := newFakeRepo()
	wallet := "0xdddd"
	// Add in reverse order to verify sort.
	repo.add(agentrepo.AgentDocument{ChainID: 42161, AgentID: "3", AgentWallet: wallet, Active: false})
	repo.add(agentrepo.AgentDocument{ChainID: 10, AgentID: "2", AgentWallet: wallet, Active: true})
	repo.add(agentrepo.AgentDocument{ChainID: 1, AgentID: "1", AgentWallet: wallet, Active: true})

	svc := newRegistrationsAgent(repo)
	result, err := svc.Registrations(context.Background(), 10, "2") // current = chainId 10
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Registrations) != 3 {
		t.Fatalf("want 3 registrations, got %d", len(result.Registrations))
	}
	// first = current (chainId=10)
	if result.Registrations[0].ChainID != 10 || !result.Registrations[0].IsCurrent {
		t.Errorf("first must be current chainId=10, got chainId=%d isCurrent=%v", result.Registrations[0].ChainID, result.Registrations[0].IsCurrent)
	}
	// second = chainId=1 (ascending)
	if result.Registrations[1].ChainID != 1 {
		t.Errorf("second must be chainId=1, got %d", result.Registrations[1].ChainID)
	}
	// third = chainId=42161
	if result.Registrations[2].ChainID != 42161 {
		t.Errorf("third must be chainId=42161, got %d", result.Registrations[2].ChainID)
	}
}
