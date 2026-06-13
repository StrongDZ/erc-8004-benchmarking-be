package service

import (
	"context"
	"errors"
	"testing"

	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

// fakeOffchainRepo satisfies agentOffchainRepo for ReconnectServiceEndpoint tests.
// It records the last upsert call so tests can assert which branch ran.
type fakeOffchainRepo struct {
	lastUpsert string // "success" | "notjson" | "failure"
	lastURI    string
	lastErrMsg string
}

func (r *fakeOffchainRepo) HasSuccessfulFetch(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (r *fakeOffchainRepo) GetContent(_ context.Context, _ string) (string, bool, error) {
	return "", false, nil
}

func (r *fakeOffchainRepo) FindByURIs(_ context.Context, _ []string) ([]offchainrepo.OffchainData, error) {
	return nil, nil
}

func (r *fakeOffchainRepo) UpsertSuccess(_ context.Context, uri, _, _, _, _ string) error {
	r.lastUpsert, r.lastURI = "success", uri
	return nil
}

func (r *fakeOffchainRepo) UpsertFetchedNotJSON(_ context.Context, uri, _, _, _, _ string) error {
	r.lastUpsert, r.lastURI = "notjson", uri
	return nil
}

func (r *fakeOffchainRepo) UpsertFailure(_ context.Context, uri, _, _, _, errMsg string) error {
	r.lastUpsert, r.lastURI, r.lastErrMsg = "failure", uri, errMsg
	return nil
}

// fakeRawFetcher implements domainuri.RawFetcher with a canned response.
type fakeRawFetcher struct {
	body []byte
	err  error
}

func (f *fakeRawFetcher) Fetch(_ context.Context, _ string) ([]byte, error) {
	return f.body, f.err
}

func newReconnectAgent(repo *fakeAgentRepo, offchain *fakeOffchainRepo, fetcher domainuri.RawFetcher) *Agent {
	return &Agent{deps: AgentDeps{
		Agents:   repo,
		Offchain: offchain,
		Resolver: domainuri.NewResolver(fetcher, "https://ipfs.io/ipfs/", "https://arweave.net/"),
	}}
}

func TestReconnectServiceEndpoint_AgentNotFound(t *testing.T) {
	svc := newReconnectAgent(newFakeRepo(), &fakeOffchainRepo{}, &fakeRawFetcher{})
	_, err := svc.ReconnectServiceEndpoint(context.Background(), 1, "999", "https://example.com/agent.json")
	if !errors.Is(err, ErrAgentNotFound) {
		t.Fatalf("want ErrAgentNotFound, got %v", err)
	}
}

func TestReconnectServiceEndpoint_EndpointNotRegistered(t *testing.T) {
	repo := newFakeRepo()
	repo.add(agentrepo.AgentDocument{
		ChainID: 1, AgentID: "1",
		Services: []agentrepo.RegistrationService{{Name: "a2a", Endpoint: "https://example.com/a2a.json"}},
	})
	svc := newReconnectAgent(repo, &fakeOffchainRepo{}, &fakeRawFetcher{})

	_, err := svc.ReconnectServiceEndpoint(context.Background(), 1, "1", "https://other.com/x.json")
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("want ErrInvalidInput, got %v", err)
	}
}

func TestReconnectServiceEndpoint_SuccessJSON(t *testing.T) {
	repo := newFakeRepo()
	repo.add(agentrepo.AgentDocument{
		ChainID: 1, AgentID: "1",
		Services: []agentrepo.RegistrationService{{Name: "a2a", Endpoint: "https://example.com/a2a.json"}},
	})
	offchain := &fakeOffchainRepo{}
	svc := newReconnectAgent(repo, offchain, &fakeRawFetcher{body: []byte(`{"ok":true}`)})

	out, err := svc.ReconnectServiceEndpoint(context.Background(), 1, "1", "https://example.com/a2a.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Health != "ok" {
		t.Errorf("want health=ok, got %q (%s)", out.Health, out.HealthInfo)
	}
	if out.Name != "a2a" {
		t.Errorf("want name=a2a, got %q", out.Name)
	}
	if offchain.lastUpsert != "success" || offchain.lastURI != "https://example.com/a2a.json" {
		t.Errorf("want UpsertSuccess for endpoint, got %q %q", offchain.lastUpsert, offchain.lastURI)
	}
}

func TestReconnectServiceEndpoint_NonJSONForJSONRequiredService(t *testing.T) {
	repo := newFakeRepo()
	repo.add(agentrepo.AgentDocument{
		ChainID: 1, AgentID: "1",
		Services: []agentrepo.RegistrationService{{Name: "a2a", Endpoint: "https://example.com/a2a.json"}},
	})
	offchain := &fakeOffchainRepo{}
	svc := newReconnectAgent(repo, offchain, &fakeRawFetcher{body: []byte("not json")})

	out, err := svc.ReconnectServiceEndpoint(context.Background(), 1, "1", "https://example.com/a2a.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Health != "warning" {
		t.Errorf("want health=warning, got %q", out.Health)
	}
	if offchain.lastUpsert != "notjson" {
		t.Errorf("want UpsertFetchedNotJSON, got %q", offchain.lastUpsert)
	}
}

func TestReconnectServiceEndpoint_NonJSONForNonJSONService(t *testing.T) {
	repo := newFakeRepo()
	repo.add(agentrepo.AgentDocument{
		ChainID: 1, AgentID: "1",
		Services: []agentrepo.RegistrationService{{Name: "web", Endpoint: "https://example.com/page"}},
	})
	offchain := &fakeOffchainRepo{}
	svc := newReconnectAgent(repo, offchain, &fakeRawFetcher{body: []byte("<html></html>")})

	out, err := svc.ReconnectServiceEndpoint(context.Background(), 1, "1", "https://example.com/page")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Health != "ok" {
		t.Errorf("want health=ok for non-JSON-required service, got %q", out.Health)
	}
}

func TestReconnectServiceEndpoint_FetchError(t *testing.T) {
	repo := newFakeRepo()
	repo.add(agentrepo.AgentDocument{
		ChainID: 1, AgentID: "1",
		Services: []agentrepo.RegistrationService{{Name: "web", Endpoint: "https://example.com/down"}},
	})
	offchain := &fakeOffchainRepo{}
	svc := newReconnectAgent(repo, offchain, &fakeRawFetcher{err: errors.New("connection refused")})

	out, err := svc.ReconnectServiceEndpoint(context.Background(), 1, "1", "https://example.com/down")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Health != "fail" {
		t.Errorf("want health=fail, got %q", out.Health)
	}
	if offchain.lastUpsert != "failure" {
		t.Errorf("want UpsertFailure, got %q", offchain.lastUpsert)
	}
	if offchain.lastErrMsg == "" {
		t.Error("want non-empty fetchError recorded")
	}
}
