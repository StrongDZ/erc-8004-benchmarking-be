package identity

import (
	"testing"

	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
)

func makeDoc() *agentrepo.AgentDocument {
	return &agentrepo.AgentDocument{
		ChainID:     8453,
		AgentID:     "42",
		Owner:       "0xowner",
		AgentWallet: "0xwallet",
		AgentURI:    "ipfs://QmSame",
		Name:        "MyBot",
		Description: "does things",
		Image:       "https://img/logo.png",
		Domains:     []string{"finance.invest", "data.analysis"},
		OASFSkills:  []string{"forecast", "classify"},
		OASFDomains: []string{"d2", "d1"},
		Services: []agentrepo.RegistrationService{
			{Name: "a2a", Endpoint: "https://a", Version: "1"},
			{Name: "mcp", Endpoint: "https://m", Version: "2"},
		},
	}
}

func TestContentHash_Deterministic(t *testing.T) {
	a := makeDoc()
	b := makeDoc()
	b.Domains = []string{"data.analysis", "finance.invest"}
	b.OASFSkills = []string{"classify", "forecast"}
	b.OASFDomains = []string{"d1", "d2"}
	b.Services = []agentrepo.RegistrationService{
		{Name: "mcp", Endpoint: "https://m", Version: "2"},
		{Name: "a2a", Endpoint: "https://a", Version: "1"},
	}
	if ContentHash(a) != ContentHash(b) {
		t.Fatalf("hash should ignore slice order; a=%s b=%s", ContentHash(a), ContentHash(b))
	}
}

func TestContentHash_IgnoresIdentifiers(t *testing.T) {
	a := makeDoc()
	b := makeDoc()
	b.ChainID = 10
	b.AgentID = "999"
	b.Owner = "0xother"
	b.AgentWallet = "0xotherwallet"
	b.CreatedAt = 1700000000
	if ContentHash(a) != ContentHash(b) {
		t.Fatalf("hash must not depend on chainId/agentId/owner/agentWallet/createdAt/score")
	}
}

func TestContentHash_DetectsFieldChange(t *testing.T) {
	cases := []func(*agentrepo.AgentDocument){
		func(d *agentrepo.AgentDocument) { d.Name = "Different" },
		func(d *agentrepo.AgentDocument) { d.Description = "Different" },
		func(d *agentrepo.AgentDocument) { d.Image = "https://other/img.png" },
		func(d *agentrepo.AgentDocument) { d.AgentURI = "ipfs://QmOther" },
		func(d *agentrepo.AgentDocument) { d.Domains = append(d.Domains, "extra") },
		func(d *agentrepo.AgentDocument) { d.OASFSkills = []string{"only-one"} },
		func(d *agentrepo.AgentDocument) { d.OASFDomains = nil },
		func(d *agentrepo.AgentDocument) { d.Services[0].Endpoint = "https://changed" },
		func(d *agentrepo.AgentDocument) { d.Services[0].Version = "9" },
		func(d *agentrepo.AgentDocument) {
			d.Services = append(d.Services, agentrepo.RegistrationService{Name: "extra", Endpoint: "https://x"})
		},
	}
	base := ContentHash(makeDoc())
	for i, mutate := range cases {
		d := makeDoc()
		mutate(d)
		if ContentHash(d) == base {
			t.Errorf("case %d: hash did not change after mutation", i)
		}
	}
}

func TestContentHash_TrimsWhitespace(t *testing.T) {
	a := makeDoc()
	b := makeDoc()
	b.Name = "  MyBot  "
	b.Services[0].Endpoint = " https://a "
	if ContentHash(a) != ContentHash(b) {
		t.Fatalf("hash should trim whitespace on string fields and service strings")
	}
}
