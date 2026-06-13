package serviceenrich

import (
	"strings"

	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
)

// a2aWellKnownAliases are well-known A2A agent card paths that agent operators
// use interchangeably for the same logical resource.
var a2aWellKnownAliases = [2]string{
	"/.well-known/agent-card.json",
	"/.well-known/agent.json",
}

// ResolveServiceRow finds the offchain_data row for a service's declared
// endpoint. If that row is missing or failed to fetch and the service is A2A,
// it falls back to the alias well-known agent-card path on the same origin.
func ResolveServiceRow(serviceName, endpoint string, rowMap map[string]*offchainrepo.OffchainData) *offchainrepo.OffchainData {
	row := rowMap[endpoint]
	if rowOK(row) {
		return row
	}
	if !strings.Contains(strings.ToLower(serviceName), "a2a") {
		return row
	}
	if alt, ok := a2aAliasURI(endpoint); ok {
		if altRow := rowMap[alt]; rowOK(altRow) {
			return altRow
		}
	}
	return row
}

func rowOK(row *offchainrepo.OffchainData) bool {
	return row != nil && row.Status == offchainrepo.StatusFetchedJSON
}

func a2aAliasURI(endpoint string) (string, bool) {
	a, b := a2aWellKnownAliases[0], a2aWellKnownAliases[1]
	if strings.HasSuffix(endpoint, a) {
		return strings.TrimSuffix(endpoint, a) + b, true
	}
	if strings.HasSuffix(endpoint, b) {
		return strings.TrimSuffix(endpoint, b) + a, true
	}
	return "", false
}
