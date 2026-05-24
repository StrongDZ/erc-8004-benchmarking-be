package utils

import "strings"

// NormalizeAddress trims whitespace and lowercases an EVM address.
// Returns "" for empty/whitespace input.
//
// All address fields persisted to Mongo (wallets._id/address, agents.owner,
// agents.agentWallet, feedback_history.clientAddress, identity_history.owner,
// feedback_history.responses[*].responder) must be normalized through this
// function at the write boundary.
func NormalizeAddress(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}
