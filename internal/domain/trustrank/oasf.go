package trustrank

// oasf.go — OASF skills/domains extraction and normalization utilities.

import (
	"strings"

	"erc-8004-benchmarking-be/internal/repository/agent"
)

// ExtractOASFCapabilities scans services for OASF and returns normalized skills + domains.
// Returns (skills, domains, hasOASF).
// This function is exported for use in migration scripts.
func ExtractOASFCapabilities(services []agent.RegistrationService) ([]string, []string, bool) {
	var allSkills []string
	var allDomains []string
	hasOASF := false

	for _, svc := range services {
		// Identify OASF service by name (case-insensitive)
		if !strings.EqualFold(svc.Name, "OASF") {
			continue
		}
		hasOASF = true

		// Extract and normalize skills
		for _, skill := range svc.Skills {
			normalized := normalizeOASFPath(skill)
			if normalized != "" {
				allSkills = append(allSkills, normalized)
			}
		}

		// Extract and normalize domains
		for _, domain := range svc.Domains {
			normalized := normalizeOASFPath(domain)
			if normalized != "" {
				allDomains = append(allDomains, normalized)
			}
		}
	}

	// Deduplicate
	allSkills = deduplicateStrings(allSkills)
	allDomains = deduplicateStrings(allDomains)

	return allSkills, allDomains, hasOASF
}

// normalizeOASFPath normalizes a skill or domain path:
// - trims space
// - converts to lowercase
// - collapses multiple slashes
// - removes leading/trailing slashes
func normalizeOASFPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	// Skip clearly invalid capability values leaked from malformed card payloads.
	// Examples: "map[...]" (object stringification), service endpoints (https://...).
	if strings.HasPrefix(path, "map[") || strings.Contains(path, "://") {
		return ""
	}

	// Lowercase
	path = strings.ToLower(path)

	// Collapse multiple slashes
	for strings.Contains(path, "//") {
		path = strings.ReplaceAll(path, "//", "/")
	}

	// Remove leading/trailing slashes
	path = strings.Trim(path, "/")

	return path
}

// deduplicateStrings removes duplicate entries while preserving order.
func deduplicateStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}

// expandPathPrefixes generates all parent prefixes from a leaf path.
// Example: "a/b/c" -> ["a", "a/b", "a/b/c"]
func expandPathPrefixes(leafPath string) []string {
	if leafPath == "" {
		return nil
	}
	segments := strings.Split(leafPath, "/")
	if len(segments) == 0 {
		return nil
	}

	prefixes := make([]string, 0, len(segments))
	current := ""
	for i, seg := range segments {
		if i == 0 {
			current = seg
		} else {
			current = current + "/" + seg
		}
		prefixes = append(prefixes, current)
	}
	return prefixes
}
