package serviceendpoint

import (
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
)

type ServiceMeta struct {
	Name     string
	Endpoint string
}

// Normalize canonicalizes an endpoint for comparisons and map keys.
func Normalize(endpoint string) string {
	return strings.TrimRight(strings.ToLower(strings.TrimSpace(endpoint)), "/")
}

// Related reports whether two endpoints refer to the same service surface:
// equal after normalize, or one is a URL-prefix of the other at a path/query boundary.
func Related(feedbackEndpoint, serviceEndpoint string) bool {
	fb := Normalize(feedbackEndpoint)
	svc := Normalize(serviceEndpoint)
	if fb == "" || svc == "" {
		return false
	}
	if fb == svc {
		return true
	}
	return hasPrefixAtBoundary(fb, svc) || hasPrefixAtBoundary(svc, fb)
}

func hasPrefixAtBoundary(longer, shorter string) bool {
	if len(shorter) > len(longer) {
		return false
	}
	if !strings.HasPrefix(longer, shorter) {
		return false
	}
	if len(longer) == len(shorter) {
		return true
	}
	switch longer[len(shorter)] {
	case '/', '?', '#':
		return true
	default:
		return false
	}
}

// MatchService finds the declared service whose endpoint best matches feedbackEndpoint.
// When multiple services match, the longest normalized service endpoint wins (most specific).
func MatchService(services []agentrepo.RegistrationService, feedbackEndpoint string) (ServiceMeta, bool) {
	fb := Normalize(feedbackEndpoint)
	if fb == "" || len(services) == 0 {
		return ServiceMeta{}, false
	}
	var best ServiceMeta
	bestLen := -1
	for _, svc := range services {
		ep := strings.TrimSpace(svc.Endpoint)
		if ep == "" || !Related(feedbackEndpoint, ep) {
			continue
		}
		keyLen := len(Normalize(ep))
		if keyLen > bestLen {
			bestLen = keyLen
			best = ServiceMeta{
				Name:     strings.TrimSpace(svc.Name),
				Endpoint: ep,
			}
		}
	}
	return best, bestLen >= 0
}

// BuildIndex maps normalized service endpoints to metadata for O(1) accumulator lookup.
func BuildIndex(services []agentrepo.RegistrationService) map[string]ServiceMeta {
	if len(services) == 0 {
		return nil
	}
	out := make(map[string]ServiceMeta, len(services))
	for _, svc := range services {
		ep := strings.TrimSpace(svc.Endpoint)
		key := Normalize(ep)
		if key == "" {
			continue
		}
		if _, exists := out[key]; exists {
			continue
		}
		out[key] = ServiceMeta{
			Name:     strings.TrimSpace(svc.Name),
			Endpoint: ep,
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// EndpointRelatedFilter returns a MongoDB clause matching feedback endpoints related
// to the registered service endpoint (same rules as Related).
func EndpointRelatedFilter(serviceEndpoint string) bson.M {
	base := Normalize(serviceEndpoint)
	if base == "" {
		return nil
	}
	escaped := regexp.QuoteMeta(base)
	ors := bson.A{
		bson.M{"endpoint": bson.M{"$regex": "^" + escaped + "(/|\\?|#|$)", "$options": "i"}},
		bson.M{"endpoint": bson.M{"$regex": "^" + escaped + "$", "$options": "i"}},
	}
	for _, prefix := range boundaryPrefixes(base) {
		if prefix == base {
			continue
		}
		prefixEsc := regexp.QuoteMeta(prefix)
		ors = append(ors, bson.M{"endpoint": bson.M{"$regex": "^" + prefixEsc + "$", "$options": "i"}})
	}
	return bson.M{"$or": ors}
}

func boundaryPrefixes(norm string) []string {
	if norm == "" {
		return nil
	}
	parts := strings.Split(norm, "/")
	if len(parts) == 0 {
		return []string{norm}
	}
	var out []string
	var built strings.Builder
	for i, part := range parts {
		if i == 0 {
			built.WriteString(part)
		} else {
			built.WriteString("/")
			built.WriteString(part)
		}
		prefix := strings.TrimRight(built.String(), "/")
		if prefix != "" {
			out = append(out, prefix)
		}
	}
	return out
}
