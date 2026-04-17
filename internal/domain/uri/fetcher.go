package uri

// fetcher.go — MetadataFetcher interface and composite Resolver.
// The Resolver dispatches to the correct strategy (data URI inline decode, IPFS via gateway, HTTP direct)
// based on URI scheme detected by DetectURIType.
//
// Thread-safe: Resolver holds no mutable state — safe for concurrent use across goroutines.

import (
	"context"
	"encoding/json"
	"fmt"
)

// RawFetcher performs a raw HTTP(S) GET and returns the response body.
// Implemented by wrapping internal/infra/https.Client; defined here at the consumer.
type RawFetcher interface {
	Fetch(ctx context.Context, url string) ([]byte, error)
}

// MetadataFetcher resolves any supported URI scheme to raw JSON bytes.
type MetadataFetcher interface {
	FetchMetadata(ctx context.Context, uri string) ([]byte, error)
}

// Resolver implements MetadataFetcher by composing:
//   - inline data URI decoding (no network)
//   - IPFS gateway redirect + HTTP fetch
//   - direct HTTP/HTTPS fetch
type Resolver struct {
	fetcher     RawFetcher
	ipfsGateway string
}

// NewResolver constructs a Resolver.
// fetcher is used for any URI type that requires an HTTP GET (IPFS via gateway, HTTPS).
// ipfsGateway is the base URL for IPFS content (e.g. "https://ipfs.io/ipfs/").
func NewResolver(fetcher RawFetcher, ipfsGateway string) *Resolver {
	return &Resolver{
		fetcher:     fetcher,
		ipfsGateway: ipfsGateway,
	}
}

// FetchMetadata resolves uri to raw bytes, dispatching based on URI scheme.
func (r *Resolver) FetchMetadata(ctx context.Context, uri string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	uriType := DetectURIType(uri)

	switch uriType {
	case URITypeDataURI:
		return r.fetchDataURI(uri)

	case URITypeIPFS:
		return r.fetchIPFS(ctx, uri)

	case URITypeHTTPS:
		return r.fetchHTTPS(ctx, uri)

	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedURIType, uri)
	}
}

func (r *Resolver) fetchDataURI(uri string) ([]byte, error) {
	data, err := ParseDataURI(uri)
	if err != nil {
		return nil, fmt.Errorf("uri resolver: parse data URI: %w", err)
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("uri resolver: data URI content is not valid JSON")
	}
	return data, nil
}

func (r *Resolver) fetchIPFS(ctx context.Context, uri string) ([]byte, error) {
	gatewayURL, err := IPFSToGatewayURL(uri, r.ipfsGateway)
	if err != nil {
		return nil, fmt.Errorf("uri resolver: %w", err)
	}
	body, err := r.fetcher.Fetch(ctx, gatewayURL)
	if err != nil {
		return nil, fmt.Errorf("uri resolver: ipfs gateway fetch: %w", err)
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("uri resolver: IPFS content is not valid JSON: cid=%s", uri)
	}
	return body, nil
}

func (r *Resolver) fetchHTTPS(ctx context.Context, uri string) ([]byte, error) {
	body, err := r.fetcher.Fetch(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("uri resolver: https fetch: %w", err)
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("uri resolver: HTTPS content is not valid JSON: url=%s", uri)
	}
	return body, nil
}

// Verify interface compliance at compile time.
var _ MetadataFetcher = (*Resolver)(nil)
