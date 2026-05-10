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
//   - Arweave gateway redirect + HTTP fetch
type Resolver struct {
	fetcher         RawFetcher
	ipfsGateway     string
	arweaveGateway  string
}

// NewResolver constructs a Resolver.
// fetcher is used for any URI type that requires an HTTP GET (IPFS via gateway, HTTPS, Arweave).
// ipfsGateway is the base URL for IPFS content (e.g. "https://ipfs.io/ipfs/").
// arweaveGateway is the base URL for Arweave content (e.g. "https://arweave.net/").
func NewResolver(fetcher RawFetcher, ipfsGateway string, arweaveGateway string) *Resolver {
	return &Resolver{
		fetcher:        fetcher,
		ipfsGateway:    ipfsGateway,
		arweaveGateway: arweaveGateway,
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

	case URITypeArweave:
		return r.fetchArweave(ctx, uri)

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

func (r *Resolver) fetchArweave(ctx context.Context, uri string) ([]byte, error) {
	gatewayURL, err := ArweaveToGatewayURL(uri, r.arweaveGateway)
	if err != nil {
		return nil, fmt.Errorf("uri resolver: %w", err)
	}
	body, err := r.fetcher.Fetch(ctx, gatewayURL)
	if err != nil {
		return nil, fmt.Errorf("uri resolver: arweave gateway fetch: %w", err)
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("uri resolver: Arweave content is not valid JSON: txid=%s", uri)
	}
	return body, nil
}

// FetchRaw fetches uri and returns the raw bytes without validating JSON.
// The bool return indicates whether the content parses as valid JSON.
// error is non-nil only on network/HTTP failures (status=-1 case).
// For unsupported URI types the error is also non-nil (treat as fetch failure).
//
// Use this when you want to distinguish between fetch failure and non-JSON content.
func (r *Resolver) FetchRaw(ctx context.Context, uri string) (body []byte, isJSON bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	uriType := DetectURIType(uri)

	switch uriType {
	case URITypeDataURI:
		data, parseErr := ParseDataURI(uri)
		if parseErr != nil {
			return nil, false, fmt.Errorf("uri resolver: parse data URI: %w", parseErr)
		}
		return data, json.Valid(data), nil

	case URITypeIPFS:
		gatewayURL, urlErr := IPFSToGatewayURL(uri, r.ipfsGateway)
		if urlErr != nil {
			return nil, false, fmt.Errorf("uri resolver: %w", urlErr)
		}
		data, fetchErr := r.fetcher.Fetch(ctx, gatewayURL)
		if fetchErr != nil {
			return nil, false, fmt.Errorf("uri resolver: ipfs gateway fetch: %w", fetchErr)
		}
		return data, json.Valid(data), nil

	case URITypeHTTPS:
		data, fetchErr := r.fetcher.Fetch(ctx, uri)
		if fetchErr != nil {
			return nil, false, fmt.Errorf("uri resolver: https fetch: %w", fetchErr)
		}
		return data, json.Valid(data), nil

	case URITypeArweave:
		gatewayURL, urlErr := ArweaveToGatewayURL(uri, r.arweaveGateway)
		if urlErr != nil {
			return nil, false, fmt.Errorf("uri resolver: %w", urlErr)
		}
		data, fetchErr := r.fetcher.Fetch(ctx, gatewayURL)
		if fetchErr != nil {
			return nil, false, fmt.Errorf("uri resolver: arweave gateway fetch: %w", fetchErr)
		}
		return data, json.Valid(data), nil

	default:
		return nil, false, fmt.Errorf("%w: %q", ErrUnsupportedURIType, uri)
	}
}

// Verify interface compliance at compile time.
var _ MetadataFetcher = (*Resolver)(nil)
