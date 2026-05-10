package uri

// parser.go — Pure URI parsing, type detection, and inline data URI decoding.
// Supports IPFS (CIDv0/CIDv1), data: URIs (plain, base64, gzip, zstd, brotli),
// HTTP/HTTPS, and Arweave (ar://).
// No infrastructure imports — all functions are safe for concurrent use from thousands of goroutines.

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

// URIType enumerates the supported metadata URI schemes.
type URIType int

const (
	URITypeUnknown URIType = iota
	URITypeIPFS
	URITypeDataURI
	URITypeHTTPS
	URITypeArweave
)

func (t URIType) String() string {
	switch t {
	case URITypeIPFS:
		return "ipfs"
	case URITypeDataURI:
		return "data"
	case URITypeHTTPS:
		return "https"
	case URITypeArweave:
		return "arweave"
	default:
		return "unknown"
	}
}

var (
	ErrUnsupportedURIType  = errors.New("uri: unsupported URI scheme")
	ErrEmptyURI            = errors.New("uri: empty URI")
	ErrInvalidDataURI      = errors.New("uri: malformed data URI")
	ErrInvalidIPFSURI      = errors.New("uri: malformed IPFS URI (missing CID)")
	ErrInvalidArweaveURI   = errors.New("uri: malformed Arweave URI (missing transaction ID)")
	ErrDecodeFailed        = errors.New("uri: base64 decode failed")
	ErrGunzipFailed        = errors.New("uri: gzip decompression failed")
	ErrZstdDecompFailed    = errors.New("uri: zstd decompression failed")
	ErrBrotliDecompFailed  = errors.New("uri: brotli decompression failed")
)

const maxDecompressedSize = 10 << 20 // 10 MiB safety cap for decompressed output

// DetectURIType classifies a URI string into one of the supported schemes.
func DetectURIType(uri string) URIType {
	trimmed := strings.TrimSpace(uri)
	switch {
	case strings.HasPrefix(trimmed, "ipfs://"):
		return URITypeIPFS
	case strings.HasPrefix(trimmed, "data:"):
		return URITypeDataURI
	case strings.HasPrefix(trimmed, "https://"), strings.HasPrefix(trimmed, "http://"):
		return URITypeHTTPS
	case strings.HasPrefix(trimmed, "ar://"):
		return URITypeArweave
	default:
		return URITypeUnknown
	}
}

// ArweaveToGatewayURL converts an ar:// URI to an HTTPS Arweave gateway URL.
// gatewayBase must end with "/" (e.g. "https://arweave.net/").
func ArweaveToGatewayURL(uri, gatewayBase string) (string, error) {
	trimmed := strings.TrimSpace(uri)
	if !strings.HasPrefix(trimmed, "ar://") {
		return "", fmt.Errorf("%w: got %q", ErrInvalidArweaveURI, uri)
	}
	txID := strings.TrimPrefix(trimmed, "ar://")
	txID = strings.TrimLeft(txID, "/")
	if txID == "" {
		return "", fmt.Errorf("%w: got %q", ErrInvalidArweaveURI, uri)
	}
	gateway := strings.TrimSpace(gatewayBase)
	if gateway == "" {
		gateway = "https://arweave.net/"
	}
	if !strings.HasSuffix(gateway, "/") {
		gateway += "/"
	}
	return gateway + txID, nil
}

// IPFSToGatewayURL converts an ipfs:// URI to a full HTTPS gateway URL.
// gatewayBase must end with "/ipfs/" (e.g. "https://ipfs.io/ipfs/").
// Supports both CIDv0 (Qm...) and CIDv1 (bafy...) hashes.
func IPFSToGatewayURL(uri, gatewayBase string) (string, error) {
	trimmed := strings.TrimSpace(uri)
	if !strings.HasPrefix(trimmed, "ipfs://") {
		return "", fmt.Errorf("%w: got %q", ErrInvalidIPFSURI, uri)
	}
	cid := strings.TrimPrefix(trimmed, "ipfs://")
	cid = strings.TrimLeft(cid, "/")
	if cid == "" {
		return "", fmt.Errorf("%w: got %q", ErrInvalidIPFSURI, uri)
	}

	gateway := strings.TrimSpace(gatewayBase)
	if gateway == "" {
		gateway = "https://ipfs.io/ipfs/"
	}
	if !strings.HasSuffix(gateway, "/") {
		gateway += "/"
	}
	return gateway + cid, nil
}

// ParseDataURI decodes a data: URI to raw bytes.
//
// Supported formats:
//
//	data:application/json,<url-encoded-json>          plain JSON (percent-encoded)
//	data:application/json;base64,<payload>            base64 JSON
//	data:application/json;enc=gzip;base64,<payload>   base64 + gzip
//	data:application/json;enc=zstd;base64,<payload>   base64 + zstd
//	data:application/json;enc=br;base64,<payload>     base64 + brotli
func ParseDataURI(uri string) ([]byte, error) {
	trimmed := strings.TrimSpace(uri)
	if !strings.HasPrefix(trimmed, "data:") {
		return nil, fmt.Errorf("%w: missing data: prefix", ErrInvalidDataURI)
	}

	commaIdx := strings.Index(trimmed, ",")
	if commaIdx < 0 {
		return nil, fmt.Errorf("%w: no comma separator found", ErrInvalidDataURI)
	}

	header := trimmed[5:commaIdx] // everything between "data:" and ","
	payload := trimmed[commaIdx+1:]

	parts := strings.Split(header, ";")
	isBase64 := false
	enc := ""

	for _, p := range parts {
		p = strings.TrimSpace(p)
		switch {
		case strings.EqualFold(p, "base64"):
			isBase64 = true
		case strings.EqualFold(p, "enc=gzip"):
			enc = "gzip"
		case strings.EqualFold(p, "enc=zstd"):
			enc = "zstd"
		case strings.EqualFold(p, "enc=br"):
			enc = "br"
		}
	}

	if !isBase64 {
		// Plain-text payload: percent-decode per RFC 2397.
		decoded, err := url.PathUnescape(payload)
		if err != nil {
			return []byte(payload), nil // best-effort fallback for unescaped JSON
		}
		return []byte(decoded), nil
	}

	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrDecodeFailed, err)
		}
	}

	switch enc {
	case "gzip":
		return gunzip(decoded)
	case "zstd":
		return dezstd(decoded)
	case "br":
		return debrotli(decoded)
	default:
		return decoded, nil
	}
}

func gunzip(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGunzipFailed, err)
	}
	defer gr.Close()

	lr := io.LimitReader(gr, maxDecompressedSize+1)
	out, err := io.ReadAll(lr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrGunzipFailed, err)
	}
	if int64(len(out)) > maxDecompressedSize {
		return nil, fmt.Errorf("%w: decompressed data exceeds %d bytes", ErrGunzipFailed, maxDecompressedSize)
	}
	return out, nil
}

func dezstd(data []byte) ([]byte, error) {
	dec, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrZstdDecompFailed, err)
	}
	defer dec.Close()

	lr := io.LimitReader(dec, maxDecompressedSize+1)
	out, err := io.ReadAll(lr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrZstdDecompFailed, err)
	}
	if int64(len(out)) > maxDecompressedSize {
		return nil, fmt.Errorf("%w: decompressed data exceeds %d bytes", ErrZstdDecompFailed, maxDecompressedSize)
	}
	return out, nil
}

func debrotli(data []byte) ([]byte, error) {
	br := brotli.NewReader(bytes.NewReader(data))
	lr := io.LimitReader(br, maxDecompressedSize+1)
	out, err := io.ReadAll(lr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBrotliDecompFailed, err)
	}
	if int64(len(out)) > maxDecompressedSize {
		return nil, fmt.Errorf("%w: decompressed data exceeds %d bytes", ErrBrotliDecompFailed, maxDecompressedSize)
	}
	return out, nil
}
