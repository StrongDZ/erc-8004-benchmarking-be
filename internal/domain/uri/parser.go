package uri

// parser.go — Pure URI parsing, type detection, and inline data URI decoding.
// Supports IPFS (v0 CIDv0 / CIDv1), data: URIs (base64 + optional gzip), and HTTP/HTTPS.
// No infrastructure imports — all functions are safe for concurrent use from thousands of goroutines.

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

// URIType enumerates the supported metadata URI schemes.
type URIType int

const (
	URITypeUnknown URIType = iota
	URITypeIPFS
	URITypeDataURI
	URITypeHTTPS
)

func (t URIType) String() string {
	switch t {
	case URITypeIPFS:
		return "ipfs"
	case URITypeDataURI:
		return "data"
	case URITypeHTTPS:
		return "https"
	default:
		return "unknown"
	}
}

var (
	ErrUnsupportedURIType = errors.New("uri: unsupported URI scheme")
	ErrEmptyURI           = errors.New("uri: empty URI")
	ErrInvalidDataURI     = errors.New("uri: malformed data URI")
	ErrInvalidIPFSURI     = errors.New("uri: malformed IPFS URI (missing CID)")
	ErrDecodeFailed       = errors.New("uri: base64 decode failed")
	ErrGunzipFailed       = errors.New("uri: gzip decompression failed")
)

const maxDecompressedSize = 10 << 20 // 10 MiB safety cap for gunzip output

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
	default:
		return URITypeUnknown
	}
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
//	data:application/json;base64,<payload>
//	data:application/json;enc=gzip;base64,<payload>
//
// When enc=gzip is present the base64-decoded bytes are gunzipped before returning.
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
	isGzip := false

	for _, p := range parts {
		p = strings.TrimSpace(p)
		switch {
		case strings.EqualFold(p, "base64"):
			isBase64 = true
		case strings.EqualFold(p, "enc=gzip"):
			isGzip = true
		}
	}

	if !isBase64 {
		return []byte(payload), nil
	}

	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrDecodeFailed, err)
		}
	}

	if !isGzip {
		return decoded, nil
	}

	return gunzip(decoded)
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
