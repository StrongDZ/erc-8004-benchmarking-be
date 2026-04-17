package evm

// metadata.go — Utilities for decoding MetadataSet event values.
// On-chain metadata values are bytes and may be encoded in many shapes.

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"math/big"
	"strings"
	"unicode/utf8"
)

// MetadataDecodeResult stores normalized decode output for one metadata value.
type MetadataDecodeResult struct {
	RawHex       string
	Decoded      string
	DetectedType string
	Confidence   string
}

// DecodeMetadataValue decodes metadataValue with key-first rules, then heuristics fallback.
func DecodeMetadataValue(key, hexValue string) MetadataDecodeResult {
	trimmed := strings.TrimSpace(hexValue)
	if trimmed == "" {
		return MetadataDecodeResult{
			RawHex:       "",
			Decoded:      "",
			DetectedType: "empty",
			Confidence:   "high",
		}
	}

	normalized := strings.TrimPrefix(strings.ToLower(trimmed), "0x")
	data, err := hex.DecodeString(normalized)
	if err != nil {
		return MetadataDecodeResult{
			RawHex:       normalized,
			Decoded:      trimmed,
			DetectedType: "raw",
			Confidence:   "low",
		}
	}

	switch key {
	case "agentWallet", "serviceRegistry", "flaunchToken", "payTo", "tokenAddress":
		if v := decodeAddressLike(data, normalized); v != "" {
			return MetadataDecodeResult{RawHex: normalized, Decoded: v, DetectedType: "address", Confidence: "high"}
		}
	case "serviceId", "priceWei", "chainId":
		if len(data) == 32 {
			return MetadataDecodeResult{
				RawHex:       normalized,
				Decoded:      new(big.Int).SetBytes(data).String(),
				DetectedType: "uint256",
				Confidence:   "high",
			}
		}
	case "ecosystem", "name", "description", "version", "source", "meshAgentId", "twitter", "website", "email", "skills", "tags", "platform", "category", "endpoint", "ensName", "mandate", "manifestUrl", "agentName", "agentType":
		if s := decodeABIString(data); s != "" && isMostlyPrintable(s) {
			return MetadataDecodeResult{RawHex: normalized, Decoded: s, DetectedType: "abi_string", Confidence: "high"}
		}
		if s := decodeASCIIHex(data); s != "" {
			return MetadataDecodeResult{RawHex: normalized, Decoded: s, DetectedType: "ascii_hex", Confidence: "high"}
		}
	}

	// Heuristic fallback for unknown keys.
	if s := decodeABIString(data); s != "" && isMostlyPrintable(s) {
		return MetadataDecodeResult{RawHex: normalized, Decoded: s, DetectedType: "abi_string", Confidence: "medium"}
	}
	if v := decodeAddressLike(data, normalized); v != "" {
		return MetadataDecodeResult{RawHex: normalized, Decoded: v, DetectedType: "address", Confidence: "medium"}
	}
	if len(data) == 32 {
		return MetadataDecodeResult{
			RawHex:       normalized,
			Decoded:      new(big.Int).SetBytes(data).String(),
			DetectedType: "uint256",
			Confidence:   "low",
		}
	}
	if s := decodeASCIIHex(data); s != "" {
		detected := "ascii_hex"
		if isJSON(s) {
			detected = "json_hex"
		}
		return MetadataDecodeResult{RawHex: normalized, Decoded: s, DetectedType: detected, Confidence: "medium"}
	}

	return MetadataDecodeResult{
		RawHex:       normalized,
		Decoded:      trimmed,
		DetectedType: "raw",
		Confidence:   "low",
	}
}

// decodeABIString decodes ABI-encoded dynamic string.
// Format: offset(32 bytes) + length(32 bytes) + data(padded to 32-byte chunks)
func decodeABIString(data []byte) string {
	// Need at least 1 word for offset.
	if len(data) < 32 {
		return ""
	}

	// ABI dynamic string layout:
	// [0:32]   -> offset to string head (usually 32)
	// [offset:offset+32] -> length
	// [offset+32:offset+32+length] -> bytes
	offsetBI := new(big.Int).SetBytes(data[:32])
	if !offsetBI.IsUint64() {
		return ""
	}
	offset := offsetBI.Uint64()

	// Protect int conversions and out-of-range offset.
	if offset > math.MaxInt || int(offset) > len(data)-32 {
		return ""
	}
	lengthStart := int(offset)
	lengthEnd := lengthStart + 32
	if lengthEnd > len(data) {
		return ""
	}

	lengthBI := new(big.Int).SetBytes(data[lengthStart:lengthEnd])
	if !lengthBI.IsUint64() {
		return ""
	}
	length := lengthBI.Uint64()
	if length == 0 {
		return ""
	}
	if length > math.MaxInt {
		return ""
	}

	dataStart := lengthEnd
	dataEnd := dataStart + int(length)
	if dataEnd > len(data) || dataEnd < dataStart {
		return ""
	}

	return string(data[dataStart:dataEnd])
}

func decodeASCIIHex(data []byte) string {
	if len(data) == 0 || !utf8.Valid(data) {
		return ""
	}
	s := string(data)
	if !isMostlyPrintable(s) {
		return ""
	}
	return strings.TrimSpace(s)
}

func decodeAddressLike(data []byte, raw string) string {
	if len(raw) == 40 {
		return "0x" + raw
	}
	if len(data) == 20 {
		return "0x" + hex.EncodeToString(data)
	}
	if len(data) == 32 {
		return "0x" + hex.EncodeToString(data[len(data)-20:])
	}
	return ""
}

func isMostlyPrintable(s string) bool {
	if s == "" {
		return false
	}
	printable := 0
	for _, r := range s {
		if (r >= 32 && r <= 126) || r == '\n' || r == '\t' || r == '\r' {
			printable++
		}
	}
	return float64(printable)/float64(len([]rune(s))) >= 0.9
}

func isJSON(s string) bool {
	var v any
	return json.Unmarshal([]byte(s), &v) == nil
}
