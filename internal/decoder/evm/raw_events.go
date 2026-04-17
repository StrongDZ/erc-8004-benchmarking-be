package evm

// decoder.go — Decodes RawLogMessages into event name + parameter maps using ABI.
// Output is a Go-friendly map[string]any that the consumer persists to MongoDB.

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"erc-8004-benchmarking-be/internal/mq"
	"erc-8004-benchmarking-be/internal/repository/config"
)

// Args is the decoded parameter map written to MongoDB.
type Args = map[string]any

// abiJSON and abiInputJSON are local structs matching the JSON shape
// expected by go-ethereum's abi.JSON() parser.
type abiJSON struct {
	Anonymous bool           `json:"anonymous,omitempty"`
	Inputs    []abiInputJSON `json:"inputs"`
	Name      string         `json:"name"`
	Type      string         `json:"type"`
}

type abiInputJSON struct {
	Indexed      bool   `json:"indexed,omitempty"`
	InternalType string `json:"internalType,omitempty"`
	Name         string `json:"name"`
	Type         string `json:"type"`
}

// buildParsedABI converts ABI fragments stored in the MQ message into a
// go-ethereum abi.ABI for event lookup and data unpacking.
func buildParsedABI(fragments []config.ABIEventFragment) (abi.ABI, error) {
	items := make([]abiJSON, 0, len(fragments))
	for _, f := range fragments {
		inputs := make([]abiInputJSON, 0, len(f.Inputs))
		for _, in := range f.Inputs {
			t := strings.TrimSpace(in.Type)
			if t == "" {
				t = strings.TrimSpace(in.InternalType)
			}
			inputs = append(inputs, abiInputJSON{
				Indexed:      in.Indexed,
				InternalType: in.InternalType,
				Name:         in.Name,
				Type:         t,
			})
		}
		items = append(items, abiJSON{
			Anonymous: f.Anonymous,
			Inputs:    inputs,
			Name:      f.Name,
			Type:      "event",
		})
	}

	raw, err := json.Marshal(items)
	if err != nil {
		return abi.ABI{}, fmt.Errorf("marshal abi: %w", err)
	}
	return abi.JSON(strings.NewReader(string(raw)))
}

// DecodeLog decodes a RawLogMessage using its attached ABI.
// Returns the event name and a map of parameter name → Go value.
//
// Indexed parameters of dynamic types (string, bytes, arrays) store only
// their keccak256 hash on-chain; those are returned as a hex string.
func DecodeLog(msg mq.RawLogMessage) (eventName string, args Args, err error) {
	if len(msg.Topics) == 0 {
		return "", nil, fmt.Errorf("log has no topics (tx=%s log=%d)", msg.TxHash, msg.LogIndex)
	}

	parsedABI, err := buildParsedABI(msg.EventABI)
	if err != nil {
		return "", nil, fmt.Errorf("build abi: %w", err)
	}

	topic0 := common.HexToHash(msg.Topics[0])
	event, err := parsedABI.EventByID(topic0)
	if err != nil {
		return "", nil, fmt.Errorf("unknown event topic0=%s: %w", msg.Topics[0], err)
	}

	args = make(Args)

	// Decode non-indexed args from data.
	data := common.FromHex(msg.Data)
	nonIndexed := event.Inputs.NonIndexed()
	if len(data) > 0 && len(nonIndexed) > 0 {
		values, err := nonIndexed.UnpackValues(data)
		if err != nil {
			return event.Name, nil, fmt.Errorf("unpack data (event=%s tx=%s): %w", event.Name, msg.TxHash, err)
		}
		for i, v := range values {
			args[nonIndexed[i].Name] = formatValue(v)
		}
	}

	// Decode indexed args from topics[1:].
	topicIdx := 1
	for _, arg := range event.Inputs {
		if !arg.Indexed {
			continue
		}
		if topicIdx >= len(msg.Topics) {
			break
		}
		topic := common.HexToHash(msg.Topics[topicIdx])
		topicIdx++

		v, decErr := decodeIndexedArg(arg, topic)
		if decErr != nil {
			args[arg.Name] = topic.Hex()
		} else {
			args[arg.Name] = v
		}
	}

	return event.Name, args, nil
}

// decodeIndexedArg decodes a single indexed topic value according to its ABI type.
// Dynamic types (string, bytes, arrays, tuples) cannot be recovered and are
// returned as the raw keccak256 hex stored on-chain.
func decodeIndexedArg(arg abi.Argument, topic common.Hash) (any, error) {
	b := topic.Bytes()

	switch arg.Type.T {
	case abi.AddressTy:
		return common.BytesToAddress(b).Hex(), nil

	case abi.BoolTy:
		return b[31] == 1, nil

	case abi.UintTy:
		return new(big.Int).SetBytes(b).String(), nil

	case abi.IntTy:
		n := new(big.Int).SetBytes(b)
		if b[0]&0x80 != 0 {
			n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 256))
		}
		return n.String(), nil

	case abi.FixedBytesTy:
		size := arg.Type.Size
		if size < 1 || size > 32 {
			size = 32
		}
		return common.Bytes2Hex(b[:size]), nil

	case abi.StringTy, abi.BytesTy, abi.SliceTy, abi.ArrayTy, abi.TupleTy:
		return topic.Hex(), nil

	default:
		return topic.Hex(), nil
	}
}

// formatValue converts go-ethereum unpacked values to JSON-friendly Go types.
func formatValue(v any) any {
	switch val := v.(type) {
	case *big.Int:
		return val.String()
	case common.Address:
		return val.Hex()
	case [32]byte:
		return common.Bytes2Hex(val[:])
	case []byte:
		return common.Bytes2Hex(val)
	default:
		return v
	}
}
