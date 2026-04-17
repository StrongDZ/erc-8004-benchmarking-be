package evm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/ethereum/go-ethereum/ethclient"

	"erc-8004-benchmarking-be/pkg/retry"
)

// rpcRetrySession keeps one working ethclient and the current RPC index so that
// after successful calls we stay on the same endpoint until retries are exhausted.
type rpcRetrySession struct {
	c        *Crawler
	ctx      context.Context
	rpcs     []string
	idx      int
	cli      *ethclient.Client
	chainID  int64
	contract string
}

func (s *rpcRetrySession) close() {
	if s.cli != nil {
		s.cli.Close()
		s.cli = nil
	}
}

func (s *rpcRetrySession) activeURL() string {
	if s == nil || len(s.rpcs) == 0 || s.idx < 0 || s.idx >= len(s.rpcs) {
		return ""
	}
	return s.rpcs[s.idx]
}

func (s *rpcRetrySession) ensureDial() error {
	if len(s.rpcs) == 0 {
		return errors.New("no RPC endpoints")
	}
	if s.cli != nil {
		return nil
	}
	for attempt := 0; attempt < len(s.rpcs); attempt++ {
		rpc := s.rpcs[s.idx]
		nextIdx := (s.idx + 1) % len(s.rpcs)
		nextRPC := s.rpcs[nextIdx]
		cl, err := DialHealthyRPC(s.ctx, rpc, s.c.rpcTimeout)
		if err != nil {
			if s.contract != "" {
				log.Printf("crawler rpc_switch: chain=%d contract=%s dial/health FAILED on %q (%v) → rotating to %q (attempt %d/%d)",
					s.chainID, s.contract, rpc, err, nextRPC, attempt+1, len(s.rpcs))
			} else {
				log.Printf("crawler rpc_switch: dial/health FAILED on %q (%v) → rotating to %q (attempt %d/%d)",
					rpc, err, nextRPC, attempt+1, len(s.rpcs))
			}
			s.idx = nextIdx
			continue
		}
		s.cli = cl
		return nil
	}
	return fmt.Errorf("no healthy RPC after full rotation (%d endpoints)", len(s.rpcs))
}

func doWithRetryValue[T any](s *rpcRetrySession, op func(*ethclient.Client) (T, error)) (T, error) {
	var zero T
	maxRotations := len(s.rpcs)
	if maxRotations < 1 {
		maxRotations = 1
	}

	for rotation := 0; rotation < maxRotations; rotation++ {
		if err := s.ensureDial(); err != nil {
			return zero, err
		}
		rpc := s.rpcs[s.idx]

		v, err := retry.Do[T](s.ctx, s.c.rpcMaxRetries, s.c.retryBackoffStart, s.c.retryStrategy, func() (T, error) {
			val, opErr := op(s.cli)
			if opErr != nil && isNodeLevelError(opErr) {
				return val, retry.NonRetryableError{Err: opErr}
			}
			return val, opErr
		})
		if err == nil {
			return v, nil
		}

		errSnippet := truncateErr(err, 200)
		nextIdx := (s.idx + 1) % len(s.rpcs)
		nextRPC := s.rpcs[nextIdx]
		if s.contract != "" {
			log.Printf("crawler rpc_switch: chain=%d contract=%s op FAILED on %q max_try=%d: %s → rotating to %q (rotation %d/%d)",
				s.chainID, s.contract, rpc, s.c.rpcMaxRetries, errSnippet, nextRPC, rotation+1, maxRotations)
		} else {
			log.Printf("crawler rpc_switch: op FAILED on %q max_try=%d: %s → rotating to %q (rotation %d/%d)",
				rpc, s.c.rpcMaxRetries, errSnippet, nextRPC, rotation+1, maxRotations)
		}

		s.close()
		s.idx = nextIdx
	}
	return zero, fmt.Errorf("all %d RPCs failed after full rotation", maxRotations)
}

// isNodeLevelError detects errors that will never succeed on retry against the
// SAME node (e.g. pruned history, missing trie) but MAY succeed on a different
// RPC (e.g. an archive node). We skip retries on the current node and rotate.
func isNodeLevelError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, pattern := range nodeLevelErrorPatterns {
		if strings.Contains(msg, pattern) {
			return true
		}
	}
	return false
}

var nodeLevelErrorPatterns = []string{
	"history has been pruned",
	"unknown block",
	"missing trie node",
	"block not found",
	"header not found",
	"error getting block header from triedb and archive",
}

func truncateErr(err error, maxLen int) string {
	if err == nil {
		return "<nil>"
	}
	s := err.Error()
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "...(truncated)"
}
