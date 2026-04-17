package retry

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"time"
)

type BackoffStrategy string

const (
	BackoffLinear      BackoffStrategy = "linear"
	BackoffExponential BackoffStrategy = "exponential"
	BackoffFibonacci   BackoffStrategy = "fibonacci"
	BackoffRandom      BackoffStrategy = "random"
)

// Delay mirrors getRetryDelay in TS: retryCount starts at 1 on first wait after a failure.
func Delay(strategy BackoffStrategy, retryCount int, startDelay time.Duration) time.Duration {
	if startDelay <= 0 {
		startDelay = time.Millisecond
	}
	if retryCount < 1 {
		retryCount = 1
	}

	switch strategy {
	case BackoffExponential:
		d := startDelay
		for i := 2; i <= retryCount; i++ {
			d = d * 2
		}
		return d
	case BackoffLinear:
		return startDelay * time.Duration(retryCount)
	case BackoffFibonacci:
		preDelay := time.Duration(0)
		delay := startDelay
		for i := 2; i <= retryCount; i++ {
			delay = delay + preDelay
			preDelay = delay - preDelay
		}
		return delay
	case BackoffRandom:
		// [startDelay, 2*startDelay), same as Math.random()*startDelay + startDelay
		u := rand.Float64()
		return time.Duration(float64(startDelay) * (1 + u))
	default:
		return startDelay * time.Duration(retryCount)
	}
}

// ParseBackoffStrategy returns Linear for unknown values.
func ParseBackoffStrategy(s string) BackoffStrategy {
	switch BackoffStrategy(s) {
	case BackoffLinear, BackoffExponential, BackoffFibonacci, BackoffRandom:
		return BackoffStrategy(s)
	default:
		return BackoffLinear
	}
}

// Do runs op until it succeeds, ctx is cancelled, or more than maxTry failures have occurred
// after the first attempt (same semantics as the TS retry: first failure uses retryCount 1 for delay).
//
// It returns the last value produced by op (zero value if op never succeeded) and the error (if any).
func Do[T any](ctx context.Context, maxTry int, startDelay time.Duration, strategy BackoffStrategy, op func() (T, error)) (T, error) {
	var zero T
	retryCount := 1
	lastVal := zero

	for {
		v, err := op()
		lastVal = v
		if err == nil {
			return v, nil
		}
		var nr nonRetryable
		if errors.As(err, &nr) {
			return lastVal, nr.Unwrap()
		}
		if retryCount > maxTry {
			log.Printf("retry: exhausted max_try=%d (failures after first attempt) last_err=%s", maxTry, truncateError(err))
			return lastVal, err
		}
		d := Delay(strategy, retryCount, startDelay)
		log.Printf("retry: op failed retry_after_first=%d/%d err=%s backoff=%s strategy=%s",
			retryCount, maxTry, truncateError(err), d, strategy)
		select {
		case <-ctx.Done():
			log.Printf("retry: cancelled during backoff: %v", ctx.Err())
			return lastVal, ctx.Err()
		case <-time.After(d):
		}
		retryCount++
	}
}

const maxLogErrorLen = 200

func truncateError(err error) string {
	if err == nil {
		return "<nil>"
	}
	s := err.Error()
	if len(s) <= maxLogErrorLen {
		return s
	}
	return s[:maxLogErrorLen] + "...(truncated)"
}

// nonRetryable marks an error as "do not retry".
// Do will return immediately with the wrapped error.
type nonRetryable interface {
	error
	Unwrap() error
}

type NonRetryableError struct {
	Err error
}

func (e NonRetryableError) Error() string { return e.Err.Error() }
func (e NonRetryableError) Unwrap() error { return e.Err }
