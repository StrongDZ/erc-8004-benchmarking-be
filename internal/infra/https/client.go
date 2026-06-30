package https

// client.go — Common HTTPS fetch helper (GET + limited body + selective retry).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"erc-8004-benchmarking-be/pkg/retry"
)

const (
	defaultTimeout         = 10 * time.Second
	defaultMaxBodyBytes    = 2 << 20 // 2 MiB
	defaultRetryMaxTry     = 1       // up to 3 attempts total (first attempt + 2 retries)
	defaultRetryStartDelay = 100 * time.Millisecond
)

type ClientOptions struct {
	// Timeout is applied by the underlying http.Client to the whole request.
	Timeout time.Duration

	// MaxBodyBytes caps the downloaded response body size.
	MaxBodyBytes int64

	UserAgent string
	Headers   map[string]string
}

// BodyTooLargeError is returned when the response body exceeds MaxBodyBytes.
type BodyTooLargeError struct {
	URL   string
	Limit int64
}

func (e *BodyTooLargeError) Error() string {
	return fmt.Sprintf("https fetch: response body too large: url=%s limitBytes=%d", e.URL, e.Limit)
}

// HTTPStatusError is returned when the server responds with a non-2xx status.
type HTTPStatusError struct {
	URL        string
	StatusCode int
	Body       string // limited snippet, for debugging only
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("https fetch: unexpected http status: url=%s status=%d body=%q", e.URL, e.StatusCode, e.Body)
}

type fetchResult struct {
	body   []byte
	header http.Header
	status int
}

// Client fetches data from HTTPS endpoints.
type Client struct {
	httpClient *http.Client
	opts       ClientOptions
}

// NewClientWithOptions constructs a HTTPS fetch client with overridable request limits.
func NewClientWithOptions(opts ClientOptions) *Client {
	if opts.Timeout <= 0 {
		opts.Timeout = defaultTimeout
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = defaultMaxBodyBytes
	}

	return &Client{
		httpClient: &http.Client{
			Timeout:   opts.Timeout,
			Transport: &http.Transport{DialContext: safeDialContext},
		},
		opts: opts,
	}
}

// FetchBody performs an HTTPS GET and returns up to MaxBodyBytes bytes.
// It retries on network errors and retryable HTTP statuses (429 and 5xx).
func (c *Client) FetchBody(ctx context.Context, url string) ([]byte, http.Header, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, 0, err
	}
	if strings.HasPrefix(strings.TrimSpace(url), "data:") {
		return nil, nil, 0, fmt.Errorf("https fetch: data URIs are not supported (url=%q)", url)
	}

	res, err := retry.Do(ctx, defaultRetryMaxTry, defaultRetryStartDelay, retry.BackoffLinear, func() (fetchResult, error) {
		if err := ctx.Err(); err != nil {
			return fetchResult{}, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return fetchResult{}, fmt.Errorf("https fetch: new request: %w", err)
		}
		for k, v := range c.opts.Headers {
			req.Header.Set(k, v)
		}
		if ua := strings.TrimSpace(c.opts.UserAgent); ua != "" {
			req.Header.Set("User-Agent", ua)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fetchResult{}, err
		}
		defer resp.Body.Close()

		body, readErr := readLimitedBody(url, resp.Body, c.opts.MaxBodyBytes)
		if readErr != nil {
			// Body size issues shouldn't be retried (same response will exceed MaxBodyBytes again).
			return fetchResult{header: resp.Header, status: resp.StatusCode}, readErr
		}

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			httpErr := &HTTPStatusError{
				URL:        url,
				StatusCode: resp.StatusCode,
				Body:       trimSnippet(body, 4<<10),
			}
			if isRetryableStatus(resp.StatusCode) {
				return fetchResult{header: resp.Header, status: resp.StatusCode}, httpErr
			}

			return fetchResult{header: resp.Header, status: resp.StatusCode}, retry.NonRetryableError{Err: httpErr}
		}

		return fetchResult{body: body, header: resp.Header, status: resp.StatusCode}, nil
	})

	if err != nil {
		// If the error is marked non-retryable, unwrap and return it directly.
		var nre retry.NonRetryableError
		if errors.As(err, &nre) {
			return nil, res.header, res.status, nre.Err
		}
		return nil, res.header, res.status, fmt.Errorf("https fetch: GET failed after retries: url=%s: %w", url, err)
	}

	return res.body, res.header, res.status, nil
}

// FetchJSONText performs FetchBody and validates the body as JSON.
// It returns the raw JSON text (not re-marshaled).
func (c *Client) FetchJSONText(ctx context.Context, url string) (string, int, error) {
	optsCopy := c.opts
	if optsCopy.Headers == nil {
		optsCopy.Headers = make(map[string]string, 1)
	}
	optsCopy.Headers["Accept"] = "application/json"

	cc := &Client{httpClient: c.httpClient, opts: optsCopy}
	body, _, status, err := cc.FetchBody(ctx, url)
	if err != nil {
		return "", status, err
	}
	if !json.Valid(body) {
		return "", status, fmt.Errorf("https fetch: invalid JSON: url=%s", url)
	}
	return string(body), status, nil
}

func isRetryableStatus(statusCode int) bool {
	if statusCode == http.StatusTooManyRequests {
		return true
	}
	return statusCode >= 500 && statusCode <= 599
}

func trimSnippet(b []byte, max int64) string {
	if int64(len(b)) <= max {
		return string(b)
	}
	return string(b[:max])
}

func readLimitedBody(url string, r io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return io.ReadAll(r)
	}
	limited := io.LimitReader(r, maxBytes+1)
	b, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("https fetch: read body: url=%s: %w", url, err)
	}
	if int64(len(b)) > maxBytes {
		return nil, &BodyTooLargeError{URL: url, Limit: maxBytes}
	}
	return b, nil
}

// Ensure errors are discoverable for callers that want to react programmatically.
var (
	_ = (*BodyTooLargeError)(nil)
	_ = (*HTTPStatusError)(nil)
)
