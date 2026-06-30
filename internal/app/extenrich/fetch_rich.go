package extenrich

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const explorerBaseURL = "https://api.etherscan.io/v2/api"
const explorerMaxBodyBytes = 10 << 20

type ExplorerClient struct {
	httpc  *http.Client
	apiKey string
	offset int
}

func NewExplorerClient(httpc *http.Client, apiKey string) *ExplorerClient {
	return &ExplorerClient{httpc: httpc, apiKey: apiKey, offset: 10000}
}

type ExplorerFeatures struct {
	AgeDays              float64
	UniqueCounterparties int
	HasHistory           bool
}

type txlistEnvelope struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Result  json.RawMessage `json:"result"`
}

type txRow struct {
	TimeStamp string `json:"timeStamp"`
	From      string `json:"from"`
	To        string `json:"to"`
}

// FetchFeatures calls txlist for one wallet on one chain.
func (c *ExplorerClient) FetchFeatures(chainID int64, address string, now time.Time) (ExplorerFeatures, error) {
	q := url.Values{}
	q.Set("chainid", strconv.FormatInt(chainID, 10))
	q.Set("module", "account")
	q.Set("action", "txlist")
	q.Set("address", address)
	q.Set("startblock", "0")
	q.Set("endblock", "99999999")
	q.Set("page", "1")
	q.Set("offset", strconv.Itoa(c.offset))
	q.Set("sort", "asc")
	q.Set("apikey", c.apiKey)

	resp, err := c.httpc.Get(explorerBaseURL + "?" + q.Encode())
	if err != nil {
		return ExplorerFeatures{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	buf, err := io.ReadAll(io.LimitReader(resp.Body, explorerMaxBodyBytes))
	if err != nil {
		return ExplorerFeatures{}, fmt.Errorf("explorer read: %w", err)
	}
	return parseTxlist(buf, strings.ToLower(address), now)
}

func parseTxlist(body []byte, walletLower string, now time.Time) (ExplorerFeatures, error) {
	var env txlistEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return ExplorerFeatures{}, fmt.Errorf("explorer decode: %w", err)
	}
	txs, err := decodeTxlistResult(env.Result)
	if err != nil {
		return ExplorerFeatures{}, err
	}
	if len(txs) == 0 {
		if env.Status == "0" && env.Message != "No transactions found" {
			return ExplorerFeatures{}, fmt.Errorf("explorer error: %s", env.Message)
		}
		return ExplorerFeatures{HasHistory: false}, nil
	}
	firstTs, _ := strconv.ParseInt(txs[0].TimeStamp, 10, 64)
	ageDays := now.Sub(time.Unix(firstTs, 0)).Hours() / 24
	if ageDays < 0 {
		ageDays = 0
	}
	cps := make(map[string]struct{})
	for _, tx := range txs {
		for _, p := range []string{strings.ToLower(tx.From), strings.ToLower(tx.To)} {
			if p == "" || p == walletLower {
				continue
			}
			cps[p] = struct{}{}
		}
	}
	return ExplorerFeatures{AgeDays: ageDays, UniqueCounterparties: len(cps), HasHistory: true}, nil
}

func decodeTxlistResult(raw json.RawMessage) ([]txRow, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	switch raw[0] {
	case '"':
		var msg string
		if err := json.Unmarshal(raw, &msg); err != nil {
			return nil, fmt.Errorf("explorer decode result string: %w", err)
		}
		if msg == "No transactions found" {
			return nil, nil
		}
		return nil, fmt.Errorf("explorer error: %s", msg)
	case '[':
		var txs []txRow
		if err := json.Unmarshal(raw, &txs); err != nil {
			return nil, fmt.Errorf("explorer decode result array: %w", err)
		}
		return txs, nil
	default:
		return nil, fmt.Errorf("explorer decode: unexpected result JSON type")
	}
}

func IsExplorerPermanentError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "explorer error:") {
		return false
	}
	for _, needle := range []string{
		"free api access is not supported",
		"not supported for this chain",
		"invalid chain",
		"chain not supported",
		"invalid api key",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}
