package explorer

// client.go — Etherscan V2 multichain account txlist → ageDays + counterparties.
// One HTTP call per wallet (page 1). The parse is isolated for unit testing.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.etherscan.io/v2/api"

type Client struct {
	httpc  *http.Client
	apiKey string
	offset int
}

func NewClient(httpc *http.Client, apiKey string) *Client {
	return &Client{httpc: httpc, apiKey: apiKey, offset: 10000}
}

type Features struct {
	AgeDays              float64
	UniqueCounterparties int
	HasHistory           bool
}

type txlistResp struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  []struct {
		TimeStamp string `json:"timeStamp"`
		From      string `json:"from"`
		To        string `json:"to"`
	} `json:"result"`
}

// FetchFeatures calls txlist for one wallet on one chain.
func (c *Client) FetchFeatures(chainID int64, address string, now time.Time) (Features, error) {
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

	resp, err := c.httpc.Get(baseURL + "?" + q.Encode())
	if err != nil {
		return Features{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 0)
	tmp := make([]byte, 32*1024)
	for {
		n, rerr := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if rerr != nil {
			break
		}
	}
	return parseTxlist(buf, strings.ToLower(address), now)
}

func parseTxlist(body []byte, walletLower string, now time.Time) (Features, error) {
	var r txlistResp
	if err := json.Unmarshal(body, &r); err != nil {
		return Features{}, fmt.Errorf("explorer decode: %w", err)
	}
	if len(r.Result) == 0 {
		return Features{HasHistory: false}, nil
	}
	firstTs, _ := strconv.ParseInt(r.Result[0].TimeStamp, 10, 64)
	ageDays := now.Sub(time.Unix(firstTs, 0)).Hours() / 24
	if ageDays < 0 {
		ageDays = 0
	}
	cps := make(map[string]struct{})
	for _, tx := range r.Result {
		for _, p := range []string{strings.ToLower(tx.From), strings.ToLower(tx.To)} {
			if p == "" || p == walletLower {
				continue
			}
			cps[p] = struct{}{}
		}
	}
	return Features{AgeDays: ageDays, UniqueCounterparties: len(cps), HasHistory: true}, nil
}
