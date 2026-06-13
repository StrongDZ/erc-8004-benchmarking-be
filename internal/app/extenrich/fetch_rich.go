package extenrich

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

const explorerBaseURL = "https://api.etherscan.io/v2/api"
const explorerMaxBodyBytes = 10 << 20

// FetchRichSignal fetches age and counterparties from Etherscan and updates the wallet's external doc.
func (a *App) FetchRichSignal(ctx context.Context, w wallet.WalletDocument) (wallet.WalletDocument, error) {
	if len(a.explorerClients) == 0 {
		return w, fmt.Errorf("no explorer clients configured")
	}
	client := a.explorerClients[rand.IntN(len(a.explorerClients))]

	feat, err := client.FetchFeatures(w.ChainID, w.Address, time.Now())
	if err != nil {
		if IsExplorerPermanentError(err) {
			doc := w.External
			doc.ExplorerSkipped = true
			doc.RichFetched = true // marked as fetched because we cannot fetch it anyway
			doc.ExplorerAt = time.Now().Unix()
			update := wallet.ExternalUpdate{ID: w.ID, Doc: doc}
			if err2 := a.writeThrough(ctx, []wallet.ExternalUpdate{update}); err2 != nil {
				return w, fmt.Errorf("write through skipped rich: %w", err2)
			}
			w.External = doc
			return w, nil
		}
		return w, err
	}

	f := extscore.Features{
		BalanceUSD:            w.External.BalanceUSD,
		Nonce:                 w.External.Nonce,
		AgeDays:               feat.AgeDays,
		UniqueCounterparties:  feat.UniqueCounterparties,
		HasENS:                w.External.ENS != "",
		BalancePresent:        w.External.Present,
		NoncePresent:          w.External.Present,
		AgePresent:            true,
		CounterpartiesPresent: true,
		ENSApplicable:         w.ChainID == 1,
	}

	doc := w.External
	doc.Score = extscore.Score(f)
	doc.Complete = extscore.Complete(f)
	doc.Present = true
	doc.AgeDays = feat.AgeDays
	doc.Counterparties = feat.UniqueCounterparties
	doc.ExplorerAt = time.Now().Unix()
	doc.RichFetched = true

	update := wallet.ExternalUpdate{ID: w.ID, Doc: doc}
	if err := a.writeThrough(ctx, []wallet.ExternalUpdate{update}); err != nil {
		return w, fmt.Errorf("write through rich signal: %w", err)
	}

	w.External = doc
	return w, nil
}

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
