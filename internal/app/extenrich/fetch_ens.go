package extenrich

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"erc-8004-benchmarking-be/internal/domain/extscore"
	"erc-8004-benchmarking-be/internal/repository/wallet"
)

const ensBaseURL = "https://api.ensdata.net/"
const ensMaxBodyBytes = 1 << 20

// FetchENSSignal fetches ENS name and avatar from ENS Client and updates the wallet's external doc.
func (a *App) FetchENSSignal(ctx context.Context, w wallet.WalletDocument) (wallet.WalletDocument, error) {
	if a.ensClient == nil {
		return w, fmt.Errorf("no ENS client configured")
	}

	res, err := a.ensClient.Lookup(w.Address)
	if err != nil {
		return w, err
	}

	update := wallet.ExternalENSUpdate{
		ID:         w.ID,
		Score:      extscore.Score(ensFeatures(w, res.ENS)),
		ENS:        res.ENS,
		ENSAvatar:  res.Avatar,
		ENSAt:      time.Now().Unix(),
		ENSFetched: true,
	}

	if err := a.writeThroughENS(ctx, []wallet.ExternalENSUpdate{update}); err != nil {
		return w, fmt.Errorf("write through ens signal: %w", err)
	}

	w.External.ENS = res.ENS
	w.External.ENSAvatar = res.Avatar
	w.External.ENSAt = update.ENSAt
	w.External.ENSFetched = true
	w.External.Score = update.Score
	return w, nil
}

type ENSClient struct {
	httpc *http.Client
}

func NewENSClient(httpc *http.Client) *ENSClient {
	return &ENSClient{httpc: httpc}
}

type ENSResult struct {
	Found  bool
	ENS    string
	Avatar string
}

type ensLookupResp struct {
	ENS       string `json:"ens"`
	Avatar    string `json:"avatar"`
	AvatarURL string `json:"avatar_url"`
	Error     bool   `json:"error"`
}

func (c *ENSClient) Lookup(address string) (ENSResult, error) {
	resp, err := c.httpc.Get(ensBaseURL + address)
	if err != nil {
		return ENSResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	buf, err := io.ReadAll(io.LimitReader(resp.Body, ensMaxBodyBytes))
	if err != nil {
		return ENSResult{}, fmt.Errorf("ens read: %w", err)
	}
	return parseENSLookup(resp.StatusCode, buf)
}

func parseENSLookup(statusCode int, body []byte) (ENSResult, error) {
	var r ensLookupResp
	if err := json.Unmarshal(body, &r); err != nil {
		return ENSResult{}, fmt.Errorf("ens decode: %w", err)
	}

	if statusCode == http.StatusNotFound && r.Error {
		return ENSResult{Found: false}, nil
	}
	if statusCode != http.StatusOK {
		return ENSResult{}, fmt.Errorf("ens: unexpected status %d", statusCode)
	}
	if r.ENS == "" {
		return ENSResult{Found: false}, nil
	}

	avatar := r.AvatarURL
	if avatar == "" {
		avatar = r.Avatar
	}
	return ENSResult{Found: true, ENS: r.ENS, Avatar: avatar}, nil
}
