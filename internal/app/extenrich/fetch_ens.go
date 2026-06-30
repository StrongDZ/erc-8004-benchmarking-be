package extenrich

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const ensBaseURL = "https://api.ensdata.net/"
const ensMaxBodyBytes = 1 << 20

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
