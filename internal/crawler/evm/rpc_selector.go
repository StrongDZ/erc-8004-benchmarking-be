package evm

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

// DialHealthyRPC dials one endpoint and checks eth_blockNumber. Caller must Close the client.
func DialHealthyRPC(ctx context.Context, rpc string, timeout time.Duration) (*ethclient.Client, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, timeout)
	client, err := ethclient.DialContext(rpcCtx, rpc)
	cancel()
	if err != nil {
		return nil, err
	}
	healthCtx, healthCancel := context.WithTimeout(ctx, timeout)
	_, err = client.BlockNumber(healthCtx)
	healthCancel()
	if err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}
