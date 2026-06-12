package price

// provider.go — native-token USD price per chain. Static provider for tests +
// deterministic runs; a live CoinGecko-backed provider can implement the same
// interface later. One lookup per chain, not per wallet.

type Provider interface {
	// NativeUSD returns the USD price of the chain's native token; ok=false if unknown.
	NativeUSD(chainID int64) (float64, bool)
}

type static struct{ m map[int64]float64 }

func NewStatic(m map[int64]float64) Provider { return &static{m: m} }

func (s *static) NativeUSD(chainID int64) (float64, bool) {
	v, ok := s.m[chainID]
	return v, ok
}
