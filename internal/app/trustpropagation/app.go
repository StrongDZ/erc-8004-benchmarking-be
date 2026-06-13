package trustpropagation

// app.go — TrustRank-pass worker: runs LoadGraph → EigenTrustPass → WriteWalletScores
// on a fixed interval.

import (
	"context"
	"log"
	"time"
)

// allChains is the chainID filter passed to Mongo loaders and backfill: 0 means every chain.
const allChains int64 = 0

// AppConfig holds runtime tunables.
type AppConfig struct {
	IntervalMin  int
	IterConfig   IterConfig
	LoaderDeps   LoaderDeps
	WriterDeps   WriterDeps
	BackfillDeps BackfillDeps
}

// App is the trustrank-pass worker.
type App struct{ cfg AppConfig }

// NewApp constructs App with defaults.
func NewApp(cfg AppConfig) *App {
	if cfg.IntervalMin <= 0 {
		cfg.IntervalMin = 10
	}
	return &App{cfg: cfg}
}

// Run blocks until ctx is cancelled, executing one pass immediately then every IntervalMin minutes.
func (a *App) Run(ctx context.Context) error {
	a.runPass(ctx)

	ticker := time.NewTicker(time.Duration(a.cfg.IntervalMin) * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			a.runPass(ctx)
		}
	}
}

func (a *App) runPass(ctx context.Context) {
	start := time.Now()

	if a.cfg.BackfillDeps.Publisher != nil {
		n, err := BackfillUnprocessed(ctx, allChains, a.cfg.BackfillDeps)
		if err != nil {
			log.Printf("trustrank-pass: backfill: %v", err)
		}
		if n > 0 {
			log.Printf("trustrank-pass: backfilled %d unprocessed feedbacks", n)
		}
	}

	log.Printf("trustrank-pass: loading graph (all chains)")

	gd, err := LoadGraph(ctx, a.cfg.LoaderDeps, allChains)
	if err != nil {
		log.Printf("trustrank-pass: load graph: %v", err)
		return
	}
	log.Printf("trustrank-pass: loaded %d nodes %d edges in %v", len(gd.Nodes), len(gd.Edges), time.Since(start))

	t1 := time.Now()
	ws, iters := EigenTrustPass(gd, a.cfg.IterConfig)
	log.Printf("trustrank-pass: iteration done in %v (%d iters, %d rated, %d unrated)", time.Since(t1), iters, len(ws.Rated), len(ws.Unrated))

	t2 := time.Now()
	if err := WriteWalletScores(ctx, a.cfg.WriterDeps, ws); err != nil {
		log.Printf("trustrank-pass: write: %v", err)
		return
	}
	log.Printf("trustrank-pass: wrote %d rated %d unrated wallet scores in %v (total: %v)", len(ws.Rated), len(ws.Unrated), time.Since(t2), time.Since(start))
}
