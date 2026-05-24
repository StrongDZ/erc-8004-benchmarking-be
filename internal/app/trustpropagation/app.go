package trustpropagation

// app.go — TrustRank-pass worker: runs LoadGraph → TrustRankPass → WritePropagedScores
// on a fixed interval.

import (
	"context"
	"log"
	"time"
)

// AppConfig holds runtime tunables.
type AppConfig struct {
	ChainID      int64
	IntervalMin  int
	IterConfig   PropagationIterConfig
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
		n, err := BackfillUnprocessed(ctx, a.cfg.ChainID, a.cfg.BackfillDeps)
		if err != nil {
			log.Printf("trustrank-pass: backfill: %v", err)
		}
		if n > 0 {
			log.Printf("trustrank-pass: backfilled %d unprocessed feedbacks", n)
		}
	}

	log.Printf("trustrank-pass: loading graph (chainID=%d)", a.cfg.ChainID)

	gd, err := LoadGraph(ctx, a.cfg.LoaderDeps, a.cfg.ChainID)
	if err != nil {
		log.Printf("trustrank-pass: load graph: %v", err)
		return
	}
	log.Printf("trustrank-pass: loaded %d nodes %d edges in %v", len(gd.Nodes), len(gd.Edges), time.Since(start))

	t1 := time.Now()
	scores, iters := TrustRankPass(gd, a.cfg.IterConfig)
	log.Printf("trustrank-pass: iteration done in %v (%d iters)", time.Since(t1), iters)

	t2 := time.Now()
	if err := WritePropagedScores(ctx, a.cfg.WriterDeps, scores, BuildNodeKindMap(gd)); err != nil {
		log.Printf("trustrank-pass: write: %v", err)
		return
	}
	log.Printf("trustrank-pass: wrote %d scores in %v (total: %v)", len(scores), time.Since(t2), time.Since(start))
}
