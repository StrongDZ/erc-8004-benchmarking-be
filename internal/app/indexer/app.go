package indexer

// app.go — Indexer application orchestrator.
// Wraps the EVM crawler and drives its run loop.

import (
	"context"
	"time"

	evmcrawler "erc-8004-benchmarking-be/internal/crawler/evm"
)

// App wraps the crawler run loop as an application-level concern.
type App struct {
	crawler           *evmcrawler.Crawler
	livePollInterval  time.Duration
	errorPollInterval time.Duration
}

// NewApp returns an App ready to run the crawler loop.
func NewApp(
	crawler *evmcrawler.Crawler,
	livePollInterval time.Duration,
	errorPollInterval time.Duration,
) *App {
	return &App{
		crawler:           crawler,
		livePollInterval:  livePollInterval,
		errorPollInterval: errorPollInterval,
	}
}

// Run blocks until ctx is cancelled, driving the crawler's polling loop.
func (a *App) Run(ctx context.Context) {
	a.crawler.Run(ctx, a.livePollInterval, a.errorPollInterval)
}
