package main

// api — REST API server exposing leaderboard, agent profiles, and transparency endpoints.
// See docs/API_SPEC_v1.md for the full contract.
//
// @title ERC-8004 Benchmarking API
// @version 1.0
// @description REST API for leaderboard, agent profiles, OASF discovery, chains metadata, and admin observability.
// @BasePath /api/v1
// @schemes http https
// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name X-API-Key

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	_ "erc-8004-benchmarking-be/swagger"
	"erc-8004-benchmarking-be/internal/api"
	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	redisinfra "erc-8004-benchmarking-be/internal/infra/redis"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("api: load config: %v", err)
	}

	mc, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.PoolOptions{MaxPoolSize: cfg.MongoMaxPoolSize, MinPoolSize: cfg.MongoMinPoolSize, MaxConnIdleTime: cfg.MongoMaxConnIdleMs})
	if err != nil {
		log.Fatalf("api: mongo connect: %v", err)
	}
	defer func() { _ = mc.Disconnect(context.Background()) }()

	// Optional Redis client: enables realtime WS fan-out by subscribing to
	// cfg.RedisEventsChannel. If unavailable, the API still serves REST but
	// realtime broadcasting is silently disabled.
	var redisClient *redisinfra.Client
	if cfg.RedisURL != "" {
		rc, err := redisinfra.NewClient(ctx, cfg.RedisURL)
		if err != nil {
			log.Printf("api: redis disabled (connect failed): %v", err)
		} else {
			redisClient = rc
			defer func() { _ = rc.Close() }()
			log.Printf("api: redis connected; realtime channel=%s", cfg.RedisEventsChannel)
		}
	}

	repos := api.NewRepositories(mc, cfg)
	srv := api.NewServer(cfg, repos, redisClient)

	if err := srv.Start(ctx); err != nil {
		log.Fatalf("api: %v", err)
	}
	log.Printf("api: shutdown complete")
}
