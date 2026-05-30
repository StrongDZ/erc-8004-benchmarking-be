package main

// oasf-schema-refresh — weekly worker that fetches the OASF taxonomy from
// https://schema.oasf.outshift.com/api/1.0.0 and persists it into the erc8004
// primary DB (oasf_skills + oasf_domains collections).

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	oasfrefreshapp "erc-8004-benchmarking-be/internal/app/oasfrefresh"
	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	"erc-8004-benchmarking-be/internal/repository/oasfschema"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	mc, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.PoolOptions{
		MaxPoolSize:     cfg.MongoMaxPoolSize,
		MinPoolSize:     cfg.MongoMinPoolSize,
		MaxConnIdleTime: cfg.MongoMaxConnIdleMs,
	})
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = mc.Disconnect(context.Background()) }()

	db := mc.Database(cfg.MongoDatabase)
	skills := oasfschema.NewRepository(db, "oasf_skills")
	domains := oasfschema.NewRepository(db, "oasf_domains")

	app := oasfrefreshapp.NewApp(skills, domains, cfg.OASFSchemaRefreshCron)

	log.Printf("oasf-schema-refresh: started cron=%q", cfg.OASFSchemaRefreshCron)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("oasf-schema-refresh: stopped: %v", err)
	}
}
