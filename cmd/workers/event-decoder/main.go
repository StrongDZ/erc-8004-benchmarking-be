package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	eventdecoderapp "erc-8004-benchmarking-be/internal/app/eventdecoder"
	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	rabbitmqclient "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	redisinfra "erc-8004-benchmarking-be/internal/infra/redis"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// --- MongoDB ---
	mongoClient, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.PoolOptions{MaxPoolSize: cfg.MongoMaxPoolSize, MinPoolSize: cfg.MongoMinPoolSize, MaxConnIdleTime: cfg.MongoMaxConnIdleMs})
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() { _ = mongoClient.Disconnect(context.Background()) }()

	db := mongoClient.Database(cfg.MongoDatabase)
	eventsRepo := eventrepo.NewRepository(db, cfg.EventsColl)

	if err := eventsRepo.EnsureIndexes(ctx); err != nil {
		log.Fatalf("events indexes: %v", err)
	}

	// --- RabbitMQ ---
	conn, err := rabbitmqclient.NewConn(cfg.RabbitMQURI)
	if err != nil {
		log.Fatalf("rabbitmq connect: %v", err)
	}
	defer conn.Close()

	consumer, err := rabbitmqclient.NewConsumer(conn, cfg.RabbitMQQueue, cfg.ConsumerPrefetch)
	if err != nil {
		log.Fatalf("rabbitmq consumer: %v", err)
	}
	defer consumer.Close()

	// --- Redis (optional; used for realtime event broadcast). ---
	var redisClient *redisinfra.Client
	if cfg.RedisURL != "" {
		rc, err := redisinfra.NewClient(ctx, cfg.RedisURL)
		if err != nil {
			log.Printf("event-decoder: redis disabled (connect failed): %v", err)
		} else {
			redisClient = rc
			defer func() { _ = rc.Close() }()
			log.Printf("event-decoder: redis connected; realtime channel=%s", cfg.RedisEventsChannel)
		}
	}

	log.Printf("event-decoder started; queue=%s prefetch=%d", cfg.RabbitMQQueue, cfg.ConsumerPrefetch)

	app := eventdecoderapp.NewApp(consumer, eventsRepo, redisClient, cfg.RedisEventsChannel)
	if err := app.Run(ctx); err != nil && err != context.Canceled {
		log.Printf("event-decoder stopped: %v", err)
	}
}
