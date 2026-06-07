package main

// restart-trustrank — destructive reset for TrustRank re-processing:
//   1) Delete stale RabbitMQ queues (classified feedback, desc summary, per-chain service_uri).
//   2) Drop the entire analyzed agents database (agents, scores, feedbacks, …).
//   3) Delete config documents whose _id matches trustrank_worker_<chain_id>
//      (per-chain TrustRank cursors in the primary DB config collection).
//
// Requires .env / MONGO_URI like other tools. Use --dry-run first.
// scripts/restart-trustrank.sh runs `docker compose -f ../docker-compose.all.yaml up --build`
// after success (skipped with --dry-run).

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	rabbitmqclient "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
)

var trustrankCheckpointID = regexp.MustCompile(`^trustrank_worker_(\d+)$`)

func main() {
	dryRun := flag.Bool("dry-run", false, "only print what would be done (no writes)")
	yes := flag.Bool("y", false, "skip interactive confirmation")
	flag.Parse()

	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("restart-trustrank: config: %v", err)
	}

	client, err := mongoclient.NewClient(ctx, cfg.MongoURI, mongoclient.DefaultPoolOptions())
	if err != nil {
		log.Fatalf("restart-trustrank: mongo connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	analyzedName := cfg.AnalyzedDatabase
	primaryName := cfg.MongoDatabase
	configColl := cfg.ConfigColl

	checkpointFilter := bson.M{
		"_id": primitive.Regex{Pattern: `^trustrank_worker_\d+$`, Options: ""},
	}

	cfgDB := client.Database(primaryName).Collection(configColl)
	delCount, err := cfgDB.CountDocuments(ctx, checkpointFilter)
	if err != nil {
		log.Fatalf("restart-trustrank: count checkpoints: %v", err)
	}

	chainIDs, err := collectChainIDs(ctx, client, cfg, checkpointFilter)
	if err != nil {
		log.Fatalf("restart-trustrank: list chain ids: %v", err)
	}
	queues := buildResetQueues(chainIDs)

	log.Printf("analyzed DB to drop: %q", analyzedName)
	log.Printf("checkpoints to delete: %d doc(s) in %q.%q matching trustrank_worker_<chain_id>", delCount, primaryName, configColl)
	log.Printf("rabbitmq queues to delete: %d (%s)", len(queues), strings.Join(queues, ", "))

	if *dryRun {
		log.Print("dry-run: no changes made")
		return
	}

	if !*yes {
		fmt.Fprintf(os.Stderr, "This will delete %d RabbitMQ queue(s), DROP database %q, and delete %d checkpoint(s). Type YES to continue: ",
			len(queues), analyzedName, delCount)
		var confirm string
		if _, err := fmt.Scanln(&confirm); err != nil || confirm != "YES" {
			log.Fatal("restart-trustrank: aborted (expected YES)")
		}
	}

	if err := deleteRabbitQueues(rabbitMQURIForHost(cfg.RabbitMQURI), queues); err != nil {
		log.Fatalf("restart-trustrank: delete rabbitmq queues: %v", err)
	}
	log.Printf("deleted %d rabbitmq queue(s)", len(queues))

	if err := client.Database(analyzedName).Drop(ctx); err != nil {
		log.Fatalf("restart-trustrank: drop database %q: %v", analyzedName, err)
	}
	log.Printf("dropped database %q", analyzedName)

	res, err := cfgDB.DeleteMany(ctx, checkpointFilter)
	if err != nil {
		log.Fatalf("restart-trustrank: delete checkpoints: %v", err)
	}
	log.Printf("deleted %d checkpoint document(s) from %q.%q", res.DeletedCount, primaryName, configColl)
}

func buildResetQueues(chainIDs []int64) []string {
	queues := []string{
		mq.QueueFeedbackClassified,
		mq.QueueAgentDescSummary,
	}
	for _, chainID := range chainIDs {
		queues = append(queues, mq.ServiceURIQueueName(chainID))
	}
	return queues
}

func collectChainIDs(ctx context.Context, client *mongo.Client, cfg config.Config, checkpointFilter bson.M) ([]int64, error) {
	seen := make(map[int64]struct{})

	contractsColl := client.Database(cfg.MongoDatabase).Collection(cfg.ContractsColl)
	cur, err := contractsColl.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"chainId": 1}))
	if err != nil {
		return nil, fmt.Errorf("contracts find: %w", err)
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var doc struct {
			ChainID int64 `bson:"chainId"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("contracts decode: %w", err)
		}
		if doc.ChainID > 0 {
			seen[doc.ChainID] = struct{}{}
		}
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("contracts cursor: %w", err)
	}

	cfgColl := client.Database(cfg.MongoDatabase).Collection(cfg.ConfigColl)
	checkpointCur, err := cfgColl.Find(ctx, checkpointFilter, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("checkpoints find: %w", err)
	}
	defer checkpointCur.Close(ctx)

	for checkpointCur.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := checkpointCur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("checkpoint decode: %w", err)
		}
		m := trustrankCheckpointID.FindStringSubmatch(doc.ID)
		if len(m) != 2 {
			continue
		}
		chainID, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil || chainID <= 0 {
			continue
		}
		seen[chainID] = struct{}{}
	}
	if err := checkpointCur.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint cursor: %w", err)
	}

	out := make([]int64, 0, len(seen))
	for chainID := range seen {
		out = append(out, chainID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func rabbitMQURIForHost(uri string) string {
	const dockerHost = "@rabbitmq:"
	if !strings.Contains(uri, dockerHost) {
		return uri
	}
	rewritten := strings.Replace(uri, dockerHost, "@localhost:", 1)
	log.Printf("restart-trustrank: RABBITMQ_URI host rewrite %q -> %q", uri, rewritten)
	return rewritten
}

func deleteRabbitQueues(uri string, queues []string) error {
	if len(queues) == 0 {
		return nil
	}

	conn, err := rabbitmqclient.NewConn(uri)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	for _, queueName := range queues {
		if _, err := ch.QueueDelete(queueName, false, false, false); err != nil {
			if isRabbitQueueNotFound(err) {
				log.Printf("rabbitmq queue %q: not found (skipped)", queueName)
				continue
			}
			return fmt.Errorf("delete queue %q: %w", queueName, err)
		}
		log.Printf("rabbitmq queue deleted: %q", queueName)
	}
	return nil
}

func isRabbitQueueNotFound(err error) bool {
	if err == nil {
		return false
	}
	if amqpErr, ok := err.(*amqp.Error); ok && amqpErr.Code == 404 {
		return true
	}
	return strings.Contains(err.Error(), "NOT_FOUND")
}
