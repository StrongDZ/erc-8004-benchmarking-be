package main

// restart-trustrank — destructive reset for TrustRank re-processing:
//   1) Stop indexer + trustrank-worker (prevents out-of-order event inserts during replay).
//   2) Delete stale RabbitMQ queues (classified feedback, desc summary, wallet_enrich, per-chain service_uri).
//   3) Drop the entire analyzed agents database (agents, scores, feedbacks, …).
//   4) Delete config documents whose _id matches trustrank_worker_<chain_id>
//      (per-chain TrustRank cursors in the primary DB config collection).
//   5) Recreate queue consumers, wait until TrustRank catches uri_fetched_* on every chain, then start indexer.
//
// Requires .env / MONGO_URI like other tools. Use --dry-run first.
// scripts/restart-trustrank.sh runs `docker compose -f ../docker-compose.all.yaml up --build`
// after success (skipped with --dry-run).

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	bootstrapapp "erc-8004-benchmarking-be/internal/app/uribootstrap"
	"erc-8004-benchmarking-be/internal/config"
	mongoclient "erc-8004-benchmarking-be/internal/infra/mongo"
	rabbitmqclient "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
)

var trustrankCheckpointID = regexp.MustCompile(`^trustrank_worker_(\d+)$`)

const debugLogPath = "/Users/macbook/Documents/Code/Bachelor_Thesis/.cursor/debug-61862d.log"

func main() {
	dryRun := flag.Bool("dry-run", false, "only print what would be done (no writes)")
	yes := flag.Bool("y", false, "skip interactive confirmation")
	noRestart := flag.Bool("no-restart-workers", false, "skip docker compose restart of queue consumers after reset")
	noStopIndexer := flag.Bool("no-stop-indexer", false, "do not stop indexer during replay (not recommended)")
	waitTimeout := flag.Duration("wait-timeout", 90*time.Minute, "max wait for TrustRank replay to catch uri_fetched cursors")
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

	log.Printf("analyzed DB to reset: %q (all collections except %q)", analyzedName, preserveCacheColl)
	log.Printf("checkpoints to delete: %d doc(s) in %q.%q matching trustrank_worker_<chain_id>", delCount, primaryName, configColl)
	log.Printf("rabbitmq queues to delete: %d (%s)", len(queues), strings.Join(queues, ", "))

	if *dryRun {
		log.Print("dry-run: no changes made")
		return
	}

	if !*yes {
		fmt.Fprintf(os.Stderr, "This will delete %d RabbitMQ queue(s), RESET database %q (drop all collections except %q), and delete %d checkpoint(s). Type YES to continue: ",
			len(queues), analyzedName, preserveCacheColl, delCount)
		var confirm string
		if _, err := fmt.Scanln(&confirm); err != nil || confirm != "YES" {
			log.Fatal("restart-trustrank: aborted (expected YES)")
		}
	}

	indexerStopped := false
	if !*noStopIndexer {
		// Stop EVERY worker that writes to the analyzed DB before dropping it.
		// Leaving score-worker / wallet-enrich / feedback-others running during
		// the drop lets them re-insert wallet docs (incl. null-key rows) into the
		// freshly-emptied collection before trustrank-worker can build the unique
		// ux_chain_address index → index build fails → trustrank-worker crash-loops.
		if err := composeStop([]string{
			"indexer", "trustrank-worker", "consumer", "score-worker",
			"wallet-enrich", "feedback-others", "feedback-grader", "uri-resolver",
		}); err != nil {
			log.Fatalf("restart-trustrank: stop workers: %v", err)
		}
		indexerStopped = true
		// #region agent log
		debugLog("H1", "main.go:stop-indexer", "stopped all DB-writer workers before reset", map[string]any{"chains": chainIDs})
		// #endregion
	}

	if err := deleteRabbitQueues(rabbitMQURIForHost(cfg.RabbitMQURI), queues); err != nil {
		log.Fatalf("restart-trustrank: delete rabbitmq queues: %v", err)
	}
	log.Printf("deleted %d rabbitmq queue(s)", len(queues))

	dropped, err := resetAnalyzedExceptCache(ctx, client.Database(analyzedName))
	if err != nil {
		log.Fatalf("restart-trustrank: reset database %q: %v", analyzedName, err)
	}
	log.Printf("reset database %q: dropped %d collection(s), preserved %q", analyzedName, dropped, preserveCacheColl)

	res, err := cfgDB.DeleteMany(ctx, checkpointFilter)
	if err != nil {
		log.Fatalf("restart-trustrank: delete checkpoints: %v", err)
	}
	log.Printf("deleted %d checkpoint document(s) from %q.%q", res.DeletedCount, primaryName, configColl)

	if !*noRestart {
		if err := restartComposeWorkers(); err != nil {
			log.Fatalf("restart-trustrank: restart workers: %v", err)
		}
		if err := waitTrustRankCaughtUp(ctx, client, cfg, chainIDs, *waitTimeout); err != nil {
			log.Fatalf("restart-trustrank: wait for replay: %v", err)
		}
	}

	if indexerStopped {
		if err := composeStart([]string{"indexer"}); err != nil {
			log.Fatalf("restart-trustrank: start indexer: %v", err)
		}
		log.Print("indexer restarted after TrustRank replay completed")
		// #region agent log
		debugLog("H1", "main.go:start-indexer", "indexer restarted after trustrank caught up", nil)
		// #endregion
	}
}

type chainCursor struct {
	BlockNumber uint64
	LogIndex    uint
}

func debugLog(hypothesisID, location, message string, data map[string]any) {
	payload := map[string]any{
		"sessionId":    "61862d",
		"hypothesisId": hypothesisID,
		"location":     location,
		"message":      message,
		"data":         data,
		"timestamp":    time.Now().UnixMilli(),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	f, err := os.OpenFile(debugLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(b, '\n'))
}

func composeFilePath() (string, string, error) {
	beRoot, err := os.Getwd()
	if err != nil {
		return "", "", fmt.Errorf("getwd: %w", err)
	}
	composeFile := filepath.Join(beRoot, "..", "docker-compose.all.yaml")
	if _, err := os.Stat(composeFile); err != nil {
		return "", "", fmt.Errorf("compose file %q: %w", composeFile, err)
	}
	return composeFile, filepath.Dir(composeFile), nil
}

func composeDocker(args ...string) error {
	composeFile, composeDir, err := composeFilePath()
	if err != nil {
		return err
	}
	full := append([]string{"compose", "-f", composeFile}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Dir = composeDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func composeStop(services []string) error {
	args := append([]string{"stop"}, services...)
	if err := composeDocker(args...); err != nil {
		return err
	}
	log.Printf("stopped docker compose services: %s", strings.Join(services, ", "))
	return nil
}

func composeStart(services []string) error {
	args := append([]string{"start"}, services...)
	if err := composeDocker(args...); err != nil {
		return err
	}
	log.Printf("started docker compose services: %s", strings.Join(services, ", "))
	return nil
}

func loadCursor(ctx context.Context, coll *mongo.Collection, key string) (*chainCursor, bool) {
	var doc struct {
		Metadata map[string]any `bson:"metadata"`
	}
	err := coll.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if err != nil {
		return nil, false
	}
	cur := parseChainCursor(doc.Metadata)
	if cur == nil {
		return nil, false
	}
	return cur, true
}

func parseChainCursor(metadata map[string]any) *chainCursor {
	if metadata == nil {
		return nil
	}
	bn, ok1 := metadata["blockNumber"]
	li, ok2 := metadata["logIndex"]
	if !ok1 || !ok2 {
		return nil
	}
	var blockNumber uint64
	var logIndex uint
	switch v := bn.(type) {
	case float64:
		blockNumber = uint64(v)
	case int64:
		blockNumber = uint64(v)
	case int32:
		blockNumber = uint64(v)
	default:
		return nil
	}
	switch v := li.(type) {
	case float64:
		logIndex = uint(v)
	case int64:
		logIndex = uint(v)
	case int32:
		logIndex = uint(v)
	default:
		return nil
	}
	return &chainCursor{BlockNumber: blockNumber, LogIndex: logIndex}
}

func cursorAtOrBeyond(tr, uri *chainCursor) bool {
	if tr == nil {
		return false
	}
	if uri == nil {
		return true
	}
	if tr.BlockNumber > uri.BlockNumber {
		return true
	}
	if tr.BlockNumber < uri.BlockNumber {
		return false
	}
	return tr.LogIndex >= uri.LogIndex
}

// waitTrustRankCaughtUp polls until every chain's trustrank_worker cursor reaches uri_fetched.
func waitTrustRankCaughtUp(ctx context.Context, client *mongo.Client, cfg config.Config, chainIDs []int64, timeout time.Duration) error {
	cfgColl := client.Database(cfg.MongoDatabase).Collection(cfg.ConfigColl)
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	log.Printf("waiting for TrustRank replay (timeout %s)...", timeout)

	for {
		pending := make([]int64, 0)
		status := make(map[string]any)

		for _, chainID := range chainIDs {
			uriKey := bootstrapapp.URIFetchedCursorKey(chainID)
			trKey := fmt.Sprintf("trustrank_worker_%d", chainID)
			uriCur, hasURI := loadCursor(ctx, cfgColl, uriKey)
			trCur, _ := loadCursor(ctx, cfgColl, trKey)
			if !hasURI {
				continue
			}
			if !cursorAtOrBeyond(trCur, uriCur) {
				pending = append(pending, chainID)
				status[fmt.Sprintf("%d", chainID)] = map[string]any{
					"trustrank": trCur,
					"uri":       uriCur,
				}
			}
		}

		if len(pending) == 0 {
			log.Print("TrustRank replay caught up on all chains")
			// #region agent log
			debugLog("H1", "main.go:wait-caught-up", "all chains caught up", map[string]any{"chains": chainIDs})
			// #endregion
			return nil
		}

		if time.Now().After(deadline) {
			// #region agent log
			debugLog("H1", "main.go:wait-timeout", "replay timeout", map[string]any{"pending": pending, "status": status})
			// #endregion
			return fmt.Errorf("timeout after %s; pending chains: %v", timeout, pending)
		}

		log.Printf("TrustRank replay pending on %d chain(s): %v", len(pending), pending)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// restartComposeWorkers recycles RabbitMQ consumers so they re-declare queues after
// restart-trustrank deletes them. Without this, wallet-enrich / feedback-others die
// with "delivery channel closed" while containers stay Up.
func restartComposeWorkers() error {
	// Start trustrank-worker ALONE first so it (re)builds the wallets unique index
	// ux_chain_address on the freshly-emptied collection before any other writer
	// (wallet-enrich / feedback-others / score-worker) can insert wallet docs.
	// Once the unique index exists it rejects duplicate null-key rows, so the later
	// workers can start safely.
	if err := composeDocker("up", "-d", "--no-deps", "trustrank-worker"); err != nil {
		return err
	}
	log.Print("started trustrank-worker first; waiting for it to build wallets indexes")
	time.Sleep(12 * time.Second)

	services := []string{
		"uri-resolver",
		"wallet-enrich",
		"feedback-others",
		"feedback-grader",
		"score-worker",
		"consumer",
	}
	args := append([]string{"up", "-d", "--no-deps"}, services...)
	if err := composeDocker(args...); err != nil {
		return err
	}
	log.Printf("recreated docker compose services: %s", strings.Join(services, ", "))
	return nil
}

func buildResetQueues(chainIDs []int64) []string {
	queues := []string{
		mq.QueueFeedbackOthers,
		mq.QueueFeedbackClassified,
		mq.QueueWalletEnrich,
	}
	for _, chainID := range chainIDs {
		queues = append(queues, mq.ServiceURIQueueName(chainID))
	}
	return queues
}

// preserveCacheColl is the ai-service runtime LLM classify cache (erc-8004-ai-service
// app/cache.py, collection "classify_cache"). It is expensive-to-rebuild LLM output,
// NOT event-derived data, so a re-derivation must preserve it — dropping it forces
// every re-run to re-classify the whole others bucket from scratch through the LLM.
const preserveCacheColl = "classify_cache"

// resetAnalyzedExceptCache drops every collection in the analyzed DB EXCEPT the LLM
// classify cache, returning the count dropped. Replaces a whole-database Drop so the
// cache survives the re-derivation.
func resetAnalyzedExceptCache(ctx context.Context, db *mongo.Database) (int, error) {
	names, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("list collections: %w", err)
	}
	dropped := 0
	for _, name := range names {
		if name == preserveCacheColl {
			continue
		}
		if err := db.Collection(name).Drop(ctx); err != nil {
			return dropped, fmt.Errorf("drop %q: %w", name, err)
		}
		dropped++
	}
	return dropped, nil
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
