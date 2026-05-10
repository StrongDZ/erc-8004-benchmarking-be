package evm

// crawler.go — Core EVM event crawler.
// Queries logs for each configured (chain, contract) pair and publishes RawLogMessages.

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"golang.org/x/sync/semaphore"

	rabbitmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	"erc-8004-benchmarking-be/internal/repository/contracts"
	"erc-8004-benchmarking-be/internal/repository/crawler"
	"erc-8004-benchmarking-be/internal/utils"
	"erc-8004-benchmarking-be/pkg/retry"
)

type crawlJob struct {
	chainID int64
	rpcs    []string
	target  contracts.ContractTarget
}

// Crawler polls EVM chains for contract events and publishes them as RawLogMessages.
type Crawler struct {
	contractsRepo     *contracts.ContractsRepository
	crawlersRepo      *crawler.Repository
	publisher         *rabbitmqinfra.Publisher
	batchSize         uint64
	rpcTimeout        time.Duration
	maxConcurrency    int64
	rpcMaxRetries     int
	retryBackoffStart time.Duration
	retryStrategy     retry.BackoffStrategy
	breakers          *BreakerRegistry

	jobMu    sync.Mutex
	jobLocks map[string]*sync.Mutex
}

// NewCrawler constructs a Crawler with the supplied dependencies and tuning parameters.
func NewCrawler(
	contractsRepo *contracts.ContractsRepository,
	crawlersRepo *crawler.Repository,
	publisher *rabbitmqinfra.Publisher,
	batchSize uint64,
	rpcTimeout time.Duration,
	maxConcurrency int,
	rpcMaxRetries int,
	retryBackoffStart time.Duration,
	retryStrategy retry.BackoffStrategy,
) *Crawler {
	mc := int64(maxConcurrency)
	if mc < 1 {
		mc = 1
	}
	if rpcMaxRetries < 0 {
		rpcMaxRetries = 0
	}
	if retryBackoffStart <= 0 {
		retryBackoffStart = 100 * time.Millisecond
	}
	return &Crawler{
		contractsRepo:     contractsRepo,
		crawlersRepo:      crawlersRepo,
		publisher:         publisher,
		batchSize:         batchSize,
		rpcTimeout:        rpcTimeout,
		maxConcurrency:    mc,
		rpcMaxRetries:     rpcMaxRetries,
		retryBackoffStart: retryBackoffStart,
		retryStrategy:     retryStrategy,
		breakers:          NewBreakerRegistry(DefaultBreakerConfig()),
		jobLocks:          make(map[string]*sync.Mutex),
	}
}

func (c *Crawler) lockJob(chainID int64, contractLower string) func() {
	key := fmt.Sprintf("%d:%s", chainID, contractLower)
	c.jobMu.Lock()
	mu, ok := c.jobLocks[key]
	if !ok {
		mu = &sync.Mutex{}
		c.jobLocks[key] = mu
	}
	c.jobMu.Unlock()
	mu.Lock()
	return mu.Unlock
}

// RunOnce executes one crawl cycle across all active chain/contract configs.
func (c *Crawler) RunOnce(ctx context.Context) error {
	configs, err := c.contractsRepo.FindActive(ctx)
	if err != nil {
		return err
	}

	jobs := make([]crawlJob, 0)
	for _, cfg := range configs {
		rpcs := normalizeRPCs(cfg.RPCs)
		for _, target := range cfg.Contracts {
			address := common.HexToAddress(target.Address)
			if (address == common.Address{}) {
				log.Printf("chain=%d invalid contract address=%s, skip", cfg.ChainID, target.Address)
				continue
			}
			jobs = append(jobs, crawlJob{
				chainID: cfg.ChainID,
				rpcs:    rpcs,
				target:  target,
			})
		}
	}

	if len(jobs) == 0 {
		return nil
	}

	sem := semaphore.NewWeighted(c.maxConcurrency)
	var wg sync.WaitGroup
	var cycleMu sync.Mutex
	var cycleErr error

	for _, j := range jobs {
		j := j
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr := strings.ToLower(common.HexToAddress(j.target.Address).Hex())
			unlockJob := c.lockJob(j.chainID, addr)
			defer unlockJob()

			if err := sem.Acquire(ctx, 1); err != nil {
				cycleMu.Lock()
				if cycleErr == nil {
					cycleErr = fmt.Errorf("semaphore acquire: %w", err)
				}
				cycleMu.Unlock()
				return
			}
			defer sem.Release(1)

			log.Printf("chain=%d contract=%s type=%s rpc_count=%d (crawl start)", j.chainID, addr, j.target.Type, len(j.rpcs))

			breaker := c.breakers.Get(j.chainID)
			if err := breaker.Allow(); err != nil {
				log.Printf("chain=%d contract=%s circuit open, skipping: %v", j.chainID, addr, err)
				return
			}
			if err := c.crawlEvents(ctx, j.rpcs, j.chainID, j.target); err != nil {
				breaker.RecordFailure()
				log.Printf("chain=%d contract=%s event crawl error: %v", j.chainID, addr, err)
				wrapped := fmt.Errorf("chain=%d contract=%s: %w", j.chainID, addr, err)
				cycleMu.Lock()
				if cycleErr == nil {
					cycleErr = wrapped
				}
				cycleMu.Unlock()
			} else {
				breaker.RecordSuccess()
			}
		}()
	}
	wg.Wait()
	return cycleErr
}

// Run calls RunOnce in a loop until ctx is cancelled.
// On success sleeps livePoll; on any error sleeps errorPoll.
func (c *Crawler) Run(ctx context.Context, livePoll, errorPoll time.Duration) {
	if livePoll <= 0 {
		livePoll = 5 * time.Second
	}
	if errorPoll <= 0 {
		errorPoll = 30 * time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		err := c.RunOnce(ctx)
		wait := livePoll
		if err != nil {
			log.Printf("crawler Run: cycle finished with error, backing off %s: %v", errorPoll, err)
			wait = errorPoll
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (c *Crawler) crawlEvents(ctx context.Context, rpcs []string, chainID int64, target contracts.ContractTarget) error {
	if len(rpcs) == 0 {
		return fmt.Errorf("no RPC endpoints for chain=%d contract=%s", chainID, target.Address)
	}

	address := common.HexToAddress(target.Address)
	contractAddress := strings.ToLower(address.Hex())

	sess := &rpcRetrySession{c: c, ctx: ctx, rpcs: rpcs, chainID: chainID, contract: contractAddress}
	defer sess.close()

	var latest uint64
	var err error
	latest, err = doWithRetryValue(sess, func(cl *ethclient.Client) (uint64, error) {
		return cl.BlockNumber(ctx)
	})
	if err != nil {
		return err
	}

	topic0s := EventTopic0Hashes(target.EventABI)
	var topics [][]common.Hash
	if len(topic0s) > 0 {
		topics = [][]common.Hash{topic0s}
	} else if len(target.EventABI) > 0 {
		log.Printf("chain=%d contract=%s: eventABI has no hashable non-anonymous events; crawling all logs for address", chainID, contractAddress)
	}

	mode := crawler.CrawlModeEvent
	state, err := c.crawlersRepo.Get(ctx, chainID, contractAddress, mode)
	if err != nil {
		return err
	}

	var startBlock uint64
	if state == nil {
		startBlock = target.StartBlock
	} else {
		startBlock = state.LastProcessedBlock + 1
	}

	var ranges []utils.BlockRange
	if startBlock <= latest {
		ranges = utils.BuildForwardRanges(startBlock, latest, c.batchSize)
	}

	if len(ranges) == 0 && state != nil {
		if err := c.crawlersRepo.Upsert(ctx, crawler.CrawlerState{
			ChainID:            chainID,
			ContractAddress:    contractAddress,
			Mode:               mode,
			LastProcessedBlock: state.LastProcessedBlock,
			Status:             "ok",
			ActiveRPC:          sess.activeURL(),
		}); err != nil {
			return err
		}
		return nil
	}

	for _, r := range ranges {
		var logs []types.Log
		logs, err := doWithRetryValue(sess, func(cl *ethclient.Client) ([]types.Log, error) {
			return cl.FilterLogs(ctx, ethereum.FilterQuery{
				FromBlock: new(big.Int).SetUint64(r.From),
				ToBlock:   new(big.Int).SetUint64(r.To),
				Addresses: []common.Address{address},
				Topics:    topics,
			})
		})
		if err != nil {
			return fmt.Errorf("filter logs [%d..%d]: %w", r.From, r.To, err)
		}

		ingestedAt := time.Now().Unix()
		blockTimeCache := make(map[uint64]int64)
		for _, lg := range logs {
			ts, ok := blockTimeCache[lg.BlockNumber]
			if !ok {
				var tsErr error
				ts, tsErr = c.blockTimestampUnix(ctx, sess, lg.BlockNumber)
				if tsErr != nil {
					return tsErr
				}
				blockTimeCache[lg.BlockNumber] = ts
			}

			logTopics := make([]string, 0, len(lg.Topics))
			for _, t := range lg.Topics {
				logTopics = append(logTopics, t.Hex())
			}

			msg := mq.RawLogMessage{
				ChainID:         chainID,
				ContractAddress: strings.ToLower(lg.Address.Hex()),
				ContractType:    string(target.Type),
				EventABI:        target.EventABI,
				BlockNumber:     lg.BlockNumber,
				BlockHash:       lg.BlockHash.Hex(),
				TxHash:          lg.TxHash.Hex(),
				LogIndex:        lg.Index,
				Topics:          logTopics,
				Data:            common.Bytes2Hex(lg.Data),
				Removed:         lg.Removed,
				Timestamp:       ts,
				IngestedAt:      ingestedAt,
			}
			if err := c.publisher.Publish(ctx, msg); err != nil {
				return fmt.Errorf("publish log (tx=%s log=%d): %w", lg.TxHash.Hex(), lg.Index, err)
			}
		}

		if err := c.crawlersRepo.Upsert(ctx, crawler.CrawlerState{
			ChainID:            chainID,
			ContractAddress:    contractAddress,
			Mode:               mode,
			LastProcessedBlock: r.To,
			Status:             "ok",
			ActiveRPC:          sess.activeURL(),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (c *Crawler) blockTimestampUnix(ctx context.Context, sess *rpcRetrySession, blockNumber uint64) (int64, error) {
	ts, err := doWithRetryValue(sess, func(cl *ethclient.Client) (int64, error) {
		header, e := cl.HeaderByNumber(ctx, new(big.Int).SetUint64(blockNumber))
		if e != nil {
			return 0, e
		}
		return int64(header.Time), nil
	})
	if err != nil {
		return 0, fmt.Errorf("fetch block header %d: %w", blockNumber, err)
	}
	return ts, nil
}

func normalizeRPCs(rpcs []string) []string {
	out := make([]string, 0, len(rpcs))
	for _, rpc := range rpcs {
		v := strings.TrimSpace(rpc)
		if v == "" {
			continue
		}
		out = append(out, v)
	}
	return slices.Compact(out)
}
