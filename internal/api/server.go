package api

// server.go — REST API router + dependency wiring.
// Uses stdlib net/http with Go 1.22 pattern-based path parameters (no Gin needed).

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	mongodrv "go.mongodb.org/mongo-driver/mongo"

	apicache "erc-8004-benchmarking-be/internal/api/cache"
	"erc-8004-benchmarking-be/internal/api/handlers"
	"erc-8004-benchmarking-be/internal/api/middleware"
	"erc-8004-benchmarking-be/internal/api/service"
	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	domainuri "erc-8004-benchmarking-be/internal/domain/uri"
	httpclient "erc-8004-benchmarking-be/internal/infra/https"
	redisinfra "erc-8004-benchmarking-be/internal/infra/redis"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	contractsrepo "erc-8004-benchmarking-be/internal/repository/contracts"
	crawlerrepo "erc-8004-benchmarking-be/internal/repository/crawler"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	oasfschema "erc-8004-benchmarking-be/internal/repository/oasfschema"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
	scorestatsrepo "erc-8004-benchmarking-be/internal/repository/scorestats"
	walletrepo "erc-8004-benchmarking-be/internal/repository/wallet"
	wsock "erc-8004-benchmarking-be/internal/websocket"

	httpSwagger "github.com/swaggo/http-swagger"
)

// rawFetchAdapter adapts internal/infra/https.Client to domainuri.RawFetcher.
type rawFetchAdapter struct {
	c *httpclient.Client
}

func (a *rawFetchAdapter) Fetch(ctx context.Context, url string) ([]byte, error) {
	body, _, _, err := a.c.FetchBody(ctx, url)
	return body, err
}

// Repositories groups all read-side repos the API depends on.
type Repositories struct {
	Agents      *agentrepo.Repository
	Feedback    *feedbackrepo.Repository
	ScoreStats  *scorestatsrepo.Repository
	Identity    *identityrepo.Repository
	Events      *eventrepo.Repository
	Offchain    *offchainrepo.Repository
	Crawlers    *crawlerrepo.Repository
	Contracts   *contractsrepo.ContractsRepository
	Wallets     *walletrepo.Repository
	OASFSkills  *oasfschema.Repository
	OASFDomains *oasfschema.Repository
}

// NewRepositories wires all repositories against both the primary and analyzed databases.
//   - Raw/crawler data (events, contracts, crawlers, offchain_data) lives in the primary DB.
//   - Analyzed data (agents, feedback_history, identity_history, agent_score_stats) lives in
//     the analyzed DB.
func NewRepositories(client *mongodrv.Client, cfg config.Config) *Repositories {
	primary := client.Database(cfg.MongoDatabase)
	analyzed := client.Database(cfg.AnalyzedDatabase)
	return &Repositories{
		Agents:      agentrepo.NewRepository(analyzed, cfg.AgentsColl, cfg.ScoreStatsColl),
		Feedback:    feedbackrepo.NewRepository(analyzed, cfg.FeedbackHistColl),
		ScoreStats:  scorestatsrepo.NewRepository(analyzed, cfg.ScoreStatsColl),
		Identity:    identityrepo.NewRepository(analyzed, cfg.IdentityHistColl),
		Events:      eventrepo.NewRepository(primary, cfg.EventsColl),
		Offchain:    offchainrepo.NewRepository(primary, cfg.OffchainColl),
		Crawlers:    crawlerrepo.NewRepository(primary, cfg.CrawlersColl),
		Contracts:   contractsrepo.NewContractsRepository(primary, cfg.ContractsColl),
		Wallets:     walletrepo.NewRepository(analyzed, cfg.WalletColl),
		OASFSkills:  oasfschema.NewRepository(primary, "oasf_skills"),
		OASFDomains: oasfschema.NewRepository(primary, "oasf_domains"),
	}
}

// Server wraps the http.Server with its router.
type Server struct {
	httpServer *http.Server
	cfg        config.Config
	hub        *wsock.Hub         // may be nil when WS is disabled
	redis      *redisinfra.Client // may be nil when realtime is disabled
	mem        *apicache.Memory
}

// NewServer builds the router, applies middleware, and returns a ready Server.
// The optional redis client enables realtime WS fan-out by subscribing to
// cfg.RedisEventsChannel and forwarding payloads to Hub.Broadcast.
func NewServer(cfg config.Config, repos *Repositories, redis *redisinfra.Client) *Server {
	formula := scoring.DefaultFormulaConfig()
	formula.Alpha = cfg.TrustRankAlpha
	formula.TBaseDays = cfg.TrustRankTBase
	formula.C = cfg.ConfidenceC
	formula.Gamma = cfg.PenaltyGamma
	formula.Theta = cfg.PenaltyTheta
	formula.AdoptionURef = cfg.AdoptionURef

	composite := scoring.CompositeWeights{
		Reputation: cfg.ScoreWeightReputation,
		Adoption:   cfg.ScoreWeightAdoption,
		Services:   cfg.ScoreWeightServices,
		Publisher:  cfg.ScoreWeightPublisher,
		Compliance: cfg.ScoreWeightCompliance,
	}

	// Services
	lbSvc := service.NewLeaderboard(service.LeaderboardDeps{
		Agents: repos.Agents, Feedback: repos.Feedback, Scores: repos.ScoreStats,
		Crawlers: repos.Crawlers, Wallets: repos.Wallets, Formula: formula,
	})
	// URI resolver for on-demand service-endpoint reconnects.
	httpCl := httpclient.NewClientWithOptions(httpclient.ClientOptions{
		Timeout:   cfg.HTTPSFetchTimeout,
		UserAgent: cfg.HTTPSUserAgent,
	})
	resolver := domainuri.NewResolver(&rawFetchAdapter{c: httpCl}, cfg.IPFSGateway, cfg.ArweaveGateway)

	agentSvc := service.NewAgent(service.AgentDeps{
		Agents: repos.Agents, Feedback: repos.Feedback, ScoreStats: repos.ScoreStats,
		Identity: repos.Identity, Events: repos.Events, Offchain: repos.Offchain,
		Contracts: repos.Contracts, Wallet: repos.Wallets, Resolver: resolver,
		Formula: formula, Composite: composite,
	})
	oasfSvc := service.NewOASF(service.OASFDeps{
		Agents:            repos.Agents,
		ScoreStats:        repos.ScoreStats,
		Formula:           formula,
		OASFSchemaSkills:  repos.OASFSkills,
		OASFSchemaDomains: repos.OASFDomains,
	})
	chainSvc := service.NewChain(service.ChainDeps{Contracts: repos.Contracts, Agents: repos.Agents})
	adminSvc := service.NewAdmin(service.AdminDeps{
		Crawlers: repos.Crawlers, Events: repos.Events, Feedback: repos.Feedback,
		Agents: repos.Agents, Contracts: repos.Contracts,
	})
	walletSvc := service.NewWallet(service.WalletDeps{
		Agents: repos.Agents, Feedback: repos.Feedback, Wallet: repos.Wallets,
	})

	// Handlers
	lbH := handlers.NewLeaderboardHandler(lbSvc)
	agH := handlers.NewAgentHandler(agentSvc)
	oasfH := handlers.NewOASFHandler(oasfSvc)
	chainH := handlers.NewChainHandler(chainSvc)
	adminH := handlers.NewAdminHandler(adminSvc)
	walletH := handlers.NewWalletHandler(walletSvc)

	// Response cache — shared across endpoints with per-endpoint TTLs.
	mem := apicache.NewMemory(30 * time.Second)
	c30s := middleware.ResponseCache(mem, 30*time.Second)
	c60s := middleware.ResponseCache(mem, 60*time.Second)
	c2m := middleware.ResponseCache(mem, 2*time.Minute)
	c5m := middleware.ResponseCache(mem, 5*time.Minute)
	c1h := middleware.ResponseCache(mem, 1*time.Hour)

	mux := http.NewServeMux()

	// Health check (outside /api/v1).
	mux.HandleFunc("GET /health", healthCheck)
	mux.Handle("GET /swagger/", httpSwagger.WrapHandler)

	// /api/v1/leaderboard*
	mux.Handle("GET /api/v1/leaderboard", c30s(http.HandlerFunc(lbH.List)))
	mux.HandleFunc("GET /api/v1/leaderboard/search", lbH.Search)
	mux.Handle("GET /api/v1/leaderboard/stats", c30s(http.HandlerFunc(lbH.Stats)))
	mux.Handle("GET /api/v1/leaderboard/rising-stars", c60s(http.HandlerFunc(lbH.RisingStars)))
	mux.Handle("GET /api/v1/leaderboard/wallet-ranking", c2m(http.HandlerFunc(lbH.WalletRanking)))
	mux.Handle("GET /api/v1/leaderboard/tags", c2m(http.HandlerFunc(lbH.Tags)))

	// /api/v1/agents/...
	mux.Handle("GET /api/v1/agents/{chainId}/{agentId}", c30s(http.HandlerFunc(agH.Profile)))
	mux.Handle("GET /api/v1/agents/{chainId}/{agentId}/overview", c30s(http.HandlerFunc(agH.Overview)))
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/feedbacks", agH.Feedbacks)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/feedback-clients", agH.FeedbackClients)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/feedbacks/{feedbackId}", agH.FeedbackDetail)
	mux.HandleFunc("GET /api/v1/offchain-by-uri", agH.OffchainByURI)
	mux.Handle("GET /api/v1/agents/{chainId}/{agentId}/identity-history", c2m(http.HandlerFunc(agH.IdentityHistory)))
	mux.Handle("GET /api/v1/agents/{chainId}/{agentId}/trust-score-history", c2m(http.HandlerFunc(agH.TrustScoreHistory)))
	mux.Handle("GET /api/v1/agents/{chainId}/{agentId}/activity-heatmap", c2m(http.HandlerFunc(agH.ActivityHeatmap)))
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/penalties", agH.Penalties)
	mux.Handle("GET /api/v1/agents/{chainId}/{agentId}/related", c60s(http.HandlerFunc(agH.Related)))
	mux.Handle("GET /api/v1/agents/{chainId}/{agentId}/radar", c60s(http.HandlerFunc(agH.Radar)))
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/proof/{txHash}", agH.Proof)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/anti-spam", agH.AntiSpam)
	mux.Handle("GET /api/v1/agents/{chainId}/{agentId}/registrations", c30s(http.HandlerFunc(agH.Registrations)))
	mux.HandleFunc("POST /api/v1/agents/{chainId}/{agentId}/services/reconnect", agH.ReconnectServiceEndpoint)

	// /api/v1/oasf/*
	mux.Handle("GET /api/v1/oasf/facets", c5m(http.HandlerFunc(oasfH.Facets)))
	mux.Handle("GET /api/v1/oasf/skills", c2m(http.HandlerFunc(oasfH.Skills)))
	mux.Handle("GET /api/v1/oasf/domains", c2m(http.HandlerFunc(oasfH.Domains)))
	mux.Handle("GET /api/v1/oasf/schema/skills", c1h(http.HandlerFunc(oasfH.SchemaSkills)))
	mux.Handle("GET /api/v1/oasf/schema/domains", c1h(http.HandlerFunc(oasfH.SchemaDomains)))

	// /api/v1/chains/*
	mux.Handle("GET /api/v1/chains", c5m(http.HandlerFunc(chainH.List)))
	mux.HandleFunc("GET /api/v1/chains/{chainId}/contracts", chainH.Contracts)

	// /api/v1/wallet/:address/*
	mux.Handle("GET /api/v1/wallet/{address}", c30s(http.HandlerFunc(walletH.Profile)))
	mux.HandleFunc("GET /api/v1/wallet/{address}/feedbacks", walletH.FeedbackGiven)
	mux.HandleFunc("GET /api/v1/wallet/{address}/feedback-agents", walletH.FeedbackAgents)

	// /api/v1/wallets/*
	mux.Handle("GET /api/v1/wallets/ens", c30s(http.HandlerFunc(walletH.WalletsENS)))

	// /api/v1/admin/* (gated)
	adminAuth := middleware.AdminAuth(cfg.AdminAPIKey)
	// Indexer status is read-only observability and intentionally public.
	mux.HandleFunc("GET /api/v1/admin/indexer-status", adminH.IndexerStatus)
	mux.Handle("POST /api/v1/admin/scoring/recompute", adminAuth(http.HandlerFunc(adminH.RecomputeScoring)))
	mux.Handle("POST /api/v1/admin/simulator/start", adminAuth(http.HandlerFunc(adminH.SimulatorStart)))

	// /api/v1/ws — realtime event stream (WebSocket upgrade).
	// Hub runs inside Server.Start; clients are upgraded & registered here.
	hub := wsock.NewHub()
	wsHandler := hub.ServeWS(wsock.ServeWSOptions{AllowedOrigins: cfg.APIAllowedOrigins})
	// NOTE: WS upgrade must bypass standard HTTP timeouts and CORS preflight; gorilla
	// hijacks the connection so no response writes happen through the default
	// write-deadline path. We still wrap with RequestID/Logger/Recover via the
	// global chain below, which is safe for upgrades.
	mux.HandleFunc("GET /api/v1/ws", wsHandler)

	rlCfg := middleware.DefaultRateLimitConfig()
	rlCfg.RequestsPerSecond = cfg.RateLimitRPS
	rlCfg.Burst = cfg.RateLimitBurst

	chain := middleware.Chain(
		middleware.RequestID,
		middleware.Logger,
		middleware.Recover,
		middleware.CORS(cfg.APIAllowedOrigins),
		middleware.RateLimit(rlCfg),
	)
	handler := chain(mux)

	return &Server{
		cfg:   cfg,
		hub:   hub,
		redis: redis,
		mem:   mem,
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", cfg.APIPort),
			Handler:      handler,
			ReadTimeout:  cfg.APIReadTimeout,
			WriteTimeout: cfg.APIWriteTimeout,
		},
	}
}

// Start runs the HTTP server and blocks until ctx is cancelled or ListenAndServe errors.
// A graceful shutdown of cfg.APIShutdownTimeout is attempted before returning.
// Start also launches the WS hub and, when a Redis client is available, a Redis
// Pub/Sub -> Hub forwarder keyed on cfg.RedisEventsChannel.
func (s *Server) Start(ctx context.Context) error {
	hubCtx, hubCancel := context.WithCancel(ctx)
	defer hubCancel()

	if s.hub != nil {
		go s.hub.Run(hubCtx)
	}

	// Cache janitor — evicts expired entries every 5 minutes.
	janitorStop := make(chan struct{})
	defer close(janitorStop)
	s.mem.StartJanitor(janitorStop, 5*time.Minute)

	// Bridge Redis Pub/Sub -> WebSocket Hub. This is the only coupling between
	// the consumer (publisher) and the API (subscriber) for realtime events.
	if s.hub != nil && s.redis != nil && s.cfg.RedisEventsChannel != "" {
		ch, unsub, err := s.redis.Subscribe(hubCtx, s.cfg.RedisEventsChannel)
		if err != nil {
			log.Printf("api: ws realtime disabled (redis subscribe failed): %v", err)
		} else {
			defer unsub()
			go func() {
				log.Printf("api: ws realtime subscribed; channel=%s", s.cfg.RedisEventsChannel)
				for payload := range ch {
					s.hub.Broadcast(payload)
				}
				log.Printf("api: ws realtime subscription closed")
			}()
		}
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("api listening on %s", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.APIShutdownTimeout)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("api: graceful shutdown: %w", err)
		}
		return nil
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		return fmt.Errorf("api: serve: %w", err)
	}
}

func healthCheck(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok","service":"erc-8004-benchmarking-api","time":"` + time.Now().UTC().Format(time.RFC3339) + `"}`))
}
