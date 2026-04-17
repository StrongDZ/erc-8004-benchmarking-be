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

	"erc-8004-benchmarking-be/internal/api/handlers"
	"erc-8004-benchmarking-be/internal/api/middleware"
	"erc-8004-benchmarking-be/internal/api/service"
	"erc-8004-benchmarking-be/internal/config"
	"erc-8004-benchmarking-be/internal/domain/scoring"
	redisinfra "erc-8004-benchmarking-be/internal/infra/redis"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
	configrepo "erc-8004-benchmarking-be/internal/repository/config"
	crawlerrepo "erc-8004-benchmarking-be/internal/repository/crawler"
	eventrepo "erc-8004-benchmarking-be/internal/repository/event"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
	identityrepo "erc-8004-benchmarking-be/internal/repository/identity"
	offchainrepo "erc-8004-benchmarking-be/internal/repository/offchain"
	scorerepo "erc-8004-benchmarking-be/internal/repository/score"
	wsock "erc-8004-benchmarking-be/internal/websocket"
	httpSwagger "github.com/swaggo/http-swagger"
)

// Repositories groups all read-side repos the API depends on.
type Repositories struct {
	Agents    *agentrepo.Repository
	Feedback  *feedbackrepo.Repository
	Scores    *scorerepo.Repository
	Identity  *identityrepo.Repository
	Events    *eventrepo.Repository
	Offchain  *offchainrepo.Repository
	Crawlers  *crawlerrepo.Repository
	Contracts *configrepo.ContractsRepository
}

// NewRepositories wires all repositories against both the primary and analyzed databases.
//   - Raw/crawler data (events, contracts, crawlers, offchain_data) lives in the primary DB.
//   - Analyzed data (agents, feedback_history, identity_history, score_history) lives in
//     the analyzed DB.
func NewRepositories(client *mongodrv.Client, cfg config.Config) *Repositories {
	primary := client.Database(cfg.MongoDatabase)
	analyzed := client.Database(cfg.AnalyzedDatabase)
	return &Repositories{
		Agents:    agentrepo.NewRepository(analyzed, cfg.AgentsColl),
		Feedback:  feedbackrepo.NewRepository(analyzed, cfg.FeedbackHistColl),
		Scores:    scorerepo.NewRepository(analyzed, cfg.ScoreHistColl),
		Identity:  identityrepo.NewRepository(analyzed, cfg.IdentityHistColl),
		Events:    eventrepo.NewRepository(primary, cfg.EventsColl),
		Offchain:  offchainrepo.NewRepository(primary, cfg.OffchainColl),
		Crawlers:  crawlerrepo.NewRepository(primary, cfg.CrawlersColl),
		Contracts: configrepo.NewContractsRepository(primary, cfg.ContractsColl),
	}
}

// Server wraps the http.Server with its router.
type Server struct {
	httpServer *http.Server
	cfg        config.Config
	hub        *wsock.Hub       // may be nil when WS is disabled
	redis      *redisinfra.Client // may be nil when realtime is disabled
}

// NewServer builds the router, applies middleware, and returns a ready Server.
// The optional redis client enables realtime WS fan-out by subscribing to
// cfg.RedisEventsChannel and forwarding payloads to Hub.Broadcast.
func NewServer(cfg config.Config, repos *Repositories, redis *redisinfra.Client) *Server {
	formula := scoring.DefaultFormulaConfig()
	formula.Alpha = cfg.TrustRankAlpha
	formula.Beta = cfg.TrustRankBeta
	formula.K = cfg.TrustRankK
	formula.TBaseDays = cfg.TrustRankTBase
	formula.Gamma = cfg.PenaltyGamma
	formula.Theta = cfg.PenaltyTheta

	// Services
	lbSvc := service.NewLeaderboard(service.LeaderboardDeps{
		Agents: repos.Agents, Feedback: repos.Feedback, Scores: repos.Scores,
		Crawlers: repos.Crawlers, Formula: formula,
	})
	agentSvc := service.NewAgent(service.AgentDeps{
		Agents: repos.Agents, Feedback: repos.Feedback, Scores: repos.Scores,
		Identity: repos.Identity, Events: repos.Events,
		Contracts: repos.Contracts, Formula: formula,
	})
	oasfSvc := service.NewOASF(service.OASFDeps{Agents: repos.Agents, Formula: formula})
	chainSvc := service.NewChain(service.ChainDeps{Contracts: repos.Contracts, Agents: repos.Agents})
	adminSvc := service.NewAdmin(service.AdminDeps{
		Crawlers: repos.Crawlers, Events: repos.Events, Feedback: repos.Feedback,
	})

	// Handlers
	lbH := handlers.NewLeaderboardHandler(lbSvc)
	agH := handlers.NewAgentHandler(agentSvc)
	oasfH := handlers.NewOASFHandler(oasfSvc)
	chainH := handlers.NewChainHandler(chainSvc)
	adminH := handlers.NewAdminHandler(adminSvc)

	mux := http.NewServeMux()

	// Health check (outside /api/v1).
	mux.HandleFunc("GET /health", healthCheck)
	mux.Handle("GET /swagger/", httpSwagger.WrapHandler)

	// /api/v1/leaderboard*
	mux.HandleFunc("GET /api/v1/leaderboard", lbH.List)
	mux.HandleFunc("GET /api/v1/leaderboard/search", lbH.Search)
	mux.HandleFunc("GET /api/v1/leaderboard/stats", lbH.Stats)
	mux.HandleFunc("GET /api/v1/leaderboard/rising-stars", lbH.RisingStars)
	mux.HandleFunc("GET /api/v1/leaderboard/tags", lbH.Tags)

	// /api/v1/agents/...
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}", agH.Profile)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/score-history", agH.ScoreHistory)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/feedbacks", agH.Feedbacks)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/feedbacks/{feedbackId}", agH.FeedbackDetail)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/identity-history", agH.IdentityHistory)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/activity-heatmap", agH.ActivityHeatmap)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/penalties", agH.Penalties)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/related", agH.Related)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/radar", agH.Radar)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/proof/{txHash}", agH.Proof)
	mux.HandleFunc("GET /api/v1/agents/{chainId}/{agentId}/anti-spam", agH.AntiSpam)

	// /api/v1/oasf/*
	mux.HandleFunc("GET /api/v1/oasf/facets", oasfH.Facets)
	mux.HandleFunc("GET /api/v1/oasf/skills", oasfH.Skills)
	mux.HandleFunc("GET /api/v1/oasf/domains", oasfH.Domains)

	// /api/v1/chains/*
	mux.HandleFunc("GET /api/v1/chains", chainH.List)
	mux.HandleFunc("GET /api/v1/chains/{chainId}/contracts", chainH.Contracts)

	// /api/v1/admin/* (gated)
	adminAuth := middleware.AdminAuth(cfg.AdminAPIKey)
	mux.Handle("GET /api/v1/admin/indexer-status", adminAuth(http.HandlerFunc(adminH.IndexerStatus)))
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

	chain := middleware.Chain(
		middleware.RequestID,
		middleware.Logger,
		middleware.Recover,
		middleware.CORS(cfg.APIAllowedOrigins),
	)
	handler := chain(mux)

	return &Server{
		cfg: cfg,
		hub: hub,
		redis: redis,
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
