package descsummarizer

// app.go — Consumer worker for the agent description-summary queue.
//
// Each delivery carries an mq.AgentDescSummaryMessage produced by the trustrank
// identity processor whenever an agent's description becomes non-empty or
// changes. The worker:
//
//  1. Skips if agents.summarizedDescriptionHash already matches msg.DescHash
//     (idempotency — protects against duplicate deliveries / replays).
//  2. POSTs the description to the AI service /summarize endpoint.
//  3. Upserts agents.summarizedDescription{,Hash,At}.
//
// Retry policy:
//   - Mongo / transient AI service error → nack + requeue (up to maxAttempts).
//   - Past maxAttempts → ack-drop with log (poison-pill protection).
//   - Agent doc missing → ack-drop; the next identity event will re-trigger.
//
// Attempt tracking uses msg.Attempt (incremented on each republish).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rmqinfra "erc-8004-benchmarking-be/internal/infra/rabbitmq"
	"erc-8004-benchmarking-be/internal/mq"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
)

const (
	defaultMaxAttempts         = 3
	defaultAIRequestTimeoutS   = 30
	defaultConsumerConcurrency = 4
)

// Config controls runtime behaviour of the desc-summarizer worker.
type Config struct {
	AIServiceURL       string // e.g. http://localhost:8000
	QueueName          string // defaults to mq.QueueAgentDescSummary
	Prefetch           int    // QoS prefetch; defaults to 4
	RequestTimeoutSecs int    // HTTP timeout for /summarize; defaults to 30
	MaxAttempts        int    // requeue ceiling; defaults to 3
	Model              string // optional model override for /summarize
}

// App is the consumer worker. It implements rabbitmq.MessageHandler[mq.AgentDescSummaryMessage].
type App struct {
	rmqinfra.BaseHandler[mq.AgentDescSummaryMessage]
	pool   *rmqinfra.ConsumerPool[mq.AgentDescSummaryMessage]
	cfg    Config
	conn   *amqp.Connection
	agents *agentrepo.Repository
	httpCl *http.Client
}

// NewApp builds a consumer App.
func NewApp(conn *amqp.Connection, agents *agentrepo.Repository, cfg Config) *App {
	if cfg.QueueName == "" {
		cfg.QueueName = mq.QueueAgentDescSummary
	}
	if cfg.Prefetch < 1 {
		cfg.Prefetch = defaultConsumerConcurrency
	}
	if cfg.RequestTimeoutSecs <= 0 {
		cfg.RequestTimeoutSecs = defaultAIRequestTimeoutS
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = defaultMaxAttempts
	}
	a := &App{
		cfg:    cfg,
		conn:   conn,
		agents: agents,
		httpCl: &http.Client{Timeout: time.Duration(cfg.RequestTimeoutSecs+2) * time.Second},
	}
	a.pool = rmqinfra.New[mq.AgentDescSummaryMessage](conn, a)
	return a
}

func (a *App) Init(_ string) rmqinfra.ReaderConfig {
	return rmqinfra.ReaderConfig{Prefetch: a.cfg.Prefetch, Workers: a.cfg.Prefetch}
}

func (a *App) QueueName(_ string) string { return a.cfg.QueueName }

// Handle processes one desc-summary message.
// Returns nil to ack (success, drop, or republish-for-retry).
// Returns non-nil only for infrastructure failures (mongo) so the pool nacks+requeues.
func (a *App) Handle(ctx context.Context, msg mq.AgentDescSummaryMessage) error {
	doc, err := a.agents.FindByAgentID(ctx, msg.ChainID, msg.AgentID)
	if err == nil && doc != nil && doc.SummarizedDescriptionHash == msg.DescHash && msg.DescHash != "" {
		return nil // already done, ack
	}

	summary, sumErr := a.fetchSummary(ctx, msg)
	if sumErr != nil {
		nextAttempt := msg.Attempt + 1
		if nextAttempt >= a.cfg.MaxAttempts {
			log.Printf("desc_summarizer: drop after %d attempts chain=%d agent=%s: %v",
				nextAttempt, msg.ChainID, msg.AgentID, sumErr)
			return nil // ack+drop
		}
		newMsg := msg
		newMsg.Attempt = nextAttempt
		if pubErr := a.republish(ctx, newMsg); pubErr != nil {
			log.Printf("desc_summarizer: republish failed chain=%d agent=%s: %v (original: %v)",
				msg.ChainID, msg.AgentID, pubErr, sumErr)
			return pubErr // OnError (from BaseHandler) → nack+requeue
		}
		return nil // ack original after successful republish
	}

	matched, err := a.agents.SetSummarizedDescription(ctx, msg.ChainID, msg.AgentID, summary, msg.DescHash, time.Now().Unix())
	if err != nil {
		log.Printf("desc_summarizer: mongo write error chain=%d agent=%s: %v", msg.ChainID, msg.AgentID, err)
		return err // OnError → nack+requeue
	}
	if !matched {
		log.Printf("desc_summarizer: agent missing, dropping chain=%d agent=%s", msg.ChainID, msg.AgentID)
	}
	return nil
}

// Run opens the AMQP consumer and blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	log.Printf("desc_summarizer: started queue=%s prefetch=%d ai=%s",
		a.cfg.QueueName, a.cfg.Prefetch, a.cfg.AIServiceURL)
	a.pool.EnsureReader(ctx, "")
	<-ctx.Done()
	return ctx.Err()
}

// republish publishes msg (with incremented Attempt) to the same queue for retry.
func (a *App) republish(ctx context.Context, msg mq.AgentDescSummaryMessage) error {
	ch, err := a.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer ch.Close()

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return ch.PublishWithContext(ctx, "", a.cfg.QueueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
}

type summarizeReq struct {
	AgentID     string `json:"agent_id"`
	Description string `json:"description"`
	Model       string `json:"model,omitempty"`
}

type summarizeResp struct {
	Summary  string `json:"summary"`
	ModelVer string `json:"model_ver"`
}

func (a *App) fetchSummary(ctx context.Context, msg mq.AgentDescSummaryMessage) (string, error) {
	if strings.TrimSpace(a.cfg.AIServiceURL) == "" {
		return "", fmt.Errorf("AI service URL not configured")
	}
	payload := summarizeReq{
		AgentID:     fmt.Sprintf("%d:%s", msg.ChainID, msg.AgentID),
		Description: msg.Description,
		Model:       a.cfg.Model,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(a.cfg.RequestTimeoutSecs)*time.Second)
	defer cancel()

	url := strings.TrimRight(a.cfg.AIServiceURL, "/") + "/summarize"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpCl.Do(req)
	if err != nil {
		return "", fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var out summarizeResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	summary := strings.TrimSpace(out.Summary)
	if summary == "" {
		return "", fmt.Errorf("empty summary from ai service")
	}
	return summary, nil
}
