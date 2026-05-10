package handlers

// leaderboard.go — HTTP handlers for /leaderboard, /leaderboard/search,
// /leaderboard/stats, /leaderboard/rising-stars.

import (
	"errors"
	"net/http"
	"strings"

	"erc-8004-benchmarking-be/internal/api/dto"
	"erc-8004-benchmarking-be/internal/api/service"
	agentrepo "erc-8004-benchmarking-be/internal/repository/agent"
)

// LeaderboardHandler exposes leaderboard endpoints.
type LeaderboardHandler struct {
	svc *service.Leaderboard
}

// NewLeaderboardHandler wires the leaderboard service to HTTP.
func NewLeaderboardHandler(svc *service.Leaderboard) *LeaderboardHandler {
	return &LeaderboardHandler{svc: svc}
}

// List handles GET /leaderboard (§2.1).
// @Summary List leaderboard
// @Description Returns paginated leaderboard rows with filters.
// @Tags leaderboard
// @Produce json
// @Param chainId query int true "Chain ID"
// @Param page query int false "Page number"
// @Param limit query int false "Page size (max 100)"
// @Param sort query string false "score_desc|score_asc|tasks_desc|recent"
// @Param query query string false "Substring match on name, description, agentId, oasfSkills, oasfDomains"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /leaderboard [get]
func (h *LeaderboardHandler) List(w http.ResponseWriter, r *http.Request) {
	// chainId accepts single value or CSV/multi. Missing chainId means "all chains".
	chainIDs := dto.ParseInt64Slice(r, "chainId")
	page, limit, skip := dto.ParsePagination(r, 50, 100)
	sort := parseLeaderboardSort(r.URL.Query().Get("sort"))

	skills := dto.ParseStringSlice(r, "skill")
	if len(skills) == 0 {
		skills = dto.ParseStringSlice(r, "skills")
	}
	domains := dto.ParseStringSlice(r, "domain")
	if len(domains) == 0 {
		domains = dto.ParseStringSlice(r, "domains")
	}

	params := service.ListParams{
		ChainIDs: chainIDs,
		Skills:   skills,
		Domains:  domains,
		Services: dto.ParseStringSlice(r, "service"),
		Tags:     dto.ParseStringSlice(r, "tag"),
		X402:     dto.ParseBoolPtr(r, "x402"),
		HasOASF:  dto.ParseBoolPtr(r, "hasOASF"),
		Active:   dto.ParseBoolPtr(r, "active"),
		MinScore: dto.ParseFloat(r, "minScore", 0),
		MinTasks: dto.ParseInt64(r, "minTasks", 0),
		Query:    strings.TrimSpace(r.URL.Query().Get("query")),
		Owner:    strings.TrimSpace(r.URL.Query().Get("owner")),
		Sort:     sort,
		Page:     page,
		Limit:    limit,
		Skip:     skip,
	}
	if len(chainIDs) == 1 {
		params.ChainID = chainIDs[0]
	}

	result, err := h.svc.List(r.Context(), params)
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.SuccessMeta(w, r, result.Rows, &dto.Meta{Page: result.Page, Limit: result.Limit, Total: result.Total})
}

// Search handles GET /leaderboard/search (§2.2).
// @Summary Search leaderboard
// @Description Autocomplete search by agent name prefix.
// @Tags leaderboard
// @Produce json
// @Param chainId query int true "Chain ID"
// @Param q query string true "Search query, min length 2"
// @Param limit query int false "Max results (1..20)"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /leaderboard/search [get]
func (h *LeaderboardHandler) Search(w http.ResponseWriter, r *http.Request) {
	chainID := dto.ParseInt64(r, "chainId", 0)
	if chainID <= 0 {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, "chainId is required")
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if len(q) < 2 {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, "q must be at least 2 characters")
		return
	}
	limit := dto.ParseInt(r, "limit", 10)
	if limit < 1 {
		limit = 1
	}
	if limit > 20 {
		limit = 20
	}
	rows, err := h.svc.Search(r.Context(), chainID, q, limit)
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, rows)
}

// Stats handles GET /leaderboard/stats (§2.4).
// @Summary Leaderboard stats
// @Description Returns KPI stats for one chain.
// @Tags leaderboard
// @Produce json
// @Param chainId query int true "Chain ID"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /leaderboard/stats [get]
func (h *LeaderboardHandler) Stats(w http.ResponseWriter, r *http.Request) {
	// chainId accepts CSV/multi. Missing = all chains.
	chainIDs := dto.ParseInt64Slice(r, "chainId")
	out, err := h.svc.StatsMulti(r.Context(), chainIDs)
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, out)
}

// Tags handles GET /leaderboard/tags — returns the top N agent tags optionally
// filtered by chain scope and a query prefix (for autocomplete).
// @Summary Top agent tags
// @Description Returns the most common tags across agents. Supports multi-chain filter and search.
// @Tags leaderboard
// @Produce json
// @Param chainId query string false "Chain ID(s), comma-separated"
// @Param q query string false "Case-insensitive prefix/substring filter"
// @Param limit query int false "Max results (1..200)"
// @Success 200 {object} dto.Response
// @Router /leaderboard/tags [get]
func (h *LeaderboardHandler) Tags(w http.ResponseWriter, r *http.Request) {
	chainIDs := dto.ParseInt64Slice(r, "chainId")
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	limit := dto.ParseInt(r, "limit", 50)
	if limit < 1 {
		limit = 1
	}
	if limit > 200 {
		limit = 200
	}
	rows, err := h.svc.TopTags(r.Context(), chainIDs, q, limit)
	if err != nil {
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, rows)
}

// RisingStars handles GET /leaderboard/rising-stars (§2.3).
// @Summary Rising stars
// @Description Returns top agents by score increase in a period.
// @Tags leaderboard
// @Produce json
// @Param chainId query int true "Chain ID"
// @Param period query string false "1d|7d|30d"
// @Param limit query int false "Max results (1..50)"
// @Success 200 {object} dto.Response
// @Failure 400 {object} dto.Response
// @Failure 500 {object} dto.Response
// @Router /leaderboard/rising-stars [get]
func (h *LeaderboardHandler) RisingStars(w http.ResponseWriter, r *http.Request) {
	chainIDs := dto.ParseInt64Slice(r, "chainId")
	if len(chainIDs) == 0 {
		dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, "chainId is required (single value or CSV)")
		return
	}
	period := strings.TrimSpace(r.URL.Query().Get("period"))
	if period == "" {
		period = "7d"
	}
	limit := dto.ParseInt(r, "limit", 10)
	if limit < 1 {
		limit = 1
	}
	if limit > 50 {
		limit = 50
	}
	rows, err := h.svc.RisingStarsMulti(r.Context(), chainIDs, period, limit)
	if err != nil {
		if strings.Contains(err.Error(), "invalid period") || errors.Is(err, errInvalidPeriod) {
			dto.Fail(w, r, http.StatusBadRequest, dto.CodeBadRequest, err.Error())
			return
		}
		dto.Fail(w, r, http.StatusInternalServerError, dto.CodeInternalError, err.Error())
		return
	}
	dto.Success(w, r, rows)
}

// errInvalidPeriod is a placeholder sentinel used by the service layer when
// an unsupported period is passed. The service currently returns a formatted
// error; this var exists so future refactors can switch to errors.Is.
var errInvalidPeriod = errors.New("invalid period")

// parseLeaderboardSort maps the public sort keyword to an agent-repo sort enum.
func parseLeaderboardSort(v string) agentrepo.LeaderboardSort {
	switch strings.TrimSpace(v) {
	case string(agentrepo.SortScoreAsc):
		return agentrepo.SortScoreAsc
	case string(agentrepo.SortTasksDesc):
		return agentrepo.SortTasksDesc
	case string(agentrepo.SortRecent):
		return agentrepo.SortRecent
	case "", string(agentrepo.SortScoreDesc):
		return agentrepo.SortScoreDesc
	default:
		return agentrepo.SortScoreDesc
	}
}
