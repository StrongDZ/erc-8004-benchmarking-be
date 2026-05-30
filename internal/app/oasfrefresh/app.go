package oasfrefresh

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"erc-8004-benchmarking-be/internal/repository/oasfschema"
)

const oasfBase = "https://schema.oasf.outshift.com/api/1.0.0"

// App runs the periodic OASF schema refresh.
type App struct {
	skills   *oasfschema.Repository
	domains  *oasfschema.Repository
	cronExpr string
	client   *http.Client
}

// NewApp creates a new oasfrefresh App.
func NewApp(skills, domains *oasfschema.Repository, cronExpr string) *App {
	return &App{
		skills:   skills,
		domains:  domains,
		cronExpr: cronExpr,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

// Run seeds the collections if empty, then starts the weekly cron.
// Blocks until ctx is cancelled.
func (a *App) Run(ctx context.Context) error {
	a.seedIfEmpty(ctx)

	c := cron.New()
	if _, err := c.AddFunc(a.cronExpr, func() { a.refresh(ctx) }); err != nil {
		return fmt.Errorf("oasf-schema-refresh: add cron: %w", err)
	}
	c.Start()
	log.Printf("oasf-schema-refresh: cron started expr=%q", a.cronExpr)
	<-ctx.Done()
	<-c.Stop().Done()
	return ctx.Err()
}

func (a *App) seedIfEmpty(ctx context.Context) {
	n, err := a.skills.Count(ctx)
	if err != nil {
		log.Printf("oasf-schema-refresh: count skills: %v", err)
		return
	}
	if n == 0 {
		log.Printf("oasf-schema-refresh: collections empty — seeding on startup")
		a.refresh(ctx)
	}
}

func (a *App) refresh(ctx context.Context) {
	log.Printf("oasf-schema-refresh: refresh start")
	if err := a.fetchAndUpsert(ctx, "skill_categories", a.skills); err != nil {
		log.Printf("oasf-schema-refresh: skills: %v", err)
	}
	if err := a.fetchAndUpsert(ctx, "domain_categories", a.domains); err != nil {
		log.Printf("oasf-schema-refresh: domains: %v", err)
	}
	log.Printf("oasf-schema-refresh: refresh done")
}

func (a *App) fetchAndUpsert(ctx context.Context, endpoint string, repo *oasfschema.Repository) error {
	url := oasfBase + "/" + endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch %s: status %d", url, resp.StatusCode)
	}

	var raw map[string]rawNode
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return fmt.Errorf("decode %s: %w", url, err)
	}

	entries := flattenEntries(raw)
	if err := repo.UpsertAll(ctx, entries); err != nil {
		return fmt.Errorf("upsert %s: %w", endpoint, err)
	}
	log.Printf("oasf-schema-refresh: %s → %d entries upserted", endpoint, len(entries))
	return nil
}

// rawNode mirrors one node in the OASF API response tree.
type rawNode struct {
	ID          int                `json:"id"`
	Name        string             `json:"name"`
	Caption     string             `json:"caption"`
	Description string             `json:"description"`
	Classes     map[string]rawNode `json:"classes"`
}

// flattenEntries recursively flattens the OASF category tree into a slice of Entry.
// This is the Go port of the frontend's parseEntries function in oasf-schema.ts.
//
// Key is the full hierarchical path from node.Name (fallback: map key).
// ShortName is the last segment of Key (leaf label for display).
// ParentKey is the full hierarchical path name of the parent node (from node.Name).
func flattenEntries(raw map[string]rawNode) []oasfschema.Entry {
	var out []oasfschema.Entry
	// visit walks the tree; fullName is the parent node's full Name path used as ParentKey.
	var visit func(tree map[string]rawNode, depth int, categoryKey, categoryName, fullName string)
	visit = func(tree map[string]rawNode, depth int, categoryKey, categoryName, fullName string) {
		for mapKey, node := range tree {
			// key is the full hierarchical path from the JSON Name field.
			key := node.Name
			if key == "" {
				key = mapKey
			}
			catKey := categoryKey
			catName := categoryName
			if depth == 0 {
				catKey = key
				catName = node.Caption
			}
			segments := strings.Split(key, "/")
			sn := segments[len(segments)-1]

			e := oasfschema.Entry{
				Key:          key,
				ShortName:    sn,
				Caption:      node.Caption,
				Description:  node.Description,
				UID:          node.ID,
				Category:     catKey,
				CategoryName: catName,
				Depth:        depth,
				ParentKey:    fullName,
			}
			out = append(out, e)

			if len(node.Classes) > 0 {
				visit(node.Classes, depth+1, catKey, catName, key)
			}
		}
	}
	visit(raw, 0, "", "", "")
	return out
}
