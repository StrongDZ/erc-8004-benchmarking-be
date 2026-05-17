package main

// prompt_row.go — Shared CSV row builder for prompt-input fields.
//
// The labelling export template (labels.go) and the main bench CSV (main.go)
// both write the prompt-input columns first. Keeping the columns and the
// builder in one place guarantees the two files stay in sync, so a label CSV
// produced by --source=others --export-only can be read back by --source=labelled
// without column drift.

import (
	"strconv"
	"strings"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// promptRowHeader is the canonical header for prompt-input columns. Every
// field listed here is shown to the LLM (possibly summarized first), so the
// reviewer can see exactly what the model sees.
var promptRowHeader = []string{
	"feedback_id", "chain_id", "agent_id",
	"tag1", "tag2",
	"value_raw", "value_decimals", "value_norm", "scale",
	"endpoint", "endpoint_host",
	"offchain_summary",
	"agent_description", "agent_services", "agent_tags",
}

// promptRow builds the prompt-input row matching promptRowHeader for one
// feedback record + cached agent metadata.
//
// `offchain_summary` is the schema-aware compact representation that goes
// into the LLM prompt, not the raw feedbackParsed JSON. `endpoint_host` is
// the bare host (no scheme/path) — same projection the prompt builder uses.
func promptRow(fb feedbackrepo.FeedbackRecord, am agentMeta) []string {
	valueNorm := classifier.NormalizeValue(fb.Value, int(fb.ValueDecimals))
	scale := ""
	if real, ok := classifier.RawValueToReal(fb.Value, int(fb.ValueDecimals)); ok {
		scale = classifier.AssignTier(real)
	}
	offRaw := offchainSummaryFromMap(fb.FeedbackParsed)
	offSummary := classifier.SummarizeJSONLikeSchema(offRaw, 240)

	return []string{
		fb.ID,
		strconv.FormatInt(fb.ChainID, 10),
		fb.AgentID,
		fb.Tag1, fb.Tag2,
		fb.Value, strconv.Itoa(int(fb.ValueDecimals)),
		strconv.FormatFloat(valueNorm, 'f', 4, 64),
		scale,
		fb.Endpoint, classifier.EndpointHost(fb.Endpoint),
		offSummary,
		truncateRunes(am.description, 240),
		truncateRunes(am.services, 240),
		strings.Join(am.tags, "|"),
	}
}
