package main

// prompt_row.go — CSV row dump for one feedback record + its agent context.
//
// After the LLM prompt logic moved to the Python AI service, this file is no
// longer a "prompt row" in the strict sense — it's the column layout the bench
// uses to surface the same input fields the AI service sees (so labelling
// reviewers and the model both work from the same context).

import (
	"encoding/json"
	"strconv"

	"erc-8004-benchmarking-be/internal/domain/classifier"
	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// promptRowHeader lists the columns dumped by promptRow, in CSV order.
var promptRowHeader = []string{
	"feedback_id",
	"chain_id",
	"agent_id",
	"tag1",
	"tag2",
	"value",
	"value_decimals",
	"value_norm",
	"scale",
	"endpoint",
	"offchain_json",
	"agent_description",
	"agent_services",
	"agent_tags",
}

// promptRow renders one feedback record as a string slice matching promptRowHeader.
func promptRow(fb feedbackrepo.FeedbackRecord, am agentMeta) []string {
	valueNorm := classifier.NormalizeValue(fb.Value, int(fb.ValueDecimals))
	scale := ""
	if real, ok := classifier.RawValueToReal(fb.Value, int(fb.ValueDecimals)); ok {
		scale = classifier.AssignTier(real)
	}
	off := ""
	if len(fb.FeedbackParsed) > 0 {
		if b, err := json.Marshal(fb.FeedbackParsed); err == nil {
			off = string(b)
			if len(off) > 2000 {
				off = off[:2000]
			}
		}
	}
	tagsCSV := ""
	for i, t := range am.tags {
		if i > 0 {
			tagsCSV += ","
		}
		tagsCSV += t
	}
	return []string{
		fb.ID,
		strconv.FormatInt(fb.ChainID, 10),
		fb.AgentID,
		fb.Tag1,
		fb.Tag2,
		fb.Value,
		strconv.FormatInt(int64(fb.ValueDecimals), 10),
		strconv.FormatFloat(valueNorm, 'f', 4, 64),
		scale,
		fb.Endpoint,
		off,
		am.description,
		am.services,
		tagsCSV,
	}
}
