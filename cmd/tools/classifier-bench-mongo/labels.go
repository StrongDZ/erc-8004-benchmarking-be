package main

// labels.go — CSV I/O for the gold-label workflow.
//
//   exportOthersForLabelling: dumps a stratified others-sample to a CSV with
//                             an empty gold_category column ready for manual
//                             labelling (Sheets / Excel).
//
//   loadGoldLabels:           reads a labelled CSV back into memory, indexed
//                             by feedback_id, so --source=labelled can join
//                             in-place against fresh DB pulls.
//
// The CSV columns are kept in sync with promptRowHeader (prompt_row.go) so
// every prompt-input field surfaces in the labelling sheet — the reviewer
// labels against the same context the LLM sees.

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	feedbackrepo "erc-8004-benchmarking-be/internal/repository/feedback"
)

// labelTemplateHeader = promptRowHeader ++ [rule_category, gold_category, notes].
// Built lazily to avoid an init-order coupling on promptRowHeader.
func labelTemplateHeader() []string {
	out := make([]string, 0, len(promptRowHeader)+3)
	out = append(out, promptRowHeader...)
	out = append(out, "rule_category", "gold_category", "notes")
	return out
}

// exportOthersForLabelling writes a stratified others-sample to `path` with
// every prompt-input field plus an empty gold_category column for the human
// labeller. Re-runs of the bench in "labelled" mode read this same file back
// in via loadGoldLabels.
func exportOthersForLabelling(
	path string,
	samples []feedbackrepo.FeedbackRecord,
	agentCache map[string]agentMeta,
) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create label template: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(labelTemplateHeader()); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	for _, fb := range samples {
		am := agentCache[fb.ID]
		row := append(promptRow(fb, am),
			fb.Classification.Rule.Category, // rule_category (will be "others" for this export)
			"",                              // gold_category — human fills
			"",                              // notes
		)
		if err := w.Write(row); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	return nil
}

// GoldLabel is the human-labelled ground truth for one feedback record.
type GoldLabel struct {
	FeedbackID   string
	GoldCategory string
	Notes        string
}

// loadGoldLabels reads a labelled CSV (matching labelTemplateHeader layout)
// and returns rows where gold_category is non-empty. Rows with empty gold are
// skipped silently — they represent records the human hasn't gotten to yet.
func loadGoldLabels(path string) (map[string]GoldLabel, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open labels: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1 // tolerate trailing-comma drift from Sheets exports

	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse labels: %w", err)
	}
	if len(rows) < 2 {
		return map[string]GoldLabel{}, nil
	}

	header := rows[0]
	idIdx := indexOf(header, "feedback_id")
	goldIdx := indexOf(header, "gold_category")
	if goldIdx < 0 {
		goldIdx = indexOf(header, "category") // gold_final.csv uses category as the human label
	}
	notesIdx := indexOf(header, "notes")
	if idIdx < 0 || goldIdx < 0 {
		return nil, fmt.Errorf("labels missing required columns feedback_id, gold_category|category (got %v)", header)
	}

	out := make(map[string]GoldLabel, len(rows))
	for _, row := range rows[1:] {
		if len(row) <= goldIdx {
			continue
		}
		gold := strings.TrimSpace(row[goldIdx])
		if gold == "" {
			continue
		}
		notes := ""
		if notesIdx >= 0 && notesIdx < len(row) {
			notes = strings.TrimSpace(row[notesIdx])
		}
		out[strings.TrimSpace(row[idIdx])] = GoldLabel{
			FeedbackID:   strings.TrimSpace(row[idIdx]),
			GoldCategory: gold,
			Notes:        notes,
		}
	}
	return out, nil
}

func indexOf(header []string, name string) int {
	name = strings.ToLower(strings.TrimSpace(name))
	for i, h := range header {
		if strings.ToLower(strings.TrimSpace(h)) == name {
			return i
		}
	}
	return -1
}
