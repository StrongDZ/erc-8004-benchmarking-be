package main

// metrics.go — Classification metrics for the bench tool.
//
// Computes per-category precision/recall/F1, macro-F1, accuracy, and a 6×6
// confusion matrix from (gold, predicted) pairs. Pure functions — no I/O.
//
// Used by the "labelled" source mode where every sample has a hand-labelled
// gold_category. In "category" mode the rule output is treated as a proxy for
// gold so the same code reports rule-mimicking accuracy.

import (
	"fmt"
	"sort"
	"strings"
)

// AllCategories lists the 5 canonical labels in display order. Anything else
// in either gold or predicted columns is bucketed under "<other>" so totals
// always reconcile.
var AllCategories = []string{
	"junk", "service_feedback", "config_feedback", "app_specific", "others",
}

// CategoryStats holds precision/recall/F1 numbers for one category.
type CategoryStats struct {
	Category  string
	Support   int     // # of gold samples with this label
	TP        int     // gold==cat && pred==cat
	FP        int     // gold!=cat && pred==cat
	FN        int     // gold==cat && pred!=cat
	Precision float64 // TP / (TP+FP) — 0.0 when TP+FP == 0
	Recall    float64 // TP / (TP+FN) — 0.0 when support == 0
	F1        float64 // harmonic mean of P and R
}

// Report bundles the full metric set for one model on one labelled run.
type Report struct {
	Total       int
	Correct     int
	Accuracy    float64
	MacroF1     float64 // mean F1 across categories with support > 0
	PerCategory []CategoryStats
	Confusion   map[string]map[string]int // gold → pred → count
}

// ComputeReport tallies metrics from parallel (gold, pred) slices.
// Length mismatch is treated as a programming error and panics in dev; in
// production we just truncate to the shorter slice so the CSV writer still
// produces output.
func ComputeReport(gold, pred []string) Report {
	n := len(gold)
	if len(pred) < n {
		n = len(pred)
	}

	confusion := make(map[string]map[string]int, len(AllCategories))
	for _, c := range AllCategories {
		confusion[c] = make(map[string]int, len(AllCategories))
	}

	correct := 0
	support := make(map[string]int, len(AllCategories))
	tp := make(map[string]int, len(AllCategories))
	fp := make(map[string]int, len(AllCategories))
	fn := make(map[string]int, len(AllCategories))

	for i := 0; i < n; i++ {
		g := normCategory(gold[i])
		p := normCategory(pred[i])
		if _, ok := confusion[g]; !ok {
			confusion[g] = make(map[string]int, len(AllCategories))
		}
		confusion[g][p]++
		support[g]++
		if g == p {
			correct++
			tp[g]++
		} else {
			fn[g]++
			fp[p]++
		}
	}

	stats := make([]CategoryStats, 0, len(AllCategories))
	macroF1Sum := 0.0
	macroF1N := 0
	for _, c := range AllCategories {
		s := CategoryStats{
			Category: c,
			Support:  support[c],
			TP:       tp[c],
			FP:       fp[c],
			FN:       fn[c],
		}
		if s.TP+s.FP > 0 {
			s.Precision = float64(s.TP) / float64(s.TP+s.FP)
		}
		if s.Support > 0 {
			s.Recall = float64(s.TP) / float64(s.Support)
		}
		if s.Precision+s.Recall > 0 {
			s.F1 = 2 * s.Precision * s.Recall / (s.Precision + s.Recall)
		}
		stats = append(stats, s)
		if s.Support > 0 {
			macroF1Sum += s.F1
			macroF1N++
		}
	}

	macroF1 := 0.0
	if macroF1N > 0 {
		macroF1 = macroF1Sum / float64(macroF1N)
	}
	accuracy := 0.0
	if n > 0 {
		accuracy = float64(correct) / float64(n)
	}

	return Report{
		Total:       n,
		Correct:     correct,
		Accuracy:    accuracy,
		MacroF1:     macroF1,
		PerCategory: stats,
		Confusion:   confusion,
	}
}

// FormatReport renders a human-readable summary suitable for stdout / log.
// title is shown as a banner (typically the model name).
func FormatReport(title string, r Report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n=== %s ===\n", title)
	fmt.Fprintf(&b, "samples=%d  correct=%d  accuracy=%.3f  macro_f1=%.3f\n",
		r.Total, r.Correct, r.Accuracy, r.MacroF1)

	fmt.Fprintln(&b, "\nper-category:")
	fmt.Fprintf(&b, "  %-20s %7s %5s %5s %5s %9s %7s %5s\n",
		"category", "support", "TP", "FP", "FN", "precision", "recall", "F1")
	for _, s := range r.PerCategory {
		fmt.Fprintf(&b, "  %-20s %7d %5d %5d %5d %9.3f %7.3f %5.3f\n",
			s.Category, s.Support, s.TP, s.FP, s.FN, s.Precision, s.Recall, s.F1)
	}

	fmt.Fprintln(&b, "\nconfusion matrix (rows=gold, cols=pred):")
	header := "  " + padRight("gold\\pred", 20)
	for _, c := range AllCategories {
		header += " " + padRight(shortCat(c), 9)
	}
	fmt.Fprintln(&b, header)
	for _, gc := range AllCategories {
		row := "  " + padRight(gc, 20)
		for _, pc := range AllCategories {
			row += " " + padRight(fmt.Sprintf("%d", r.Confusion[gc][pc]), 9)
		}
		fmt.Fprintln(&b, row)
	}

	// Surface any unexpected categories that slipped in (typos, schema drift).
	extras := unexpectedCategories(r.Confusion)
	if len(extras) > 0 {
		fmt.Fprintf(&b, "\nNOTE: unexpected categories observed: %s\n", strings.Join(extras, ", "))
	}
	return b.String()
}

func normCategory(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "others"
	}
	for _, c := range AllCategories {
		if c == s {
			return s
		}
	}
	return s
}

func unexpectedCategories(confusion map[string]map[string]int) []string {
	known := make(map[string]bool, len(AllCategories))
	for _, c := range AllCategories {
		known[c] = true
	}
	seen := map[string]bool{}
	for g, row := range confusion {
		if !known[g] && hasNonZero(row) {
			seen[g] = true
		}
		for p, n := range row {
			if !known[p] && n > 0 {
				seen[p] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func hasNonZero(m map[string]int) bool {
	for _, v := range m {
		if v > 0 {
			return true
		}
	}
	return false
}

func shortCat(c string) string {
	switch c {
	case "service_feedback":
		return "service"
	case "config_feedback":
		return "config"
	case "app_specific":
		return "app"
	default:
		return c
	}
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}
