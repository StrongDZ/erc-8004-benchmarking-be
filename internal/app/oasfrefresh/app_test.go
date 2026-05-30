package oasfrefresh

import (
	"sort"
	"testing"
)

func TestFlattenEntries_TopLevel(t *testing.T) {
	raw := map[string]rawNode{
		"nlp": {
			ID:      1,
			Name:    "natural_language_processing",
			Caption: "Natural Language Processing",
		},
	}
	entries := flattenEntries(raw)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Key != "natural_language_processing" {
		t.Errorf("Key = %q, want %q", e.Key, "natural_language_processing")
	}
	if e.ShortName != "natural_language_processing" {
		t.Errorf("ShortName = %q, want %q", e.ShortName, "natural_language_processing")
	}
	if e.Depth != 0 {
		t.Errorf("Depth = %d, want 0", e.Depth)
	}
	if e.ParentKey != "" {
		t.Errorf("ParentKey = %q, want empty", e.ParentKey)
	}
	if e.Category != "natural_language_processing" {
		t.Errorf("Category = %q, want %q", e.Category, "natural_language_processing")
	}
	if e.CategoryName != "Natural Language Processing" {
		t.Errorf("CategoryName = %q", e.CategoryName)
	}
}

func TestFlattenEntries_NestedChild(t *testing.T) {
	raw := map[string]rawNode{
		"nlp": {
			ID:      1,
			Name:    "natural_language_processing",
			Caption: "Natural Language Processing",
			Classes: map[string]rawNode{
				"nlu": {
					ID:      101,
					Name:    "natural_language_processing/natural_language_understanding",
					Caption: "Natural Language Understanding",
					Classes: map[string]rawNode{
						"ctx": {
							ID:      10101,
							Name:    "natural_language_processing/natural_language_understanding/contextual_comprehension",
							Caption: "Contextual Comprehension",
						},
					},
				},
			},
		},
	}
	entries := flattenEntries(raw)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })

	leaf := entries[2] // natural_language_processing/natural_language_understanding/contextual_comprehension
	if leaf.Depth != 2 {
		t.Errorf("leaf depth = %d, want 2", leaf.Depth)
	}
	if leaf.ParentKey != "natural_language_processing/natural_language_understanding" {
		t.Errorf("leaf parentKey = %q", leaf.ParentKey)
	}
	if leaf.ShortName != "contextual_comprehension" {
		t.Errorf("leaf shortName = %q", leaf.ShortName)
	}
	if leaf.Category != "natural_language_processing" {
		t.Errorf("leaf category = %q", leaf.Category)
	}
}

func TestFlattenEntries_EmptyName_FallsBackToMapKey(t *testing.T) {
	raw := map[string]rawNode{
		"my_skill": {
			ID:      99,
			Caption: "My Skill",
			// Name intentionally empty — should fall back to map key
		},
	}
	entries := flattenEntries(raw)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Key != "my_skill" {
		t.Errorf("Key = %q, want %q", entries[0].Key, "my_skill")
	}
}
