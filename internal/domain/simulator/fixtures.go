package simulator

// fixtures.go — Synthetic feedback templates used by the sandbox simulator.
// All entries produce deterministic, classifiable tag pairs so the scoring
// pipeline exercises real code paths (rule classifier → quality/quantity score).

type fixture struct {
	tag1  string
	tag2  string
	scale string // "star5" | "pct100" | "binary"
	value string // pre-scaled integer as string (valueDecimals == 0)
}

// fixturePool is rotated round-robin across synthetic feedback records.
var fixturePool = []fixture{
	{tag1: "response_time", tag2: "latency", scale: "pct100", value: "82"},
	{tag1: "task_success", tag2: "accuracy", scale: "star5", value: "4"},
	{tag1: "reliability", tag2: "uptime", scale: "pct100", value: "91"},
	{tag1: "helpfulness", tag2: "", scale: "star5", value: "5"},
	{tag1: "tool_use", tag2: "efficiency", scale: "pct100", value: "74"},
}
