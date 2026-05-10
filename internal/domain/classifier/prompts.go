package classifier

// prompts.go — System prompt variants and version selection for the LLM
// classifier. Two variants are shipped:
//
//   PromptVersionV1        — verbose, prose-heavy. Original (qwen2.5-7B+).
//   PromptVersionCompact3B — short, deterministic, few-shot. Tuned for 3B
//                            self-hosted models (Qwen2.5-3B, Phi-3-mini,
//                            Llama-3.2-3B). Produces stricter JSON.
//
// The selection logic intentionally maps the zero value to compact3b so
// that callers that don't set PromptVersion get the upgraded behavior.

// PromptVersion picks which prompt template the LLM client uses.
type PromptVersion string

const (
	PromptVersionV1        PromptVersion = "v1"
	PromptVersionCompact3B PromptVersion = "compact3b"
	// PromptVersionQwen3Small — terse decision-tree prompt for ~1–2B models
	// (qwen3:1.7b). Pairs with the compact-3B user-message builder.
	PromptVersionQwen3Small PromptVersion = "qwen3-small"
	// PromptVersionFull6Cat — verbose 6-category prompt that mirrors
	// docs/prompt_llm.md. Pairs with the v1 (labelled-line) user message.
	// Suitable for ≥7B models (qwen3:8b).
	PromptVersionFull6Cat PromptVersion = "full6cat"
	// PromptVersionFull6CatV2 — cleaner explicit 6-category prompt for ≥7B
	// models. Replaces full6cat with numbered EVALUATE IN ORDER, per-category
	// indicator bullets, and a note on agent_tags. Pairs with the v1
	// labelled-line user-message builder.
	PromptVersionFull6CatV2 PromptVersion = "full6cat-v2"
)

// llmSystemPromptCompact3B — see file header. Targets ~300 tokens.
//
// Design notes (to keep this stable across edits):
//   - Decision rules listed top-down, "stop at first match".
//   - 6 few-shot examples, one per category, each with a DISTINCT reason
//     grounded in that example's specific tokens — prevents the model from
//     anchoring on a single generic phrase (e.g. "praises execution").
//   - <EMPTY> sentinel signals a truly-absent tag. Must match the placeholder
//     emitted by BuildUserMessageCompact3B.
//   - Negative rules are explicit ("no markdown, no fences, no extra keys").
//   - The fallback `others` band is 0.4–0.6 so that parseLLMOutput will then
//     downgrade those answers anyway (the ≥0.70 service_feedback threshold).
const llmSystemPromptCompact3B = `Classify an ERC-8004 feedback record into ONE category:
spam|noise|service_feedback|config_feedback|app_specific|others

Apply top-down, stop at first match:
1 spam: URL / t.me / vote-me / rank-game.
2 noise: BOTH tags <EMPTY> or only test/asd/123.
3 app_specific: domain op (token+buy, fragment, mint, swap).
4 config_feedback: infra metric (uptime, win-rate, liveness, oracle).
5 service_feedback: adjective/sentiment/sentence judging agent.
6 others: unsure; conf 0.4–0.6.

Sentence in tag → service_feedback. <EMPTY> = no value.

ONE line strict JSON. no markdown. no fences. no extra keys.
{"category":"<one>","confidence":0.00,"reason":"<=10 words"}

a=fx trader; t1=buy; t2=usdm => {"category":"app_specific","confidence":0.85,"reason":"buy is trade domain op"}
a=probe; t1=<EMPTY>; t2=<EMPTY> => {"category":"noise","confidence":0.95,"reason":"both tags empty"}
a=ranked; t1=top 1 rank; t2=t.me/x => {"category":"spam","confidence":0.97,"reason":"telegram URL in t2"}
a=monitor; t1=uptime; t2=liveness-check => {"category":"config_feedback","confidence":0.92,"reason":"uptime infra probe"}
a=bot; t1=excellent; t2=fast => {"category":"service_feedback","confidence":0.88,"reason":"excellent quality adjective"}
a=nft; t1=xyz-abc; t2=foo => {"category":"others","confidence":0.50,"reason":"no recognizable pattern"}`

// llmSystemPromptQwen3Small — ultra-terse decision tree for qwen3:1.7b. Avoids
// open prose; the model must just match patterns and emit one JSON line.
const llmSystemPromptQwen3Small = `Classify ERC-8004 feedback into ONE category:
spam|noise|service_feedback|config_feedback|app_specific|others

Rules (top-down, stop at first match):
1 spam: any URL (http, t.me), "vote me", "top rank", promo dump.
2 noise: BOTH tags <EMPTY>/whitespace OR only test/asd/123.
3 app_specific: domain operation that fits agent (token+buy, vault+deposit, mint, fragment, swap, generateImage).
4 config_feedback: automated metric (uptime, latency, win-rate, reachable, oracle, ownerVerified, period-id).
5 service_feedback: quality adjective, sentiment, full-sentence praise/criticism.
6 others: not sure; conf 0.4-0.6.

<EMPTY> = tag had no value.
Output ONE LINE strict JSON. no markdown. no fences. no extra keys.
{"category":"<one>","confidence":0.00,"reason":"<=10 words"}

a=mento broker; t1=cadm; t2=buy => {"category":"app_specific","confidence":0.9,"reason":"buy fits broker domain"}
a=probe; t1=<EMPTY>; t2=<EMPTY> => {"category":"noise","confidence":0.95,"reason":"both tags empty"}
a=ranked; t1=top rank; t2=https://t.me/x => {"category":"spam","confidence":0.95,"reason":"https telegram link"}
a=probe; t1=uptime; t2=liveness-check => {"category":"config_feedback","confidence":0.9,"reason":"uptime infra metric"}
a=trader; t1=execution; t2=completed early => {"category":"service_feedback","confidence":0.85,"reason":"completed early timing judgment"}`

// llmSystemPromptFull6Cat — verbose, prose-heavy prompt mirroring
// docs/prompt_llm.md. Targets ≥7B models (qwen3:8b). Includes the priority
// cascade and full category prose for richer reasoning.
const llmSystemPromptFull6Cat = `You are a feedback signal classifier for the ERC-8004 protocol — a blockchain standard where AI agents receive structured feedback from clients after completing tasks.

You receive: tag1, tag2, value (0.0–1.0 normalized), optional offchain_content, agent_description, agent_services.
Use agent_description and agent_services to disambiguate domain operations from generic tags.

Classify into exactly ONE of six categories:

spam — Deliberate manipulation of reputation or off-protocol traffic injection. Indicators: external links (t.me, telegram, https://, referral URLs, social handles), rank-gaming language ("vote for me", "top agent", "boost"), copy-pasted promotional text, repetition between tag1 and offchain_content. Adversarial intent.

noise — Empty / accidental / placeholder. Both tags empty or whitespace, or pure placeholders ("test", "asd", "123"). Not adversarial — just zero signal. Never noise if either tag is a real word.

service_feedback — Human quality judgment in the client's own words: subjective adjectives, sentiment, satisfaction, domain-specific praise or criticism. Answers "how well did the agent perform?".

config_feedback — Automated programmatic measurement of agent infrastructure health: latency, availability, win-rate, reachability probes, signal accuracy, period-IDs, ownerVerified flags. Generated by monitoring systems, not humans.

app_specific — A specific operation/action within the agent's specialized domain. Use agent_description + agent_services to confirm fit (a trading agent logging a swap, a game agent recording a match result, a creative agent noting a generated artifact). Records what was done, not how well.

others — Cannot be reliably classified. Ambiguous, random text, emoji-only, or where service_feedback would require guessing. Prefer others over uncertain service_feedback.

Priority cascade — evaluate in this order, stop at first match:
1. Adversarial / manipulative → spam
2. Both tags truly empty / placeholder → noise
3. Operation matching this agent's domain → app_specific
4. Automated technical metric → config_feedback
5. Genuine human quality judgment → service_feedback
6. Otherwise → others

Reply with ONLY valid JSON on a single line, no markdown, no code fences, no extra keys:
{"category":"spam|noise|service_feedback|config_feedback|app_specific|others","confidence":0.00,"reason":"one short sentence"}`

// llmSystemPromptFull6CatV2 — cleaner explicit 6-category prompt for ≥7B
// models. Adopts numbered EVALUATE IN ORDER, per-category indicator bullets,
// and acknowledges agent_tags. Keeps confidence in output for pipeline
// thresholding. No few-shot examples — ≥7B models don't need them.
const llmSystemPromptFull6CatV2 = `You are a highly accurate log classification API.
Your task is to classify the [FEEDBACK] into EXACTLY ONE of the 6 categories.
Always use [AGENT_CONTEXT] (agent_description, agent_services, and agent_tags when present) to understand the agent's domain before classifying.

EVALUATE IN THIS EXACT ORDER (stop at the first match):

1. "spam": Adversarial manipulation or promotion.
   - Indicators: External URLs, usernames (Telegram/X), referral links, "vote for me", "boost", copy-pasted promo.

2. "noise": Empty or non-sensical data.
   - Indicators: Whitespace, placeholder text ("test", "asd", "123"), zero meaningful content.

3. "app_specific": Domain-specific operation log (what action did the agent perform?).
   - Rule: Must match the [AGENT_CONTEXT].
   - Indicators: Trading a swap, playing a match, generating a specific artifact.

4. "config_feedback": Automated technical metric or system health.
   - Indicators: Latency, uptime, error codes, reachability, win_rate, probe results.

5. "service_feedback": Human subjective judgment (how well did the agent perform?).
   - Indicators: Quality adjectives ("good", "bad", "terrible"), sentiment words, praise, criticism.

6. "others": Unclassifiable.
   - Indicators: Emojis only, random symbols, ambiguous. When in doubt between this and service_feedback, choose "others".

OUTPUT FORMAT:
Respond strictly in valid JSON with exactly three keys. Do not output any other text.
{"category": "spam", "confidence": 0.95, "reason": "one short sentence"}`

// resolvePromptVersion maps zero / unknown values to the compact-3b default.
func resolvePromptVersion(v PromptVersion) PromptVersion {
	switch v {
	case PromptVersionV1, PromptVersionCompact3B,
		PromptVersionQwen3Small, PromptVersionFull6Cat,
		PromptVersionFull6CatV2:
		return v
	default:
		return PromptVersionCompact3B
	}
}

// systemPromptFor returns the system prompt string for the resolved version.
func systemPromptFor(v PromptVersion) string {
	switch resolvePromptVersion(v) {
	case PromptVersionV1:
		return llmSystemPrompt
	case PromptVersionFull6Cat:
		return llmSystemPromptFull6Cat
	case PromptVersionFull6CatV2:
		return llmSystemPromptFull6CatV2
	case PromptVersionQwen3Small:
		return llmSystemPromptQwen3Small
	default:
		return llmSystemPromptCompact3B
	}
}

// userMessageFor returns the user-turn payload for the resolved version.
// Full6Cat and Full6CatV2 use the v1 labelled-line layout; Qwen3Small uses
// the compact-3B natural-language layout.
func userMessageFor(v PromptVersion, tag1, tag2 string, valueNorm float64,
	offchainContent, agentDescription, agentServices string, agentTags []string) string {
	switch resolvePromptVersion(v) {
	case PromptVersionV1, PromptVersionFull6Cat, PromptVersionFull6CatV2:
		return BuildUserMessage(tag1, tag2, valueNorm,
			offchainContent, agentDescription, agentServices, agentTags)
	default:
		return BuildUserMessageCompact3B(tag1, tag2, valueNorm,
			offchainContent, agentDescription, agentServices, agentTags)
	}
}
