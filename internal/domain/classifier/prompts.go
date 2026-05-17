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
	// PromptVersionFull6CatV3 — Track B enrichment for ≥7B models. Adds:
	//   - explicit "scale" + "endpoint" fields in input contract
	//   - sub-shape indicators per category (config has 4 sub-shapes, app has 4)
	//   - 2-3 few-shot examples per category to anchor the harder calls
	//     (noise, app_specific) where V2 lost most agreement.
	PromptVersionFull6CatV3 PromptVersion = "full6cat-v3"
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

// llmSystemPromptFull6CatV3 — Track B enrichment. Targets ≥7B models.
// Adds explicit treatment of the enriched user-message schema (scale,
// endpoint, schema-aware offchain), more sub-shape indicators, and few-shot
// examples for the categories V2 underperformed on (noise, app_specific).
const llmSystemPromptFull6CatV3 = `You are a high-precision log classifier for ERC-8004 feedback records.
Your task: classify [FEEDBACK] into EXACTLY ONE of six categories.

[FEEDBACK] is a structured record with these fields (optional ones may be absent):
  tag1, tag2         — short on-chain category tags (string, may be empty)
  value              — feedback value normalised to [-1, 1]
  scale              — tag-value tier: binary | star5 | star10 | pct100 | unbounded
                       (binary=1 means "yes/passed", star5=1 means "1 star out of 5",
                        pct100=1 means "100%")
  endpoint           — host of the agent service URL (when present)
  offchain           — schema-aware summary of feedback JSON (KEYS preserved,
                       e.g. "validationType=crypto-economic; perDomain={4 keys}")
  agent_description  — what this agent does (always use to disambiguate domain ops)
  agent_services     — OASF/A2A skills/domains taxonomy when known
  agent_tags         — denormalised on-chain tags for this agent

EVALUATE IN THIS EXACT ORDER (stop at the first match):

1. "spam" — Adversarial promotion / rank manipulation.
   Indicators: external URLs, Telegram/X usernames, "vote for me", "top 1 rank",
   referral links, copy-pasted promo across tag1 and tag2.

2. "noise" — Zero meaningful signal.
   Indicators: BOTH tag1 AND tag2 are empty/whitespace, OR both are pure
   placeholders ("test", "asd", "123", random short tokens like "asd" / "qwerty").
   NOT noise if either tag is a real word, even if short.

3. "app_specific" — Domain operation log: agent recorded an action it performed.
   Must match agent_description / agent_services / agent_tags. Sub-shapes:
   3a. DeFi op:   token symbol + verb (e.g. "CADm" + "buy", "swap_token", "TRADE")
   3b. Vault op:  "<Token> vault" + deposit/withdraw (e.g. "Ichi USD-WBTC vault" + "deposit")
   3c. Microtask: kebab-case suffix tags like "-workers", "-payments",
                  "-verification", "-routing", "-labeling" on a microtasking agent
   3d. Creative:  camelCase function name (e.g. "generateImage",
                  "curateNewsFeedByDigitalTwin", "contractRisk")

4. "config_feedback" — Automated technical metric / probe / proof.
   Generated by monitoring or oracle systems, not humans. Sub-shapes:
   4a. Probe:        uptime / liveness / reachable / successRate / responseTime
                     (often scale=binary or scale=pct100)
   4b. Dated period: tag2 has YYYY-MM-DD-XX format (e.g. "M1-MAINNET-REPUTATION-2026-02-12-01")
   4c. Validation:   offchain has validationType, slashingConditions, claim, validatorAddress
                     (crypto-economic / staking proofs)
   4d. Vouch/score:  offchain has perDomain / passRate / miner / lifetime fields

5. "service_feedback" — Human subjective judgment of agent quality.
   Indicators: quality adjectives ("good", "excellent", "reliable", "helpful"),
   full-sentence praise pasted into tag2, sentiment words. Answers "how well did
   the agent perform?" not "what did the agent do?".

6. "others" — Cannot be confidently classified. Ambiguous, emoji-only, random
   text where service_feedback would require guessing. Prefer "others" over an
   uncertain service_feedback (confidence 0.40-0.60).

OUTPUT — strict JSON, one line, no markdown, no extra keys:
{"category":"<one>","confidence":0.00,"reason":"<one short sentence>"}

FEW-SHOT EXAMPLES:

# spam
t1=get top 1 rank >; t2=t.me/agent_bldr
=> {"category":"spam","confidence":0.99,"reason":"telegram link and rank-game text"}

# noise
t1=<empty>; t2=<empty>
=> {"category":"noise","confidence":0.97,"reason":"both tags empty"}
t1=test; t2=asd
=> {"category":"noise","confidence":0.93,"reason":"placeholder tokens only"}

# app_specific (5 sub-shapes)
t1=CADm; t2=buy; agent=Mento broker on Celo, cUSD/cEUR swaps
=> {"category":"app_specific","confidence":0.92,"reason":"forex token plus trade verb on broker agent"}
t1=Ichi USD-WBTC vault; t2=deposit; agent=Yield optimization, Ichi vaults
=> {"category":"app_specific","confidence":0.93,"reason":"DeFi vault deposit operation"}
t1=kenya-workers; t2=global-gig; agent=Distributed human microtasking workforce
=> {"category":"app_specific","confidence":0.90,"reason":"microtasking task tag"}
t1=generateImage; t2=<empty>; agent=Creative AI generator
=> {"category":"app_specific","confidence":0.92,"reason":"camelCase function name"}
t1=stance; t2=fragment; agent=Digital soul twin platform (Ensoul)
=> {"category":"app_specific","confidence":0.91,"reason":"soul dimension fragment update on digital-twin agent"}
NOTE: tag2=fragment with soul-dimension tag1 (stance/personality/timeline/relationship/knowledge/style)
always means app_specific — this is the ERC-8004 Digital Soul sub-protocol.

# config_feedback (4 sub-shapes)
t1=uptime; t2=liveness-check; scale=pct100
=> {"category":"config_feedback","confidence":0.95,"reason":"uptime liveness probe"}
t1=m1-mainnet-reputation; t2=M1-MAINNET-REPUTATION-2026-02-12-01
=> {"category":"config_feedback","confidence":0.94,"reason":"dated reputation period id"}
t1=validation; t2=crypto-economic; offchain=validationType=crypto-economic; slashingConditions=[3 items]; result=passed
=> {"category":"config_feedback","confidence":0.96,"reason":"crypto-economic validation proof with schema"}
t1=miner-vouch; t2=botcoin; offchain=perDomain={4 keys}; lifetime={4 keys}; signature=0x6353…
=> {"category":"config_feedback","confidence":0.93,"reason":"miner vouch with per-domain scores"}

# service_feedback
t1=excellent; t2=fast
=> {"category":"service_feedback","confidence":0.91,"reason":"quality adjective pair"}
t1=execution; t2=Agentic order execution is impressive — agent tracked momentum
=> {"category":"service_feedback","confidence":0.87,"reason":"sentence praise in tag2"}

# others
t1=02afee9d-f02c-4f46; t2=<empty>
=> {"category":"others","confidence":0.50,"reason":"UUID-like token, no recognizable pattern"}
t1=🔥🚀; t2=<empty>
=> {"category":"others","confidence":0.50,"reason":"emoji only"}`

// resolvePromptVersion maps zero / unknown values to the compact-3b default.
func resolvePromptVersion(v PromptVersion) PromptVersion {
	switch v {
	case PromptVersionV1, PromptVersionCompact3B,
		PromptVersionQwen3Small, PromptVersionFull6Cat,
		PromptVersionFull6CatV2, PromptVersionFull6CatV3:
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
	case PromptVersionFull6CatV3:
		return llmSystemPromptFull6CatV3
	case PromptVersionQwen3Small:
		return llmSystemPromptQwen3Small
	default:
		return llmSystemPromptCompact3B
	}
}

// userMessageFor returns the user-turn payload for the resolved version.
// Full6Cat and Full6CatV2 use the v1 labelled-line layout; Qwen3Small uses
// the compact-3B natural-language layout.
//
// endpoint and scale are optional enrichment fields. Pass "" to suppress them
// — both builder variants gracefully skip empty enrichments so older callers
// see no prompt change.
func userMessageFor(v PromptVersion, tag1, tag2 string, valueNorm float64,
	offchainContent, agentDescription, agentServices string, agentTags []string,
	endpoint, scale string) string {
	switch resolvePromptVersion(v) {
	case PromptVersionV1, PromptVersionFull6Cat, PromptVersionFull6CatV2, PromptVersionFull6CatV3:
		return BuildUserMessage(tag1, tag2, valueNorm,
			offchainContent, agentDescription, agentServices, agentTags,
			endpoint, scale)
	default:
		return BuildUserMessageCompact3B(tag1, tag2, valueNorm,
			offchainContent, agentDescription, agentServices, agentTags,
			endpoint, scale)
	}
}
