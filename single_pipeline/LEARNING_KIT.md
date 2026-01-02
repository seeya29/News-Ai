# Learning Kit

## Async Python Pipelines
- Use `asyncio` to concurrently fetch from RSS, APIs, and live sources.
- Prefer non-blocking I/O for network calls; batch tasks and gather results.
- Separate orchestration from worker functions for maintainability.

## Existing Agents in Bucket
- FilterAgent: language detection, category/tone/audience tagging, dedup flag.
- ScriptGenAgent: generates bullets, headline, narration; respects tone/audience.
- TTSAgentStub: language + tone routing; file-backed audio outputs.
- AvatarAgentStub: preset mapping; renders stub MP4 or JSON metadata.
- BucketOrchestrator: routes by language/tone to prioritized buckets.

## Modular Feed Architecture
- Dynamic registry (YAML/JSON) defines feeds; convert to `sources.json`.
- FetcherHub reads `sources.json` and runs RSS, API, and live connectors.
- Each connector returns normalized items with title/body/timestamp/source.

## RAG Overview
- Lightweight cache stores recent items and hashes for dedup.
- `is_duplicate(title, body)` flags near-duplicates via token overlap.
- Optional embeddings provider can enhance search later.

## LLM Orchestration via Uniguru
- Local mode: `UNIGURU_PROVIDER=local` uses bundled adapter for tags.
- HTTP mode: configure `UNIGURU_BASE_URL`, `UNIGURU_TAG_PATH`, `UNIGURU_API_KEY`.
- Return fields: `category`, `tone`, `audience` for downstream routing.

## Mini-Exercise: Deduplicate News Items
- Prepare a list of mixed titles/bodies with slight variations.
- Run FilterAgent; inspect `dedup_flag` and groups in `rag_cache.json`.
- Verify representative selection across similar items.
- **Try it now**: Run `python scripts/verify_rag_filter.py` to see dedup in action.

## Prompt Engineering: Tone & Style
- Define style intents: formal, kids, youth, devotional.
- Structure prompts with clear context, constraints, and audience focus.
- Iterate quickly; log outputs for review and refinement.

## Fine-Tuning Basics (LoRA/PEFT)
- Use parameter-efficient adapters for domain/style specialization.
- Keep a small validation set; monitor drift across categories.
- Save adapters separately to enable quick swapping.

## Agentic Workflow Chaining
- Chain FilterAgent → ScriptGenAgent → TTS → Avatar.
- Pass language, tone, audience through each stage.
- Use bucketed orchestration to prioritize and parallelize work.

## LangGraph for Visualization
- Export stage traces to DOT/JSON; render dependency graphs.
- Inspect node transitions and metadata for debugging.
- Use graphs to identify bottlenecks and retry strategies.

## Mapping JSON Outputs
- Items: `*_items.json`; Filtered: `*_filtered.json`; Scripts: `*_scripts.json`.
- Voice: `*_voice.json` with `audio_url`; Avatar: `*_avatar.json` with `video_url`.
- Keep schemas stable to simplify downstream integration.
