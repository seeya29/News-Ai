# Learning Kit: Async Pipelines & Modular Feeds

This kit helps you understand, extend, and teach the single-folder pipeline. It focuses on async Python, modular feed architecture, and how agents interact in the Bucket.

## 1) Async Python Pipelines (Quick Intro)
- Why async: handle I/O-bound work (HTTP, file, RSS) concurrently without threads.
- Core pieces: `async def`, `await`, `asyncio.gather`, `aiohttp.ClientSession`.
- Pattern:
  ```python
  async def fetch_one(session, url):
      async with session.get(url) as resp:
          return await resp.text()

  async def fetch_many(urls):
      async with aiohttp.ClientSession() as session:
          tasks = [fetch_one(session, u) for u in urls]
          return await asyncio.gather(*tasks, return_exceptions=True)
  ```
- Error handling: wrap tasks; return sentinel items with `raw.error` to keep the pipeline moving.
- Backpressure: set `limit`, timeouts; avoid unbounded concurrency.

## 2) Agents in the Bucket (Overview)
- Fetchers: pull raw items (RSS, APIs, stubs). Each implements `fetch()` or `fetch_async()`.
- ScriptGenAgent: composes newsroom-style scripts from items.
- TTSAgentStub: turns scripts into voice payloads via `VaaniTools` stub.
- AvatarAgentStub: renders the final avatar bundle from script + voice.
- Logging: `PipelineLogger` prints lightweight events across stages.

### Day 2 Additions
- FilterAgent: multi-language detection (en/hi/ta/bn), Uniguru integration for category/tone/audience, and `dedup_flag` via RAG cache.
- RAGClient: local JSON cache for dedup and keyword search; simple and offline-friendly.

### Uniguru Endpoint Integration (Quick Guide)
- Purpose: improve tagging quality (category, tone, audience) beyond heuristics.
- Environment:
  - `UNIGURU_PROVIDER`: set to `local` to use `providers/uniguru_local/` adapter, or leave unset to use `UniguruClient` if available.
  - Optional: `UNIGURU_ENDPOINT`, `UNIGURU_API_KEY` for cloud clients (if your `UniguruClient` implementation uses them).
- Expected request (conceptual):
  ```json
  {
    "title": "Example Headline",
    "body": "Story body text...",
    "language": "en"
  }
  ```
- Expected response fields:
  ```json
  {
    "category": "technology",
    "tone": "formal",
    "audience": "general"
  }
  ```
- Wiring in code: `FilterAgent` tries `UniguruLocalAdapter` when `UNIGURU_PROVIDER=local`, else `UniguruClient` if importable.
- Failure handling: if tagging fails, defaults to `{category: None, tone: None, audience: "general"}` to keep the pipeline moving.

## 3) Modular Feed Architecture
- Registry-driven: connectors defined in `feed_registry.json` or `feed_registry.yaml`.
- Adapter-based: each entry declares a module + class/function and params.
- Plug-and-play: add an entry, no code changes; the hub loads and runs it.
- Mix sync + async: `FetcherHub` detects `fetch_async` and runs concurrently.
- Extensibility: implement new fetchers under `single_pipeline/fetchers/*`.

### Example: Add a new RSS connector
```yaml
my_blog_rss:
  adapter: RSSFetcher
  module: single_pipeline.fetchers.rss_fetchers
  class: GenericRSSFetcher
  params:
    url: https://example.com/feed
```
Run: `python cli.py fetch --sources my_blog_rss --registry feed_registry.yaml --out-prefix single --pretty`

### Example: Add a domain API
```yaml
weather_api:
  adapter: APIFetcher
  module: single_pipeline.fetchers.api_fetchers
  class: DomainAPIFetcher
  params:
    base_url: https://api.weather.gov
    endpoint: /alerts
    query:
      area: CA
```

## 4) Quick Guides
- Concurrency: prefer batches (`sources=a,b,c`) and let hub gather tasks.
- Offline testing: use stubs like `telegram_stub`, `generic_rss`.
- Observability: inspect `single_items.json` after fetch; keep `raw` fields.
- Safety: avoid official APIs unless you have keys; prefer RSS/Atom mirrors.
- Limits: set sensible `limit` per connector to avoid rate limits.

## 5) Prompt Engineering for Tone & Style Variation
- Goal: get consistent tone (`formal`, `kids`, `youth`, `devotional`) while keeping factual content.
- Technique:
  - Use system-style instructions to set editorial voice.
  - Include audience hints (e.g., "Explain to school-age children").
  - Provide constraints: length, bullets vs. prose, avoid jargon.
- Example prompt template:
  ```text
  Rewrite the following news into a {tone} style for {audience}.
  Constraints: be clear, avoid slang unless suitable, keep it concise.
  Headline: {title}
  Body:
  {body}
  ```
- Testing: try the same story across tones; inspect differences and adherence.

## 6) LoRA/PEFT Fine-Tuning Basics
- When: you want tone/style consistency beyond prompts (category-specific).
- Concepts:
  - PEFT: Parameter-Efficient Fine-Tuning modifies a small subset of weights.
  - LoRA: Low-Rank Adapters inject trainable matrices to specialize the base model.
- Steps (high-level):
  - Collect category-specific examples (e.g., finance, wellness) with target tone.
  - Prepare small instruction pairs (input → desired output).
  - Train LoRA adapters; evaluate with validation prompts.
  - Deploy as an endpoint or load adapters at runtime.
- Practical tip: start with prompts; add LoRA only if consistency gaps persist.

## 7) Agentic Chaining: Filter → Script (Dedicated Guide)
- Objective: pass enriched metadata from FilterAgent into ScriptGenAgent to tailor scripts.
- FilterAgent outputs: `lang`, `category`, `tone`, `audience`, `dedup_flag`.
- ScriptGenAgent consumes: `tone`, `audience` to select style variants.
- Data flow:
  1. Fetch → `items.json`
  2. Filter → adds metadata → `filtered.json`
  3. Script → uses tone/audience → `scripts.json`
- Context-aware enrichment: ScriptGenAgent now queries `RAGClient.search()` with title + body terms and appends a brief "Related Context" block from prior cached stories.
- CLI chaining (already supported): later stages prefer prior outputs if present.
- Exercise:
  - Fetch 20+ items, run filter, then script.
  - Verify scripts include correct `[Style: … | Audience: …]` header.
  - Confirm a "Related Context" block appears when the cache has similar items.
  - Toggle tones (`formal`, `kids`, `youth`, `devotional`) and review differences.

## 6) Mapping External Pipelines
- Gurukul, StockAgent, WellnessBot, UsedCar can be bridged via registry adapters:
  - File adapter: point to a wrapper file that exposes `fetch()` returning normalized items.
  - Module adapter: import existing modules and specify the fetcher class.
- Normalize into `{title, body, timestamp, raw}`; keep `raw` for diagnostics.

## Mini-Exercise: Deduplicate 20 Items
- Run `python cli.py fetch --sources reddit_rss,hnrss --out-prefix demo` to collect ~20+ items.
- Run filter stage; inspect `demo_filter.json` for `dedup_flag` and `rag_cache.json`.
- Modify threshold in `rag_client.py` to see sensitivity changes.

## 5) What to Teach
- Compose and test small fetchers; measure latency.
- Design registries for portability (JSON/YAML), minimal params.
- Build a pipeline end-to-end (fetch → script → voice → avatar) offline.
- Swap connectors with only registry changes.

## 6) Mapping Akash’s Pipelines (Gurukul, StockAgent, WellnessBot, UsedCar)
- Goal: map existing fetchers/scrapers from external pipelines into this registry-driven hub without rewriting their core logic.
- Preferred approaches:
  - Write a thin `BaseFetcher` adapter in `fetchers/` that calls the external module/library and normalizes output to `{title, body, timestamp, raw}`.
  - Use `FunctionAdapterFetcher` when the external code exposes a simple `run()`/`process()` function returning items.
- Registry examples:
```yaml
# Thin adapter class living in fetchers/gurukul_fetchers.py
gurukul_feed:
  file: fetchers/gurukul_fetchers.py
  class: GurukulFetcher
  params:
    source: lectures
    limit: 10

# Function adapter pointing to an external script
stockagent_feed:
  file: function_adapter.py
  class: FunctionAdapterFetcher
  params:
    target_file: external/stockagent_scraper.py
    target_function: run
```
- Normalization checklist:
  - Ensure each item includes `title`, `body`, `timestamp` (or `None`), `raw` for original fields.
  - Fail gracefully: on errors, return a single item with `raw.error`.
  - Keep params minimal and documented in the registry.
\n+### Adapter Recipe: Bridging Gurukul/Stock/Wellness/UsedCar
This recipe shows how to convert your real pipeline code into plug-and-play adapters.
\n+1) Create a thin class adapter (recommended)
```python
# file: single_pipeline/fetchers/gurukul_fetchers.py
from typing import Any, Dict, List, Optional
from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger

# Replace this with your real client import
from external.gurukul import GurukulClient  # your codebase

class GurukulFetcher(BaseFetcher):
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        self.source = cfg.get("source", "lectures")
        self.client = GurukulClient()

    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        try:
            rows = self.client.list(self.source, limit=limit)
            for r in rows:
                items.append({
                    "title": r.get("title") or "Untitled",
                    "body": r.get("content") or "",
                    "timestamp": r.get("timestamp"),
                    "raw": r,
                })
        except Exception as e:
            items = [{
                "title": "Gurukul fetch error",
                "body": str(e),
                "timestamp": None,
                "raw": {"error": str(e)}
            }]
        if logger:
            logger.log_event("fetch", {"connector": "gurukul_feed", "count": len(items)})
        return items
```
\n+2) Use a function adapter when your code exposes a function
```python
# file: external/stockagent_scraper.py (your code)
def run(limit: int = 10):
    # return a list of dicts with your source fields
    return [{"headline": "...", "text": "...", "ts": 1700000000}]

# registry entry
stockagent_feed:
  file: function_adapter.py
  class: FunctionAdapterFetcher
  params:
    target_file: external/stockagent_scraper.py
    target_function: run
    map_keys:
      title: headline
      body: text
      timestamp: ts
```
\n+3) YAML/JSON registry entries
```yaml
wellness_feed:
  file: fetchers/external_adapters.py
  class: WellnessBotAdapterFetcher
  params:
    limit: 10

usedcar_feed:
  file: fetchers/used_car_fetchers.py
  class: UsedCarFetcher
  params:
    region: IN
    limit: 20
```
\n+4) Normalization & error handling
- Required fields: `title` (string), `body` (string), `timestamp` (epoch or ISO, optional), `raw` (original dict for audit).
- On exceptions: return one sentinel item with `raw.error` so the pipeline continues.
- Logging: call `logger.log_event("fetch", {"connector": key, "count": N})` for observability.
\n+5) Testing the adapter
- Add/update the registry entry (`feed_registry.yaml` or `.json`).
- Run: `python single_pipeline/cli.py fetch --sources gurukul_feed --registry single_pipeline/feed_registry.yaml --pretty`
- Inspect `single_pipeline/output/demo_items.json` for normalized shape.
- Chain to filter/script: `python single_pipeline/cli.py full --sources gurukul_feed --pretty`
\n+6) Concurrency & limits
- If your external client supports async, implement `fetch_async()` on the adapter; the hub will run it concurrently.
- Otherwise, provide `fetch()`; the hub will offload sync calls to a thread pool.
- Use `limit` from registry params or CLI to avoid rate limits.
\n+7) Configuration
- Read credentials/keys from environment vars (e.g., `GURUKUL_API_KEY`) inside your adapter.
- Provide minimal `params` in registry to keep adapters portable across environments.
\n+8) Migration tips
- Start by wrapping your current outputs “as-is” under `raw` and move normalization gradually.
- Keep adapter logic thin; heavy transforms belong in FilterAgent or downstream agents.

## 8) LangGraph Visualization & Debugging (Optional)
- Objective: visualize stage transitions and inspect payloads.
- Setup: `pip install langgraph` (optional; not required for core pipeline).
- Concept: nodes represent agents (`Filter`, `Script`, `TTS`, `Avatar`); edges represent data flow.
- Minimal example (pseudo):
  ```python
  from langgraph.graph import Graph

  g = Graph()
  g.add_node("Filter")
  g.add_node("Script")
  g.add_node("TTS")
  g.add_node("Avatar")
  g.add_edge("Filter", "Script")
  g.add_edge("Script", "TTS")
  g.add_edge("Script", "Avatar")  # Avatar uses script + metadata
  g.draw(path="single_pipeline/output/pipeline_graph.png")
  ```
- Debugging: log inputs/outputs per node using `PipelineLogger`; capture payload snapshots.

## 9) Automation Tips: Multi-language Voice/Avatar Sync
- Keep `lang` consistent from Filter → Script → TTS → Avatar.
- Tone: set one of (`formal`, `kids`, `youth`, `devotional`) in Filter; Script retains it.
- Audience: choose (`general`, `students`, `investors`, etc.) in Filter for persona selection.
- TTS routing: selects voice via `lang` + `tone`; fallback to neutral if unknown.
- Avatar persona: chooses via `lang` + `tone` + `audience`; fallback to `Avatar_Neutral`.
- Practice:
  - Run test runner to produce per-category scripts/voice/avatar.
  - Inspect `*_voice.json` and `*_avatar.json` for consistent metadata.
  - If tone/audience missing, update FilterAgent rules or source hints.

## 10) QA Best Practices for Agent Pipelines
- Build synthetic fixtures: duplicates, empty fields, malformed structures, mixed languages.
- Sanitize early: convert non-string bodies to strings, default missing titles.
- Deterministic logging: log stage + counts + key payloads for reproducible QA.
- Snapshot payloads: save JSON at each stage to inspect transformations.
- Coverage strategy: test at least 3 stories per category and across key languages.
- Dedup strategy: verify hash matches and token overlap handling; track false positives.
- Failure handling: ensure stages tolerate missing fields and skip or sanitize gracefully.

## 11) Core/Bucket Bridges (Design Notes)
- Purpose: orchestrate multi-agent flows between Core (aggregation/analysis) and Bucket (distribution/publishing).
- Bridge contracts: define message schemas and normalized fields (`title`, `body`, `lang`, `tone`, `audience`, `category`).
- Idempotency: include stable IDs (`dedup_key`) and timestamps to avoid reprocessing.
- Fallbacks: allow partial data to flow with defaults, reserving enrichments for Core.
- Routing: select downstream personas/voices based on `lang + tone + audience` metadata.
- Note: implementation intentionally deferred per request; use this as a planning guide.

## 12) MCP & Automation Concepts (Preview)
- Model Context Protocol (MCP): standardize agent-to-tool interactions with typed messages.
- Connectors: define tools for fetchers, taggers, cache/search, and renderers.
- Scheduling: cron-like automation to run pipeline segments periodically.
- Triggers: event-driven processing (new feed item → filter → script → voice/avatar).
- Metrics: collect throughput, dedup rate, error/retry counts, language distribution.
- Audit: log structured events with payload hashes for traceability.

## 13) Next Steps
- Add auth-enabled fetchers (Telegram Bot API, Twitter v2) behind opt-in.
- Introduce caching and retry policies.
- Package CLI options for registry path, sources, limits, and output prefix.

## 14) Bucket Agents Overview (Quick Reference)
- Agents in this pipeline map to Bucket-style roles:
  - `FilterAgent`: normalization, language/tone/audience tagging, dedup flags.
  - `ScriptGenAgent`: rewrite into audience-aware scripts with tone variants.
  - `TTSAgentStub`: language + tone routing for voice content.
  - `AvatarAgentStub`: persona selection via `lang + tone + audience` + script bundling.
- Data contract: items always carry `title`, `body/script`, `lang`, `tone`, `audience`, `timestamp`, and `raw`.
- Personalization: audience field covers Bhishma-style hooks (e.g., `students`, `investors`).

## 15) Downstream JSON Mapping (Bridge Ready)
- Goal: map pipeline JSON outputs to downstream modules (visual, avatar, distribution).
- Script JSON (`*_scripts.json`):
  - keys: `title`, `script`, `lang`, `tone`, `audience`, `timestamp`, `raw`
  - downstream: script renderers, subtitle generators, content reviewers.
- Voice JSON (`*_voice.json`):
  - keys: `title`, `voice` (text payload in stub), `lang`, `tone`, `timestamp`, `raw`
  - downstream: TTS systems, audio pipelines (replace stub payload with audio URI).
- Avatar JSON (`*_avatar.json`):
  - keys: `title`, `persona`, `script`, `lang`, `tone`, `audience`, `timestamp`, `raw`
  - downstream: avatar renderers, scene composers, CMS modules.
- Contract tip: keep `lang + tone + audience` consistent across chain; store `raw` for audit.