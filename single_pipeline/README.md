# Single-Folder Pipeline

A minimal, self-contained content pipeline with five stages: fetch, filter, script, voice, and avatar. Everything lives in this one folder.

## Prerequisites
- Python 3.10+ (3.11 recommended)
- Windows PowerShell or any terminal

## Install
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Run
The pipeline writes outputs into `./output`.

- Fetch items (choose sources via registry):
```bash
python cli.py fetch --sources telegram_stub,youtube_rss,x_nitter,domain_api --registry feed_registry.yaml --out-prefix demo --pretty
```

- Generate scripts from fetched items:
```bash
python cli.py scripts --out-prefix demo --pretty
```

- Synthesize voice (stubbed, no audio dependencies required):
```bash
python cli.py voice --out-prefix demo --pretty
```

- Render avatar bundle (stub persona + script):
```bash
python cli.py avatar --out-prefix demo --pretty
```

- Bucketed orchestration (routes by language+tone, runs scripts→voice→avatar):
```bash
python -m single_pipeline.cli buckets --registry single --category general
```

## Quick Start (Project Root)
- Create venv and install requirements:
```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt   # installs all project Python deps
```
- End-to-end stages (run from project root):
```powershell
python -m single_pipeline.cli fetch --sources telegram_stub,youtube_rss,x_nitter,domain_api --registry single_pipeline/feed_registry.yaml --out-prefix demo --pretty
python -m single_pipeline.cli filter --out-prefix demo --pretty
python -m single_pipeline.cli scripts --out-prefix demo --pretty
python -m single_pipeline.cli voice --out-prefix demo --pretty
python -m single_pipeline.cli avatar --out-prefix demo --pretty
python -m single_pipeline.cli buckets --registry demo --category general
```
- Category test runner (3 stories per category):
```powershell
python single_pipeline/test_category_runner.py --sources gurukul_stub,stock_agent_stub,wellness_bot_stub,used_car_stub,telegram_stub,youtube_rss,x_nitter,domain_api --limit 100 --out-prefix category_test --pretty
```
- QA harness (edge cases):
```powershell
python single_pipeline/qa_harness.py --out-prefix qa --pretty
```
- Tip: If you `cd single_pipeline`, you can use `python cli.py ...` instead of `python -m single_pipeline.cli ...`.

### Alternative: Install only single_pipeline deps
```powershell
pip install -r single_pipeline/requirements.txt
```

## How to Run (Step-by-Step)

### LLM Local Mode (No External Folder Linkage)
- Set `UNIGURU_PROVIDER=local` to use a local adapter bundled under `single_pipeline/providers/uniguru_local/`.
- Drop your Uniguru LLM zip contents inside `single_pipeline/providers/uniguru_local/` and implement your logic inside `adapter.py` (method `tag_text(title, body, language)` returning `category`, `tone`, `audience`).
- This keeps all LLM code self-contained in `single_pipeline` so you can delete any external LLM folders without breaking the pipeline.
- To use HTTP mode instead, leave `UNIGURU_PROVIDER` unset and configure `UNIGURU_BASE_URL`, `UNIGURU_TAG_PATH`, and `UNIGURU_API_KEY` as documented.

### Dedup Strategy (Built-in, No Embeddings Required)
- Deduplication uses a lightweight RAG cache at `single_pipeline/output/rag_cache.json`.
- Strategy: hash + token-overlap to avoid near-duplicate items across runs.
- No embeddings provider is required; the embeddings adapter and external folders have been removed.
- Optional future integration: you may add an embeddings/Qdrant client later if desired, but it is not needed for core functionality.
1. Create and activate a virtual environment (Windows):
   - `python -m venv .venv`
   - `.\.venv\Scripts\activate`
2. Install all project Python dependencies from the root:
   - `pip install -r requirements.txt`
3. Fetch items from selected sources:
   - `python -m single_pipeline.cli fetch --sources telegram_stub,youtube_rss,x_nitter,domain_api --registry single_pipeline/feed_registry.yaml --out-prefix demo --pretty`
4. Filter items to add `lang`, `category`, `tone`, `audience`, and `dedup_flag`:
   - `python -m single_pipeline.cli filter --out-prefix demo --pretty`
5. Generate scripts (tone/audience-aware):
   - `python -m single_pipeline.cli script --out-prefix demo --pretty`
6. Synthesize voice (stub routing by `lang + tone`):
   - `python -m single_pipeline.cli voice --out-prefix demo --pretty`
7. Render avatar bundle (persona by `lang + tone + audience`):
   - `python -m single_pipeline.cli avatar --out-prefix demo --pretty`
8. Optional — test 3 stories per category end-to-end:
   - `python single_pipeline/test_category_runner.py --sources gurukul_stub,stock_agent_stub,wellness_bot_stub,used_car_stub,telegram_stub,youtube_rss,x_nitter,domain_api --limit 100 --out-prefix category_test --pretty`
9. Optional — run QA harness for edge cases:
   - `python single_pipeline/qa_harness.py --out-prefix qa --pretty`

Outputs are written to `single_pipeline/output/` (e.g., `demo_items.json`, `demo_filtered.json`, `demo_scripts.json`, `demo_voice.json`, `demo_avatar.json`).

## Server Endpoints (FastAPI)
- Start the API server:
```powershell
uvicorn server.app:APP --reload
```
- All endpoints require `Authorization: Bearer <jwt>`; per-user rate limiting is applied and reported via `X-RateLimit-*` headers.

### Feed (Grouped by Dedup Key)
- `GET /api/articles/feed/{user_id}`
  - Query: `limit` (1–100), `page` (>=1), optional `category` (`general|finance|tech|science`).
  - Returns grouped articles: representative per dedup group with `group_key`, `title`, `summary`, `metadata.published_at`.
  - Example:
```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8000/api/articles/feed/demo_user?limit=20&page=1&category=general"
```

### Trending (Aggregated by Group)
- `GET /api/articles/trending`
  - Query: `timeframe` (`1h|24h|7d`), optional `category`, `limit` (1–100).
  - Returns `trending_score`, `engagement_rate`, `views`, `shares` for each group representative.
  - Example:
```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8000/api/articles/trending?timeframe=24h&category=general&limit=20"
```

### Voice Generation
- `POST /api/agents/voice/generate`
  - Body: `{ "registry": "single", "category": "general", "voice": "en-US-Neural-1" }`
  - Triggers the voice stage and writes `single_pipeline/output/<registry>_voice.json`.
  - Example:
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"registry":"single","category":"general","voice":"en-US-Neural-1"}' \
  http://127.0.0.1:8000/api/agents/voice/generate
```

### Avatar Rendering
- `POST /api/agents/avatar/render`
  - Body: `{ "registry": "single", "category": "general", "style": "news-anchor" }`
  - Triggers the avatar stage and writes `single_pipeline/output/<registry>_avatar.json`.
  - Example:
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"registry":"single","category":"general","style":"news-anchor"}' \
  http://127.0.0.1:8000/api/agents/avatar/render
```

### Full Pipeline Run
- `POST /api/pipeline/run`
  - Body: `{ "registry": "single", "category": "general", "voice": "en-US-Neural-1", "style": "news-anchor" }`
  - Chains stages: fetch → filter → scripts → voice → avatar.
  - Returns output file paths for each stage under `single_pipeline/output/`.
  - Example:
```bash
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"registry":"single","category":"general","voice":"en-US-Neural-1","style":"news-anchor"}' \
  http://127.0.0.1:8000/api/pipeline/run
```

### Notes on Grouping & Dedup
- Grouping is performed via a RAG-lite dedup key (`group_key`) combining hash and token-overlap.
- The feed endpoint returns one representative per group; trending aggregates views/engagement/shares per group.
- File-backed outputs: `output/rag_cache.json` maintains recent item signatures for dedup across runs.

### Test: 3 Stories per Category (End-to-End)
- Purpose: produce per-category outputs for scripts, voice, and avatar using up to 3 items per category.
- Command:
```bash
python single_pipeline/test_category_runner.py --sources gurukul_stub,stock_agent_stub,wellness_bot_stub,used_car_stub,telegram_stub,youtube_rss,x_nitter,domain_api --limit 100 --out-prefix category_test --pretty
```
- Outputs (written under `single_pipeline/output/`):
  - `category_test_items.json`, `category_test_filtered.json`, `category_test_summary.json`
  - Per category: `category_test_<category>_scripts.json`, `category_test_<category>_voice.json`, `category_test_<category>_avatar.json`

### QA Harness: Edge Cases (Duplicates, Empty, Malformed)
- Purpose: validate pipeline resilience to common edge cases and produce a QA report.
- Command:
```bash
python single_pipeline/qa_harness.py --out-prefix qa --pretty
```
- Outputs (written under `single_pipeline/output/`):
  - `qa_items.json`, `qa_filtered.json`, `qa_scripts.json`, `qa_voice.json`, `qa_avatar.json`, `qa_report.json`
  - Report summarizes duplicates flagged, empty bodies, malformed items, and language distribution.

## Day 1–5 Sprint Capabilities
- Day 1: Plug-and-play feed hub (JSON/YAML registry) with 10+ sources.
- Day 2: Multi-language filter with Uniguru tags (category/tone/audience) and dedup via local RAG cache.
- Day 3: ScriptGen style variants driven by tone and audience.
- Day 4: Language + tone routing for TTS/Avatar stubs.
- Day 5: Logging across stages; sample outputs in `output/`.

### Uniguru LLM Tagging (Optional)
- Configure env: `UNIGURU_BASE_URL` (e.g., http://localhost:3000), optional `UNIGURU_API_KEY`.
- Endpoint path override: `UNIGURU_TAG_PATH` (default `/api/llm/tag`).
- If not set, a heuristic fallback tags as general/neutral.

### RAG Dedup (Initial)
- Lightweight cache at `output/rag_cache.json` performs hash + token-overlap dedup.
- No external DB required; optional Qdrant/embeddings can be added later.

## Dynamic Feed Registry (JSON/YAML)
- The hub can load registries from either JSON (`feed_registry.json`) or YAML (`feed_registry.yaml`).
- Switch registries via `--registry`:
```bash
python cli.py fetch --sources telegram_stub,youtube_rss --registry single_pipeline/feed_registry.yaml --out-prefix demo --pretty
```

### Plug-and-Play Sources (10+)
Configured in `feed_registry.yaml`:
- `telegram_stub` (offline JSON messages)
- `youtube_rss` (YouTube channel RSS)
- `x_nitter` (X/Twitter via Nitter RSS)
- `reddit_rss` (subreddit RSS)
 - `domain_api` (generic API fetcher)
 - Adapters for external pipelines (stubs): Gurukul, StockAgent, WellnessBot, UsedCar
- `medium_rss` (Medium tag RSS)
- `stackoverflow_rss` (StackOverflow tag RSS)
- `github_atom` (GitHub commits Atom)
- `generic_rss` (any RSS URL)
- `domain_api` (async HTTP JSON API)
- `generic_json` (sync HTTP JSON API)
- `http_json` (HTTP JSON adapter with mapping)
- `hackernews` (HN top stories)

## Agents Folder
- Agents are organized under `agents/`:
  - `agents/filter_agent.py`: Multi-language detection (English, Hindi, Tamil, Bengali)
  - `agents/script_gen_agent.py`: Script generation
  - `agents/tts_agent_stub.py`: Stub TTS
  - `agents/avatar_agent_stub.py`: Stub avatar bundling
- CLI imports updated to use `agents.*` modules.

## Files
- `cli.py`: Orchestrates stages and manages outputs
- `fetcher_hub.py`: Loads fetchers from registries (JSON/YAML), runs sync/async concurrently
- `vaani_tools.py`: Local VaaniTools stub (no external ML dependencies)
- `rag_client.py`: Stub RAG client
- `feed_registry.json`: JSON registry with approved connectors
- `feed_registry.yaml`: Dynamic registry with 10+ connectors across RSS/APIs/stubs
- `fetchers/`: Modular fetchers (RSS, APIs, Telegram stub, Nitter)
- `LEARNING_KIT.md`: Intro to async pipelines and modular feeds

## Bucketed Orchestration (Routing, Priority, Concurrency)

Run a bucketed multi-agent flow that routes items by `(language, tone)` into fixed buckets and processes them across `scripts → voice → avatar`:

- Command:
  - `python -m single_pipeline.cli buckets --registry <prefix> --category <category>`
  - `<prefix>` must correspond to your filtered output file name: `single_pipeline/output/<prefix>_filtered.json`.

- Routing table:
  - `(hi, youth) → HI-YOUTH`, `(hi, news) → HI-NEWS`, `(en, news) → EN-NEWS`, `(en, kids) → EN-KIDS`, `(ta, news) → TA-NEWS`, `(bn, news) → BN-NEWS`.
  - Default bucket: `EN-NEWS` for any unmapped pair.

- Priority order:
  - `HI-NEWS`, `EN-NEWS`, `HI-YOUTH`, `EN-KIDS`, `TA-NEWS`, `BN-NEWS`.

- Concurrency limits:
  - Global workers: `min(4, os.cpu_count())`.
  - Per-bucket caps: `EN-NEWS: 2`, others: `1` (scheduler respects global cap).

- Error handling:
  - Retries with exponential backoff: `1s → 2s → 4s`, `max_retries=3`.
  - Dead-letter queue appends JSON lines to `single_pipeline/data/dead_letter/{stage}.jsonl`.

- Outputs:
  - Per-bucket stage files at `single_pipeline/output/{prefix}_{bucket}_{stage}.json` for `scripts`, `voice`, `avatar`.

- Tracing (LangGraph-friendly):
  - Redacted JSONL traces stored under `single_pipeline/data/traces/` per stage.
  - Each entry:
    ```json
    { "trace_id": "uuid", "stage": "FilterAgent|ScriptGenAgent|TTSAgent|AvatarAgent", "input": { ... }, "output": { ... }, "ts": "ISO", "status": "running|success|failed" }
    ```
  - Sensitive fields like `user_id`, `handle`, `email`, `ip` are masked as "***".

## Customize
- Add a new connector by defining a fetcher class and wiring it in the registry.
- RSS connectors accept `feed_url`; YouTube accepts `channel_id`; Nitter uses `username`.
- API connectors accept `base_url` + `endpoint` + `query` or direct `url`.

## Notes
- This setup avoids external APIs and heavy dependencies by default. If you want real TTS or RAG, swap stubs with actual clients and update `requirements.txt` accordingly.

## Live Ingestion + API Feed (Database-backed)
- Prepare environment variables (project root):
  - Create an `.env` file. On Windows PowerShell: `copy .env.example .env`
  - Fill values:
    - `TELEGRAM_API_ID`
    - `TELEGRAM_API_HASH`
    - `TWITTER_BEARER_TOKEN`
    - `YOUTUBE_API_KEY` (optional; RSS fallback is used if missing)
- Confirm sources configuration:
  - `single_pipeline/data/sources.json` already includes Telegram channels, X handles, YouTube channel IDs, and per-source cadences.
- Install live ingestion libraries (in your activated venv):
  - `pip install telethon tweepy feedparser python-dotenv`
  - Optional: `pip install google-api-python-client` (YouTube Data API)
  - Optional: `pip install snscrape` (X fallback if API is limited)
- Start the ingestion worker (writes to SQLite DB):
  - `python single_pipeline/ingest.py`
  - Reads `.env` and `single_pipeline/data/sources.json`; stores articles in `single_pipeline/data/app.db`.
- Start the API server (serves DB-backed feed with file fallback if DB empty):
  - `uvicorn server.app:APP --reload`
  - Verify at: `http://127.0.0.1:8000/api/articles/feed?limit=20`
- Notes & tips:
  - If Telegram prompts for a login code on first run, enter it in terminal; otherwise ensure keys are correct.
  - X API may be rate-limited; `snscrape` provides a non-API fallback when installed.
  - YouTube uses RSS if no API key; when a key is present, the Data API is used.