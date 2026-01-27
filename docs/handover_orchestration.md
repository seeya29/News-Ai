# Orchestration Handover & Operations Guide

## 1. System Overview
The News-Ai Orchestration Pipeline converts raw feed items (RSS/API) into narrated video segments. It enforces a strict **Frozen Schema** (`contracts/orchestration_contract_v1.json`) to ensure compatibility across stages.

### Key Components
- **BucketOrchestrator**: Manages flow, concurrency, and priority buckets.
- **Agents**:
  - `FilterAgent`: Validates relevance, language, and quality.
  - `ScriptGenAgent`: Summarizes text into headlines and bullets.
  - `TTSAgentStub`: Generates audio (uses local `pyttsx3` or API).
  - `AvatarAgentStub`: Bundles audio and assets into a video object.
- **API Server**: FastAPI wrapper for UI integration.

## 2. Deployment & Usage

### Staging Deployment
To start the server in a production-like staging mode:
```powershell
.\scripts\start_staging.ps1
```
This script:
1. Sets secure environment variables.
2. Generates a valid Admin JWT.
3. Starts the API on `http://localhost:8000`.

### API Endpoints
Base URL: `http://localhost:8000`
Auth: `Authorization: Bearer <token>`

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/articles/feed/{user_id}` | GET | Returns personalized feed for the user. |
| `/api/health` | GET | Health check. |
| `/api/feedback/article` | POST | Submits explicit feedback (like, save, etc.). |
| `/api/metrics/engagement` | POST | Tracks implicit engagement (time, scroll). |
| `/api/agents/voice/generate` | POST | On-demand voice generation (if enabled). |

### CLI Usage
For debugging or headless operation (fetching/processing):
```bash
python -m single_pipeline.cli fetch
python -m single_pipeline.cli process --registry single --category general
```

## 3. Debugging & Observability

### Output Artifacts
Check `single_pipeline/output/` for stage results:
- `*_filtered.json`: Items passing the filter.
- `*_scripts.json`: Generated scripts.
- `*_voice.json`: Items with audio paths.
- `*_avatar.json`: Final video objects.

### Failure Modes
The system uses explicit failure statuses (no silent failures):
- **`degraded`**: Item processed but with reduced quality (e.g., fallback TTS).
- **`rejected`**: Item blocked by filter (empty body, bad words).
- **`failed`**: Unrecoverable error (exception during processing).
- **`missing_but_allowed`**: Optional assets missing (e.g., background music).

Refer to `docs/failure_modes.md` for the complete policy.

### Performance
- **Latency**: ~15-20s per batch (3 items) with local TTS.
- **Throughput**: Limited by TTS generation speed.
- **Bottleneck**: `pyttsx3` engine initialization.

---

## LEARNING KIT

### 1. Schema Freeze & API Contracts
**Why Freeze?**
Changing data structures breaks downstream consumers (UI, Mobile App). A frozen schema acts as a "Truth Contract".
- **Best Practice**: Use semantic versioning (v1, v2). Never delete fields in a live version; mark them deprecated instead.
- **Implementation**: We used `orchestration_contract_v1.json` to validate every stage output.

### 2. Error Discipline
**Clear Failure Responses**
- **Anti-Pattern**: Returning `200 OK` with an empty body for errors.
- **Pattern**: Explicit status codes or status fields (`stage_status.voice = "failed"`).
- **Benefit**: The UI knows *exactly* why an item is missing (e.g., "rejected" vs "processing").

### 3. Deployment Confidence
**Production Readiness Checklist**:
1. **Burn-In Testing**: We ran 30 flows to verify stability over time.
2. **Configuration**: Use Environment Variables (`.env`) for secrets, never hardcode.
3. **Observability**: Structured logs (JSON) and explicit metrics files.
