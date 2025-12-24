# News AI – Integrated UI & API

A lightweight end-to-end news pipeline with a polished UI, wrapper API endpoints, RL feedback, and a simple handover for quick onboarding.

## Quickstart
- Create venv and install deps:
```
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```
- Set a dev secret and start the server:
```
$env:JWT_SECRET = "your_dev_secret"
python -m uvicorn server.app:APP --port 8000 --reload
```
- Generate a dev token and copy it:
```
python scripts/dev_jwt.py demo_user user --ttl 3600
```
- Open the dashboard: `http://127.0.0.1:8000/ui/` and paste the token.

## UI Overview
- Theme toggle, language selector, and sticky top bar.
- Pipeline stages (Filter → Verify → Script → Voice) with progress states.
- Feed preview cards with badges (lang, audience, tone).
- Feedback actions: Like, Dislike, Approve (save), Flag.

## API Wrappers
- `POST /fetch` — returns `count`, `preview[]`, `files.items`.
- `POST /process` — returns `counts.filtered`, `counts.scripts`, `preview[]`, `files`.
- `POST /voice` — returns `count`, `preview[]`, `files.voice`.
- `POST /feedback` — alias to `/api/feedback/article`.

All endpoints require `Authorization: Bearer <jwt>`.

## End-to-End Test
Run the headless loop test:
```
$env:JWT_SECRET = "your_dev_secret"
python scripts/e2e_loop_test.py
```
It calls `/fetch` → `/process` → `/voice` → `/feedback` and prints a summary.

## Handover Documentation
See `docs/handover.md` for:
- UI Flow Diagram (Mermaid)
- API mapping table
- Integration guide (front + back)
- E2E test procedure
- 1-minute demo script

## Demo Video
Record a short screen capture (UI interactions + narration) and place the link here:
- Demo: <ADD_LINK_HERE>

## Project Structure
- `server/app.py` — FastAPI app + UI mount + wrapper endpoints.
- `server/static/` — UI assets (`index.html`, `styles.css`, `app.js`).
- `single_pipeline/` — core pipeline, agents, outputs, registry, debug tools.
- `scripts/` — dev JWT and e2e loop test.
- `tests/` — unit and integration tests.

## Notes
- Tone/audience classification can run offline via `UNIGURU_PROVIDER=local`.
- Dedup is RAG-lite via token overlap and cache (`output/rag_cache.json`).
- Voice/Avatar are stubbed; real providers can be wired later.