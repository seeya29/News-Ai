# Failure Modes & Determinism Policy

## Core Philosophy
1. **No Silent Fallbacks**: Errors must be explicit. If a component fails to deliver the "ideal" result, it must mark the item status accordingly.
2. **Determinism**: The pipeline should produce predictable results. Random fallbacks without audit trails are forbidden.
3. **Explicit Statuses**: Every stage must report a clear status.

## Status Definitions

The `stage_status` object in the contract tracks the health of each item through the pipeline.

| Status | Meaning | Action |
| :--- | :--- | :--- |
| `pending` | Processing not yet started. | Wait. |
| `success` | Operation completed successfully with optimal quality. | Proceed. |
| `degraded` | Operation completed, but with reduced quality (e.g., fallback provider, stub assets). | Proceed with caution. |
| `missing_but_allowed` | Asset (e.g., audio) was not generated, but this is acceptable by policy. | Skip dependent stages if needed. |
| `rejected` | Item failed validation (e.g., corrupted data, missing required fields). | Stop processing. |
| `failed` | System error or unrecoverable failure. | Retry or Dead Letter Queue. |

## Scenario Mappings

### 1. Fetch / Ingestion
- **Scenario**: RSS feed returns 5xx error or timeout.
- **Policy**: 
  - If partial items fetched: Mark batch as `degraded`.
  - If total failure: Mark run as `failed`.

### 2. Filter / Validation
- **Scenario**: Item missing title or body (corrupted).
- **Policy**: Mark as `rejected`. Do not attempt to "fix" empty bodies with placeholder text unless explicitly configured for testing.

### 3. Text-to-Speech (TTS)
- **Scenario**: Primary provider (e.g., Azure/D-ID) fails (API error, quota exceeded).
- **Policy**: 
  - **Do NOT** silently swap to a local stub without changing status.
  - If fallback is enabled: Generate stub audio and set `stage_status.voice = "degraded"`.
  - If fallback is disabled: Set `stage_status.voice = "failed"`.
- **Scenario**: Audio generation skipped (e.g., text-only item).
- **Policy**: Set `stage_status.voice = "missing_but_allowed"`.

### 4. Avatar / Video
- **Scenario**: Rendering service fails.
- **Policy**: Set `stage_status.avatar = "failed"`.
- **Scenario**: No audio available (but allowed).
- **Policy**: Set `stage_status.avatar = "skipped"` (or `missing_but_allowed` if consistent).

## Implementation Guidelines
- **Contract**: `orchestration_contract_v1.json` enums must include `degraded`, `rejected`, `missing_but_allowed`.
- **Logs**: Structured logs must accompany any non-success status OR fallback logic (e.g., `event="tts_fallback"`, `status="degraded"`, `event="unknown_tone_fallback"`).
- **No Silent Failures**: Even if a safe default is used (e.g. "neutral" tone), it must be logged as a warning.
