# Orchestration Handover

## Overview
The **BucketOrchestrator** is the central nervous system of the News-Ai pipeline. It manages the lifecycle of news items from raw ingestion to final multimedia artifacts (scripts, audio, avatar videos). It enforces a strict, deterministic flow where items are grouped into "buckets" (e.g., `EN-NEWS`, `HI-YOUTH`) based on language and tone.

## Core Responsibilities
1.  **Ingestion & Routing**: Accepts raw items from `FetcherHub`, filters duplicates via `FilterAgent`, and routes them to specific buckets defined in `orchestration_contract_v1.json`.
2.  **Stage Management**: execute stages sequentially: `Summarize` -> `Voice` -> `Avatar`.
3.  **Error Handling**: Catches exceptions at each stage, logs them to `StageLogger` and `TraceLogger`, and ensures failures in one item do not crash the entire bucket (unless critical).
4.  **Artifact Management**: Ensures all generated files (scripts, WAVs, MP4s) are saved to deterministic paths and correctly referenced in output JSONs.

## Guarantees
*   **Deterministic Output**: For a given input item (Title + Body), the pipeline generates the same unique ID and file paths. Re-running the pipeline on the same data yields consistent results.
*   **Strict Schema Adherence**: All output JSONs (`*_scripts.json`, `*_voice.json`, `*_avatar.json`) strictly follow `orchestration_contract_v1.json`.
*   **Explicit Status**: Every item has a `status` field (`success` or `failed`). There are no "partial successes" or ambiguous states.
*   **Atomic file references**: `audio_path` and `video_url` are either valid strings pointing to existing files (on success) or explicitly `null` (on failure).

## What It Will NEVER Do
*   **Never Block Indefinitely**: All external calls (TTS, rendering) are wrapped in retries and timeouts (where applicable) or fail fast.
*   **Never Return "Maybe"**: An item is either fully processed or marked as failed.
*   **Never Modify Source Data**: Raw input data is preserved; enhancements are additive.
*   **Never Guess File Types**: Mime types and formats are explicitly defined in metadata.

## Integration Points
*   **Input**: `single_filtered.json` (List of validated news items).
*   **Output**: Bucket-specific JSONs in `single_pipeline/output/` (e.g., `single_EN-NEWS_avatar.json`).
*   **Frontend (Chandragupta)**: Should consume the `avatar` output JSONs. The `video_url` field is the primary artifact for display.
