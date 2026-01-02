# Orchestration Field Notes

This document provides a definitive reference for all fields used in the pipeline's JSON outputs. It serves as a companion to `orchestration_contract_v1.json` to remove ambiguity.

## Core Fields (Shared Across Stages)

- **`title`**: The headline of the news item. *Note: Must be cleaned of source prefixes (e.g., "BBC News - ") before ScriptGen.*
- **`body`**: The main content text. *Note: Should be plain text, no HTML.*
- **`lang`**: ISO 639-1 language code (e.g., `en`, `hi`, `ta`). *Note: 'mixed' is used if no single language dominates > 30%.*
- **`category`**: The topic classification (e.g., `technology`, `politics`, `health`). *Note: normalized to lowercase.*
- **`timestamp`**: ISO 8601 formatted string (`YYYY-MM-DDTHH:MM:SSZ`). *Note: critical for deduplication ordering.*

## Filter Agent Outputs (`*_filtered.json`)

- **`tone`**: The detected emotional tone (e.g., `neutral`, `formal`, `casual`). *Note: Used to guide ScriptGen style selection.*
- **`audience`**: Target demographic (e.g., `general`, `kids`, `youth`). *Note: Defaults to 'general'.*
- **`dedup_flag`**: Boolean (`true`/`false`). Indicates if this item is a duplicate of a recently processed item. *Note: Downstream agents should skip items where `dedup_flag` is true.*
- **`dedup_key`**: A unique string signature (e.g., `title::timestamp`) used for fast equality checks.
- **`raw`**: The original source payload. *Note: Kept for debugging, do not rely on schema stability here.*

## ScriptGen Agent Outputs (`*_scripts.json`)

- **`variants`**: A dictionary containing multiple script versions.
  - **`bullets`**: List of 3-5 summary points. *Note: Good for visual slides.*
  - **`headline`**: A short (<140 char) ticker-style summary.
  - **`narration`**: A conversational script for standard reading.
  - **`styles`**: Nested dictionary with specific persona scripts.
    - **`formal`**: Professional news anchor style.
    - **`kids`**: Simplified vocabulary, enthusiastic tone.
    - **`youth`**: Fast-paced, slang-aware (e.g., "Quick take").
    - **`devotional`**: Slow-paced, respectful tone.
- **`metadata`**: Processing details.
  - **`generated_at`**: Timestamp of script generation.
  - **`category`**: Category used for generation context.

## Known Issues & Resolutions (Audit Log)

- **RAG Cache Corruption**: 
  - *Issue*: `rag_cache.json` could become corrupted (empty or invalid JSON) causing pipeline crash.
  - *Fix*: Added empty check and backup logic in `RAGClient._load()`.
- **ScriptGen Style Missing**:
  - *Issue*: `ScriptGenAgent` in `repo-clone` lacked `styles` field required by contract.
  - *Fix*: Backported `_style_variant` logic and updated `generate()` method.
- **Language Detection**:
  - *Note*: Heuristic-based detection is robust for long text but may default to `mixed` for short snippets.
