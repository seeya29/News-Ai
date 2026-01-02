# Failure Modes & UI Behavior

This document outlines how the pipeline handles failures and what the frontend (Chandragupta) should display in each scenario.

## 1. Fetcher Failures
*   **Scenario**: RSS feed is down, API rate limit reached, or credentials missing.
*   **Pipeline Behavior**: `FetcherHub` logs a warning and skips the source. If all sources fail, the pipeline exits early.
*   **Output**: `single_filtered.json` may be empty or missing items from specific sources.
*   **UI Behavior**:
    *   **Global**: Show "No new updates" if list is empty.
    *   **Specific Source**: Not visible to end-user (items just don't appear). Admin dashboard sees warning logs.

## 2. Filter/Deduplication Failures
*   **Scenario**: RAG service unavailable or semantic deduplication fails.
*   **Pipeline Behavior**: `FilterAgent` falls back to simple title-based deduplication.
*   **Output**: Duplicate items might appear if titles differ slightly but content is same.
*   **UI Behavior**: User sees multiple similar stories. UI handles them as distinct items.

## 3. Script Generation Failures
*   **Scenario**: LLM refusal, template error, or empty body text.
*   **Pipeline Behavior**: Item is marked as `failed` in the script stage. It is **excluded** from downstream Voice/Avatar stages.
*   **Output**: Item missing from `*_scripts.json` or marked failed.
*   **UI Behavior**: Item does not appear in the video feed.

## 4. TTS (Voice) Failures
*   **Scenario**: TTS provider error (e.g., Azure quota, pyttsx3 deadlock), generation timeout.
*   **Pipeline Behavior**:
    *   `TTSAgentStub` catches exception.
    *   Sets `status: "failed"`.
    *   Sets `audio_path: null`.
    *   **Crucial**: `BucketOrchestrator` filters these items out before the Avatar stage.
*   **Output**: Item present in `*_voice.json` with `status: "failed"`, but **absent** in `*_avatar.json`.
*   **UI Behavior**: Item does not appear in the video feed. (Prevents silent videos).

## 5. Avatar Rendering Failures
*   **Scenario**: Renderer crash, missing audio file, filesystem write error.
*   **Pipeline Behavior**:
    *   `AvatarAgentStub` catches exception.
    *   Sets `status: "failed"`.
    *   Sets `video_url: null`.
*   **Output**: Item present in `*_avatar.json` with `status: "failed"`.
*   **UI Behavior**:
    *   Frontend filters list for `status == "success"`.
    *   Failed items are hidden.
    *   **Fallback**: If UI receives a failed item, it should display a "Content Unavailable" placeholder or skip to the next item.

## Summary Table

| Stage | Failure Outcome | Downstream Effect | UI Action |
| :--- | :--- | :--- | :--- |
| **Fetch** | Missing items | Fewer items to process | Show what's available |
| **Filter** | Duplicates / Bad tags | Low quality metadata | Display as-is |
| **Script** | Generation Error | Dropped from pipeline | Item Hidden |
| **Voice** | Audio Gen Error | Dropped from pipeline | Item Hidden |
| **Avatar** | Render Error | `status: "failed"` | **Hide Item** / Skip |

**Frontend Rule**: ALWAYS check `status === "success"` and `video_url !== null` before attempting playback.
