import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from logging_utils import PipelineLogger
from agents.filter_agent import FilterAgent
from agents.script_gen_agent import ScriptGenAgent
from agents.tts_agent_stub import TTSAgentStub
from agents.avatar_agent_stub import AvatarAgentStub


def write_json(path: Path, data: Any, pretty: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        if pretty:
            json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            json.dump(data, f, ensure_ascii=False)


def make_edge_case_items() -> List[Dict[str, Any]]:
    return [
        {"title": "Duplicate Story", "body": "Same content across two entries", "timestamp": 1},
        {"title": "Duplicate Story", "body": "Same content across two entries", "timestamp": 1},
        {"title": "Empty Body", "body": "", "timestamp": 2},
        {"title": "", "body": "Has body but empty title", "timestamp": 3},
        {"title": "Malformed Body", "body": {"unexpected": True}, "timestamp": 4},
        {"title": "Non-string timestamp", "body": "Valid body", "timestamp": "not-a-time"},
        {"title": "Hindi text", "body": "यह एक हिंदी वाक्य है।", "timestamp": 5},
        {"title": "Tamil text", "body": "இது ஒரு தமிழ் வாக்கியம்.", "timestamp": 6},
        {"title": "Bengali text", "body": "এটি একটি বাংলা বাক্য।", "timestamp": 7},
        {"title": "Mixed content", "body": "English mixed हिन्दी বাংলা தமிழ்", "timestamp": 8},
    ]


def sanitize_items(items: List[Dict[str, Any]], logger: PipelineLogger) -> List[Dict[str, Any]]:
    sanitized: List[Dict[str, Any]] = []
    for it in items:
        title = it.get("title")
        body = it.get("body")
        if not isinstance(title, str) or not title.strip():
            title = "Untitled"
        if not isinstance(body, str):
            try:
                body = json.dumps(body, ensure_ascii=False)
            except Exception:
                body = str(body) if body is not None else ""
        sanitized.append({
            "title": title,
            "body": body,
            "timestamp": it.get("timestamp"),
            "raw": it,
        })
    logger.log_event("qa_sanitize", {"count": len(sanitized)})
    return sanitized


def run_qa(out_prefix: str, pretty: bool) -> None:
    logger = PipelineLogger()
    # Build items and sanitize
    items = make_edge_case_items()
    write_json(Path(f"single_pipeline/output/{out_prefix}_items.json"), items, pretty)
    items = sanitize_items(items, logger)

    # Filter
    filter_agent = FilterAgent()
    filtered = filter_agent.filter_items(items, logger=logger)
    write_json(Path(f"single_pipeline/output/{out_prefix}_filtered.json"), filtered, pretty)

    # Script
    script_agent = ScriptGenAgent()
    scripts = script_agent.generate_scripts(filtered, logger=logger)
    write_json(Path(f"single_pipeline/output/{out_prefix}_scripts.json"), scripts, pretty)

    # Voice
    tts_agent = TTSAgentStub()
    voices = tts_agent.synthesize(scripts, logger=logger)
    write_json(Path(f"single_pipeline/output/{out_prefix}_voice.json"), voices, pretty)

    # Avatar
    avatar_agent = AvatarAgentStub()
    avatars = avatar_agent.render(scripts, logger=logger)
    write_json(Path(f"single_pipeline/output/{out_prefix}_avatar.json"), avatars, pretty)

    # QA Report
    duplicate_count = sum(1 for it in filtered if it.get("dedup_flag"))
    empty_body_count = sum(1 for it in filtered if not it.get("body"))
    malformed_count = sum(1 for it in filtered if isinstance(it.get("raw", {}).get("body"), dict))
    lang_counts: Dict[str, int] = {}
    for it in filtered:
        lang = it.get("lang", "unknown")
        lang_counts[lang] = lang_counts.get(lang, 0) + 1
    report = {
        "total_items": len(items),
        "filtered_items": len(filtered),
        "duplicates_flagged": duplicate_count,
        "empty_bodies": empty_body_count,
        "malformed_bodies": malformed_count,
        "language_distribution": lang_counts,
    }
    logger.log_event("qa_report", report)
    write_json(Path(f"single_pipeline/output/{out_prefix}_report.json"), report, pretty)


def main():
    parser = argparse.ArgumentParser(description="QA Harness: edge cases for duplicates/empty/malformed feeds")
    parser.add_argument("--out-prefix", type=str, default="qa", help="Prefix for output files")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON outputs")
    args = parser.parse_args()
    run_qa(out_prefix=args.out_prefix, pretty=args.pretty)


if __name__ == "__main__":
    main()