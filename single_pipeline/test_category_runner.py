import argparse
import json
from pathlib import Path
from typing import List, Dict, Any

from fetcher_hub import FetcherHub
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


def run_pipeline_for_categories(
    sources: List[str],
    registry_path: Path,
    limit: int,
    out_prefix: str,
    pretty: bool,
) -> None:
    logger = PipelineLogger()
    hub = FetcherHub(registry_path=registry_path)

    # Fetch
    logger.log_event("fetch", {"sources": sources, "limit": limit})
    items = hub.fetch(sources=sources, limit=limit, logger=logger)
    write_json(Path(f"single_pipeline/output/{out_prefix}_items.json"), items, pretty)

    # Filter
    logger.log_event("filter", {"count": len(items)})
    filter_agent = FilterAgent()
    filtered = filter_agent.filter_items(items, logger=logger)
    write_json(Path(f"single_pipeline/output/{out_prefix}_filtered.json"), filtered, pretty)

    # Group by category
    cat_map: Dict[str, List[Dict[str, Any]]] = {}
    for it in filtered:
        cat = it.get("category") or "general"
        cat_map.setdefault(cat, []).append(it)

    # Process up to 3 items per category
    script_agent = ScriptGenAgent()
    tts_agent = TTSAgentStub()
    avatar_agent = AvatarAgentStub()

    for cat, items_list in cat_map.items():
        selected = items_list[:3]
        logger.log_event("script", {"category": cat, "count": len(selected)})
        scripts = script_agent.generate_scripts(selected, logger=logger)
        write_json(Path(f"single_pipeline/output/{out_prefix}_{cat}_scripts.json"), scripts, pretty)

        logger.log_event("voice", {"category": cat, "count": len(scripts)})
        voices = tts_agent.synthesize(scripts, logger=logger)
        write_json(Path(f"single_pipeline/output/{out_prefix}_{cat}_voice.json"), voices, pretty)

        # Avatar agent consumes tone/audience/lang and script; use scripts for clarity
        logger.log_event("avatar", {"category": cat, "count": len(scripts)})
        avatars = avatar_agent.render(scripts, logger=logger)
        write_json(Path(f"single_pipeline/output/{out_prefix}_{cat}_avatar.json"), avatars, pretty)

    # Summary
    summary = {k: len(v[:3]) for k, v in cat_map.items()}
    logger.log_event("summary", summary)
    write_json(Path(f"single_pipeline/output/{out_prefix}_summary.json"), summary, pretty)


def main():
    parser = argparse.ArgumentParser(description="Run 3 stories per category through the pipeline")
    parser.add_argument(
        "--sources",
        type=str,
        default="gurukul_stub,stock_agent_stub,wellness_bot_stub,used_car_stub,telegram_stub,youtube_rss,x_nitter,domain_api",
        help="Comma-separated source keys from the registry",
    )
    parser.add_argument(
        "--registry",
        type=str,
        default="single_pipeline/feed_registry.yaml",
        help="Path to feed registry YAML/JSON",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max items per source to fetch",
    )
    parser.add_argument(
        "--out-prefix",
        type=str,
        default="category_test",
        help="Prefix for output files",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON outputs",
    )

    args = parser.parse_args()
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    run_pipeline_for_categories(
        sources=sources,
        registry_path=Path(args.registry),
        limit=args.limit,
        out_prefix=args.out_prefix,
        pretty=args.pretty,
    )


if __name__ == "__main__":
    main()