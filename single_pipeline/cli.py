import argparse
import json
from pathlib import Path
from typing import List, Optional

from fetcher_hub import FetcherHub
from agents.filter_agent import FilterAgent
from agents.script_gen_agent import ScriptGenAgent
from agents.tts_agent_stub import TTSAgentStub
from agents.avatar_agent_stub import AvatarAgentStub
from logging_utils import PipelineLogger

DEFAULT_OUTPUT_DIR = Path(__file__).parent / "output"
DEFAULT_REGISTRY = Path(__file__).parent / "feed_registry.json"


def parse_sources(sources: Optional[str]) -> Optional[List[str]]:
    if sources is None:
        return None
    return [s.strip() for s in sources.split(",") if s.strip()]


def run(
    stage: str,
    sources: Optional[str] = None,
    registry: Optional[str] = None,
    limit: int = 10,
    out_prefix: str = "demo",
    pretty: bool = False,
):
    output_dir = DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    registry_path = Path(registry) if registry else DEFAULT_REGISTRY
    hub = FetcherHub(registry_path)

    logger = PipelineLogger()
    src_list = parse_sources(sources)

    if stage == "fetch":
        items = hub.fetch(src_list, limit=limit, logger=logger)
        out_file = output_dir / f"{out_prefix}_items.json"
        with out_file.open("w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2 if pretty else None)
        return str(out_file)

    # common: read inputs with chaining by stage
    items_file = output_dir / f"{out_prefix}_items.json"
    filtered_file = output_dir / f"{out_prefix}_filtered.json"
    scripts_file = output_dir / f"{out_prefix}_scripts.json"
    voice_file = output_dir / f"{out_prefix}_voice.json"

    if stage == "filter":
        # filter always reads fetched items
        input_file = items_file
    elif stage == "script":
        # prefer filtered if present, else fetched
        input_file = filtered_file if filtered_file.exists() else items_file
    elif stage == "voice":
        # prefer scripts if present, else filtered, else fetched
        input_file = scripts_file if scripts_file.exists() else (
            filtered_file if filtered_file.exists() else items_file
        )
    elif stage == "avatar":
        # prefer voice if present, else scripts, else filtered, else fetched
        input_file = voice_file if voice_file.exists() else (
            scripts_file if scripts_file.exists() else (
                filtered_file if filtered_file.exists() else items_file
            )
        )
    else:
        input_file = items_file

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found for stage '{stage}': {input_file}")
    with input_file.open("r", encoding="utf-8") as f:
        fetched_items = json.load(f)

    if stage == "filter":
        agent = FilterAgent()
        filtered = agent.filter_items(fetched_items, logger=logger)
        out_file = output_dir / f"{out_prefix}_filtered.json"
        with out_file.open("w", encoding="utf-8") as f:
            json.dump(filtered, f, ensure_ascii=False, indent=2 if pretty else None)
        return str(out_file)

    if stage == "script":
        agent = ScriptGenAgent()
        scripted = agent.generate_scripts(fetched_items, logger=logger)
        out_file = output_dir / f"{out_prefix}_scripts.json"
        with out_file.open("w", encoding="utf-8") as f:
            json.dump(scripted, f, ensure_ascii=False, indent=2 if pretty else None)
        return str(out_file)

    if stage == "voice":
        agent = TTSAgentStub()
        voiced = agent.synthesize(fetched_items, logger=logger)
        out_file = output_dir / f"{out_prefix}_voice.json"
        with out_file.open("w", encoding="utf-8") as f:
            json.dump(voiced, f, ensure_ascii=False, indent=2 if pretty else None)
        return str(out_file)

    if stage == "avatar":
        agent = AvatarAgentStub()
        avatars = agent.render(fetched_items, logger=logger)
        out_file = output_dir / f"{out_prefix}_avatar.json"
        with out_file.open("w", encoding="utf-8") as f:
            json.dump(avatars, f, ensure_ascii=False, indent=2 if pretty else None)
        return str(out_file)

    raise ValueError(f"Unknown stage: {stage}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified content pipeline")
    parser.add_argument("stage", choices=["fetch", "filter", "script", "voice", "avatar"], help="Pipeline stage to run")
    parser.add_argument("--sources", type=str, default=None, help="Comma-separated connector keys to run")
    parser.add_argument("--registry", type=str, default=None, help="Path to feed_registry.json")
    parser.add_argument("--limit", type=int, default=10, help="Max items per connector")
    parser.add_argument("--out-prefix", type=str, default="demo", help="Output file prefix")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON outputs")

    args = parser.parse_args()
    path = run(
        stage=args.stage,
        sources=args.sources,
        registry=args.registry,
        limit=args.limit,
        out_prefix=args.out_prefix,
        pretty=args.pretty,
    )
    print(f"Wrote: {path}")