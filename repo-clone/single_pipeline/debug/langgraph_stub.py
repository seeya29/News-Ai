import os
import json
from typing import Any, Dict, List


def _traces_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "traces"))


def _graphs_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "graphs"))


def _read_traces(stage: str, limit: int = 100) -> List[Dict[str, Any]]:
    path = os.path.join(_traces_root(), f"{stage}.jsonl")
    entries: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    entries.append(json.loads(line))
                    if len(entries) >= limit:
                        break
                except Exception:
                    continue
    except Exception:
        pass
    return entries


def build_graph_from_traces() -> Dict[str, Any]:
    """Build a simple pipeline graph JSON from trace logs.

    Nodes are derived from stages observed in traces; edges follow the
    standard pipeline order. This is a lightweight stand-in for LangGraph
    visualization systems.
    """
    os.makedirs(_graphs_root(), exist_ok=True)

    stages = ["ScriptGenAgent", "TTSAgent", "AvatarAgent"]
    traces: Dict[str, List[Dict[str, Any]]] = {s: _read_traces(s) for s in stages}

    nodes = [{"id": s, "label": s, "samples": len(traces.get(s, []))} for s in stages]
    edges = [
        {"from": "ScriptGenAgent", "to": "TTSAgent", "label": "voice"},
        {"from": "TTSAgent", "to": "AvatarAgent", "label": "render"},
    ]
    graph = {"nodes": nodes, "edges": edges}

    out_path = os.path.join(_graphs_root(), "pipeline_graph.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(graph, f, ensure_ascii=False, indent=2)

    return {"status": "ok", "graph_file": out_path, "nodes": len(nodes), "edges": len(edges)}