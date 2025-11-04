from typing import Any, Dict, List, Optional

from logging_utils import PipelineLogger


class AvatarAgentStub:
    def __init__(self):
        pass

    def render(self, items: List[Dict[str, Any]], logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for it in items:
            tone = (it.get("tone") or "neutral").lower()
            lang = it.get("lang", "en")
            audience = it.get("audience", "general")
            persona = f"Avatar_{lang}_{tone}_{audience}"
            script = it.get("script", "")
            results.append({
                "title": it.get("title", "Untitled"),
                "persona": persona,
                "script": script,
                "lang": lang,
                "tone": tone,
                "audience": audience,
                "timestamp": it.get("timestamp"),
                "raw": it
            })
        if logger:
            logger.log_event("avatar", {"count": len(results)})
        return results