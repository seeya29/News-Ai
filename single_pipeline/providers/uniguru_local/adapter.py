from typing import Any, Dict, Optional


class UniguruLocalAdapter:
    """Lightweight local adapter that mimics UniguruClient tagging.

    Provides `tag_text(title, body, language)` and returns a dict with
    `category`, `tone`, and `audience` keys. This is heuristic-based and
    deterministic, intended for offline/dev environments.
    """

    def __init__(self):
        pass

    def _classify_category(self, title: str, body: str) -> Optional[str]:
        t = (title or "") + " " + (body or "")
        tl = t.lower()
        if any(k in tl for k in ["stock", "market", "earnings", "crypto", "finance"]):
            return "finance"
        if any(k in tl for k in ["ai", "software", "hardware", "startup", "technology", "tech"]):
            return "tech"
        if any(k in tl for k in ["research", "study", "scientists", "space", "biology", "science"]):
            return "science"
        return "general"

    def _classify_tone(self, title: str, body: str) -> Optional[str]:
        tl = ((title or "") + " " + (body or "")).lower()
        if any(k in tl for k in ["breaking", "urgent", "official", "announced"]):
            return "formal"
        if any(k in tl for k in ["tips", "guide", "how to", "explained"]):
            return "neutral"
        if any(k in tl for k in ["meme", "lol", "funny", "trend"]):
            return "casual"
        return "neutral"

    def _classify_audience(self, language: Optional[str], title: str, body: str) -> str:
        lang = (language or "en").lower()
        tl = ((title or "") + " " + (body or "")).lower()
        # Simple audience routing by keywords and language
        if lang in ("hi", "bn", "ta"):
            if any(k in tl for k in ["school", "kids", "young", "students"]):
                return "kids"
            if any(k in tl for k in ["youth", "college", "campus", "festival"]):
                return "youth"
            return "general"
        else:
            if any(k in tl for k in ["kids", "children", "toy", "cartoon"]):
                return "kids"
            if any(k in tl for k in ["youth", "college", "sports", "gaming"]):
                return "youth"
            return "general"

    def tag_text(self, title: str, body: str, language: Optional[str] = None) -> Dict[str, Any]:
        """Return classification tags for given text.

        Response keys:
        - category: one of general|finance|tech|science
        - tone: one of neutral|formal|casual
        - audience: one of general|kids|youth
        """
        category = self._classify_category(title, body)
        tone = self._classify_tone(title, body)
        audience = self._classify_audience(language, title, body)
        return {"category": category, "tone": tone, "audience": audience}