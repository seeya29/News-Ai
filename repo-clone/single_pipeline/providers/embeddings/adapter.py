import os
from typing import List, Optional

try:
    from sentence_transformers import SentenceTransformer, util as st_util
except Exception:
    SentenceTransformer = None
    st_util = None


class EmbeddingLocalAdapter:
    """Thin wrapper around sentence-transformers for local embeddings.

    Reads model choice from EMBED_MODEL_NAME (default: all-MiniLM-L6-v2).
    Provides embed(text) -> List[float] and cosine(vec1, vec2) -> float.
    Lazily loads the model on first use.
    """

    def __init__(self, model_name: Optional[str] = None):
        self.model_name = (
            model_name
            or os.getenv("EMBED_MODEL_NAME")
            or "all-MiniLM-L6-v2"
        )
        # Allow either bare name or full repo path
        if self.model_name.lower() in {"minilm", "mini", "all-minilm-l6-v2"}:
            self.model_name = "all-MiniLM-L6-v2"
        self._model: Optional[SentenceTransformer] = None

    def _ensure_model(self) -> None:
        if self._model is not None:
            return
        if SentenceTransformer is None:
            raise RuntimeError("sentence-transformers not installed. Run: pip install sentence-transformers")
        # Accept both HF hub path and short name
        name = self.model_name
        if not ("/" in name or name.startswith("sentence-transformers/")):
            # Map short names to canonical hub path
            short_map = {
                "all-MiniLM-L6-v2": "sentence-transformers/all-MiniLM-L6-v2",
                "all-mpnet-base-v2": "sentence-transformers/all-mpnet-base-v2",
            }
            name = short_map.get(name, f"sentence-transformers/{name}")
        self._model = SentenceTransformer(name)

    def embed(self, text: str) -> List[float]:
        self._ensure_model()
        vec = self._model.encode(text or "", convert_to_tensor=False, normalize_embeddings=True)
        # Ensure plain python list of floats for JSON/storage safety
        try:
            return vec.tolist() if hasattr(vec, "tolist") else list(vec)
        except Exception:
            return [float(x) for x in vec]

    def cosine(self, a: List[float], b: List[float]) -> float:
        if st_util is None:
            # Fallback simple manual cosine
            import math
            if not a or not b or len(a) != len(b):
                return 0.0
            dot = sum(x * y for x, y in zip(a, b))
            na = math.sqrt(sum(x * x for x in a))
            nb = math.sqrt(sum(y * y for y in b))
            return (dot / (na * nb)) if na > 0 and nb > 0 else 0.0
        # sentence-transformers util expects tensors, but also works with lists
        try:
            import torch
            ta = torch.tensor(a)
            tb = torch.tensor(b)
            s = st_util.cos_sim(ta, tb)
            return float(s.item())
        except Exception:
            # Fallback manual cosine
            import math
            if not a or not b or len(a) != len(b):
                return 0.0
            dot = sum(x * y for x, y in zip(a, b))
            na = math.sqrt(sum(x * x for x in a))
            nb = math.sqrt(sum(y * y for y in b))
            return (dot / (na * nb)) if na > 0 and nb > 0 else 0.0