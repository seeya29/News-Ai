from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from logging_utils import PipelineLogger


class BaseFetcher(ABC):
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg

    @abstractmethod
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        raise NotImplementedError