from __future__ import annotations
from abc import ABC, abstractmethod

class Matcher(ABC):
    """All engines write rows to uri_suggestion with identical shape; only scoring differs."""
    @abstractmethod
    async def run(self, pool, cfg, run_id, suggest_sql: str): ...
