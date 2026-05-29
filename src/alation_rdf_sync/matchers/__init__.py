from .trgm import TrgmMatcher
from .agent import AgentMatcher

def get_matcher(engine: str, cfg):
    if engine == "trgm":      return TrgmMatcher()
    if engine == "agent":     return AgentMatcher(cfg["matcher"]["agent"])
    if engine == "embedding": raise NotImplementedError("optional pgvector engine; not required for v1.0")
    raise ValueError(f"unknown matcher.engine: {engine}")
