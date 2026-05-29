from __future__ import annotations
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from ..matchers import get_matcher
from .. import db

@lru_cache(maxsize=1)
def _suggest_sql() -> str:
    """Load suggest_trgm.sql lazily, tolerating both the source tree (sql/ at repo root) and the
    container (sql/ copied next to the workdir, cwd=/app). The wheel does not vendor sql/, so an
    import-time __file__-relative read would crash once installed — hence call-time + candidates."""
    candidates = [Path(__file__).resolve().parents[3] / "sql" / "suggest_trgm.sql",
                  Path.cwd() / "sql" / "suggest_trgm.sql"]
    for p in candidates:
        if p.exists():
            return p.read_text()
    raise FileNotFoundError("suggest_trgm.sql not found in: " + ", ".join(str(p) for p in candidates))

async def run(cfg, pool, run_id):
    """Stage 3. Score unbound objects -> uri_suggestion; no_match -> gap_candidate.

    The primary `matcher.engine` produces publishable rows. An optional `matcher.shadow_engine`
    runs additionally in the same run for evaluation (e.g. engine=trgm + shadow_engine=agent):
    the agent writes status='shadow' rows so precision@1 can be compared without publishing
    (design §8 row 7). Promotion is operational — make the shadow engine primary once it wins."""
    sql = _suggest_sql()
    started, t0 = datetime.now(timezone.utc), time.monotonic()
    try:
        m = cfg["matcher"]
        await get_matcher(m["engine"], cfg).run(pool, cfg, run_id, suggest_sql=sql)
        shadow = m.get("shadow_engine")
        if shadow and shadow != m["engine"]:
            await get_matcher(shadow, cfg).run(pool, cfg, run_id, suggest_sql=sql)
        await db.record_sync_state(pool, "suggest", 0, run_id, "success",
                                   started_at=started, duration_ms=int((time.monotonic() - t0) * 1000))
    except Exception as e:  # noqa: BLE001 - record failure state, then propagate
        await db.record_sync_state(pool, "suggest", 0, run_id, "failed",
                                   err=str(e), started_at=started, duration_ms=int((time.monotonic() - t0) * 1000))
        raise
