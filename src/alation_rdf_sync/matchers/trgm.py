from __future__ import annotations
import re
from .base import Matcher

_COMMENT = re.compile(r"--[^\n]*")
_NAMED = re.compile(r":(\w+)")

def translate_named(sql: str, params: dict):
    """Translate :named placeholders to asyncpg $1.. positionals without touching the
    query logic (design/prompt: don't rewrite suggest_trgm.sql). Each distinct name maps
    to one positional, reused on repeat; returns (sql, args-in-positional-order).
    SQL `--` comments are stripped first so a comment's param list can't skew ordering."""
    sql = _COMMENT.sub("", sql)
    order: list[str] = []
    def repl(m):
        name = m.group(1)
        if name not in order:
            order.append(name)
        return f"${order.index(name) + 1}"
    out = _NAMED.sub(repl, sql)
    return out, [params[n] for n in order]

class TrgmMatcher(Matcher):
    async def run(self, pool, cfg, run_id, suggest_sql: str):
        m = cfg["matcher"]
        params = {"run_id": run_id, "top_n": m["top_n"],
                  "min_similarity": m["min_similarity"], "min_label_length": m["min_label_length"]}
        sql, args = translate_named(suggest_sql, params)
        async with pool.acquire() as c:
            await c.execute(sql, *args)
