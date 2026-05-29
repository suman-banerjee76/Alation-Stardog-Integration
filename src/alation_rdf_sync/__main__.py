from __future__ import annotations
import asyncio, os, random, sys
from .config import Config
from .db import create_pool
from . import db
from .ids import uuid7
from .stages import sync, extract_concepts, suggest, writeback

STAGES = {"sync": sync.run, "extract_concepts": extract_concepts.run,
          "suggest": suggest.run, "writeback": writeback.run}

# One-off operator commands (not stages). Each prints to stdout and exits.
COMMANDS = {"resolve-fields", "resolve-hub", "render-sms2"}

async def main(only: str | None = None):
    cfg = Config.load()
    if only in COMMANDS:
        await _command(only, cfg)
        return
    if only is None:
        await _maybe_jitter(cfg)          # spread overlapping CronJob starts (schedule.jitter_seconds)
    run_id = uuid7()  # monotonic run ordering for reconcile
    pool = await create_pool(cfg.pg_dsn)
    order = [only] if only else ["sync", "extract_concepts", "suggest", "writeback"]
    for name in order:
        await STAGES[name](cfg, pool, run_id)

async def _command(name, cfg):
    if name == "resolve-fields":
        await _resolve_fields_cmd(cfg)
    elif name == "resolve-hub":
        await _resolve_hub_cmd(cfg)
    elif name == "render-sms2":
        _render_sms2_cmd(cfg)

async def _maybe_jitter(cfg):
    if os.environ.get("ALATION_RDF_SYNC_NO_JITTER"):
        return
    jitter = (cfg.raw.get("schedule") or {}).get("jitter_seconds", 0)
    if jitter and jitter > 0:
        await asyncio.sleep(random.uniform(0, jitter))

async def _resolve_fields_cmd(cfg):
    """Discover the four binding-field ids by name (read token) and print YAML to pin in config."""
    from .alation import AlationClient, resolve_field_ids, BINDING_FIELD_NAMES
    names = (cfg["writeback"].get("field_names") if "writeback" in cfg.raw else None) or BINDING_FIELD_NAMES
    al = AlationClient(cfg["alation"]["base_url"], cfg.read_token)
    try:
        ids = await resolve_field_ids(al, names, pinned=None)   # force lookup of all four
    finally:
        await al.aclose()
    print("# Resolved binding-field ids — paste under writeback: in config.yaml")
    print("writeback:")
    print("  field_ids:")
    for k in BINDING_FIELD_NAMES:
        print(f"    {k}: {ids[k]}")

async def _resolve_hub_cmd(cfg):
    """List document hubs from the landing zone so the operator can identify the Glossary Hub id.
    Requires `sync` to have populated alation_document first."""
    pool = await create_pool(cfg.pg_dsn)
    try:
        hubs = await db.list_document_hubs(pool)
    finally:
        await pool.close()
    if not hubs:
        print("no documents synced yet — run `sync` first", file=sys.stderr)
        return
    print(f"{'document_hub_id':>16}  {'documents':>9}  sample_titles")
    for h in hubs:
        print(f"{h['document_hub_id']:>16}  {h['documents']:>9}  {list(h['sample_titles'])}")
    print("\n# set stardog.glossary_document_hub_id in config.yaml to the Glossary Hub id above")

def _render_sms2_cmd(cfg):
    """Render vg_alation.sms2 with the configured Glossary Hub id; print to stdout for `virtual add`."""
    from .stardog import render_sms2
    hub = (cfg.raw.get("stardog") or {}).get("glossary_document_hub_id")
    if not hub:
        print("ERROR: set stardog.glossary_document_hub_id (run `resolve-hub` to find it)", file=sys.stderr)
        sys.exit(1)
    sys.stdout.write(render_sms2(hub))

if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else None))
