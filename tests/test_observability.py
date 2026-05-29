"""Phase 8: static checks on the dashboards/alerts SQL and the jitter helper.

KPI/alert SQL can't run without a live PG, but we can verify offline that every KPI/alert
is present and that every table they read exists in the DDL (catches schema drift breaking
a dashboard or alert)."""
from __future__ import annotations
import os, re
from pathlib import Path
import pytest

from alation_rdf_sync import __main__ as cli

ROOT = Path(__file__).resolve().parent.parent
DDL = (ROOT / "migrations" / "001_init.sql").read_text()
KPIS = (ROOT / "sql" / "kpis.sql").read_text()
ALERTS = (ROOT / "sql" / "alerts.sql").read_text()

DDL_TABLES = set(re.findall(r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(\w+)", DDL))


def _from_join_targets(sql: str) -> set[str]:
    body = re.sub(r"--[^\n]*", "", sql)                  # strip comments
    ctes = set(re.findall(r"(\w+)\s+AS\s*\(", body, re.I))   # WITH x AS ( ... )
    targets = set(re.findall(r"\b(?:FROM|JOIN)\s+(\w+)", body, re.I))
    return targets - ctes


def test_all_kpis_present():
    for kpi in ("binding coverage (tables)", "binding coverage (data products)",
                "suggestion coverage", "acceptance rate", "precision@1",
                "write-back success rate", "conflicts logged", "no-match (gap) rate",
                "per-stream sync duration", "freshness lag"):
        assert f"KPI: {kpi}" in KPIS, f"missing KPI: {kpi}"

def test_all_alerts_present():
    for a in ("stage failure", "freshness lag exceeded", "concept extract stale",
              "write-back failures", "write-back conflicts", "CFV API errors"):
        assert f"ALERT: {a}" in ALERTS, f"missing ALERT: {a}"

def test_kpi_tables_exist_in_ddl():
    for t in _from_join_targets(KPIS):
        assert t in DDL_TABLES, f"kpis.sql references unknown table {t}"

def test_alert_tables_exist_in_ddl():
    for t in _from_join_targets(ALERTS):
        assert t in DDL_TABLES, f"alerts.sql references unknown table {t}"


# ---- jitter helper ---------------------------------------------------------

async def test_jitter_disabled_by_env(monkeypatch):
    monkeypatch.setenv("ALATION_RDF_SYNC_NO_JITTER", "1")
    slept = []
    async def fake_sleep(s): slept.append(s)
    monkeypatch.setattr(cli.asyncio, "sleep", fake_sleep)
    class Cfg: raw = {"schedule": {"jitter_seconds": 120}}
    await cli._maybe_jitter(Cfg())
    assert slept == []

async def test_jitter_sleeps_within_window(monkeypatch):
    monkeypatch.delenv("ALATION_RDF_SYNC_NO_JITTER", raising=False)
    slept = []
    async def fake_sleep(s): slept.append(s)
    monkeypatch.setattr(cli.asyncio, "sleep", fake_sleep)
    class Cfg: raw = {"schedule": {"jitter_seconds": 120}}
    await cli._maybe_jitter(Cfg())
    assert len(slept) == 1 and 0 <= slept[0] <= 120

async def test_jitter_noop_when_zero(monkeypatch):
    slept = []
    async def fake_sleep(s): slept.append(s)
    monkeypatch.setattr(cli.asyncio, "sleep", fake_sleep)
    class Cfg: raw = {"schedule": {"jitter_seconds": 0}}
    await cli._maybe_jitter(Cfg())
    assert slept == []
