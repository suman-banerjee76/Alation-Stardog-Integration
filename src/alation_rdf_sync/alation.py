from __future__ import annotations
import httpx, asyncio
from dataclasses import dataclass

@dataclass
class CFVResult:
    """Normalised outcome of a Custom Field Values write, insulating callers from sync-vs-async
    + job-polling differences. `status` is the initial PUT's HTTP status (429 -> caller backs off)."""
    ok: bool
    status: int
    retry_after: float | None = None
    detail: str = ""

def _retry_after_secs(resp):
    v = resp.headers.get("Retry-After")
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None

def _job_id_from(body):
    # The async PUT returns the responsible Jobs API id. Shape varies by version; accept the
    # common keys and a list wrapper.
    if isinstance(body, list):
        body = body[0]
    for k in ("job_id", "id", "job"):
        if isinstance(body, dict) and body.get(k) is not None:
            return body[k]
    raise KeyError(f"no job id in CFV async response: {body!r}")

# Default display names of the four binding fields (design §4.2). Override via
# writeback.field_names when your catalog uses different labels.
BINDING_FIELD_NAMES = {
    "ontology_uri": "Ontology URI",
    "suggested_ontology_uri": "Suggested Ontology URI",
    "suggestion_confidence": "Suggestion Confidence",
    "binding_status": "Binding Status",
}

# Candidate keys for a custom field's display name in the Custom Fields API payload.
# ASSUMPTION (confirm per Alation version): Alation exposes the label as `name_singular`.
_FIELD_NAME_KEYS = ("name_singular", "name", "title", "label")

def _field_display_name(f: dict):
    for k in _FIELD_NAME_KEYS:
        v = f.get(k)
        if v:
            return v
    return None

async def resolve_field_ids(client, field_names: dict, pinned: dict | None = None) -> dict:
    """Resolve logical binding-field -> numeric field id (design §4.1, "resolve once").

    Pinned ids (truthy values in `pinned`) win and need no API call; any remaining logical
    field is looked up by its display name via the Custom Fields API. Raises ValueError if a
    name can't be found (listing what IS available) so a misconfigured name fails loud rather
    than silently writing to field id 0."""
    pinned = {k: v for k, v in (pinned or {}).items() if v}
    need = {k: name for k, name in field_names.items() if k not in pinned}
    out = dict(pinned)
    if not need:
        return out
    resp = await client.list_custom_fields()
    resp.raise_for_status()
    by_name = {}
    for f in resp.json():
        name = _field_display_name(f)
        if name is not None:
            by_name[name] = f["id"]
    missing = []
    for k, name in need.items():
        if name in by_name:
            out[k] = by_name[name]
        else:
            missing.append(name)
    if missing:
        raise ValueError(f"custom fields not found by name: {missing}; "
                         f"available: {sorted(by_name)}")
    return out

class AlationClient:
    """Read client uses the read-only token; write ops require a client built with the write token."""
    def __init__(self, base_url: str, token: str, timeout: float = 120.0, *,
                 cfv_async: bool = True, job_poll_interval: float = 2.0, job_poll_max: int = 30):
        self._c = httpx.AsyncClient(base_url=base_url, headers={"TOKEN": token}, timeout=timeout)
        self._cfv_async = cfv_async              # async endpoint is current; sync deprecated @2024.3.1
        self._job_poll_interval = job_poll_interval
        self._job_poll_max = job_poll_max

    async def get_data_sources(self, skip: int, limit: int = 100):
        return await self._c.get("/integration/v2/datasource/", params={"skip": skip, "limit": limit})

    async def get_tables(self, ds_id: int, skip: int, limit: int = 1500):
        r = await self._c.get("/integration/v2/table/",
            params={"ds_id": ds_id, "skip": skip, "limit": limit, "custom_fields": "true"})
        return r  # caller inspects status (504 -> halve page; 429 -> backoff)

    async def get_documents(self, skip: int, limit: int = 100):
        return await self._c.get("/integration/v2/document/", params={"skip": skip, "limit": limit})

    async def get_data_products(self, skip: int, limit: int = 100):
        return await self._c.get("/data-products/", params={"skip": skip, "limit": limit})

    async def list_custom_fields(self):
        """Custom Fields API: field definitions incl. numeric id + display name. Used by
        resolve_field_ids() to map the four binding fields to ids (design §4.1)."""
        return await self._c.get("/integration/v2/custom_field/")

    async def set_custom_field_values(self, otype: str, oid: int, field_values) -> CFVResult:
        """The single Custom Field Values write site. WRITE-SCOPED TOKEN ONLY.

        `field_values`: iterable of (field_id:int, value) to set on one object in one call.

        Payload shape CONFIRMED against Alation docs ("PUT multiple Custom Field Values"): a JSON
        array of {field_id, otype, oid, value}. Targets the async endpoint by default — the sync
        endpoint is deprecated as of release 2024.3.1; set cfv_async=False for older instances.
        The async PUT only *accepts* the write and returns a Jobs API id, so we poll the job to
        confirm it actually applied (a bare 200 is not success).

        Residual per-instance unknown (still isolated here): the `value` encoding for picker fields
        (Binding Status). A scalar string is the common case; the authoritative per-field-type
        schema is at {instance}/openapi/custom_field_value/."""
        payload = [{"field_id": fid, "otype": otype, "oid": oid, "value": v} for fid, v in field_values]
        path = "/integration/v2/custom_field_value/async/" if self._cfv_async \
            else "/integration/v2/custom_field_value/"
        resp = await self._c.put(path, json=payload)
        if resp.status_code == 429:
            return CFVResult(False, 429, retry_after=_retry_after_secs(resp))
        if not (200 <= resp.status_code < 300):
            return CFVResult(False, resp.status_code, detail=resp.text[:300])
        if not self._cfv_async:
            return CFVResult(True, resp.status_code)          # sync endpoint applies inline
        return await self._confirm_cfv_job(resp)

    async def _confirm_cfv_job(self, resp) -> CFVResult:
        """Poll the Jobs API for the async CFV write until it reaches a terminal state."""
        try:
            job_id = _job_id_from(resp.json())
        except (KeyError, ValueError) as e:
            return CFVResult(False, resp.status_code, detail=str(e))
        for _ in range(self._job_poll_max):
            body = (await self.get_job(job_id)).json()
            rec = body[0] if isinstance(body, list) else body
            status = (rec.get("status") or "").lower()
            if status in ("successful", "succeeded", "success"):
                return CFVResult(True, resp.status_code, detail=f"job {job_id} {status}")
            if status in ("failed", "error", "skipped"):
                return CFVResult(False, resp.status_code, detail=f"job {job_id} {status}: {rec.get('msg')}")
            await asyncio.sleep(self._job_poll_interval)
        return CFVResult(False, resp.status_code, detail=f"job {job_id} not terminal after {self._job_poll_max} polls")

    async def get_job(self, job_id):
        """Jobs API status: GET /api/v1/bulk_metadata/job/?id=<id> -> {status, msg, result}."""
        return await self._c.get("/api/v1/bulk_metadata/job/", params={"id": job_id})

    async def aclose(self): await self._c.aclose()
