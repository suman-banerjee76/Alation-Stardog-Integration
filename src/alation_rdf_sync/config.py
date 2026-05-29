from __future__ import annotations
import os, yaml, httpx
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit, quote

class VaultError(RuntimeError):
    pass

class SecretResolver:
    """Resolve `vault://<mount>/<path>/<field>` secret refs.

    Production: read from HashiCorp Vault using the standard `VAULT_ADDR` + `VAULT_TOKEN`
    (and optional `VAULT_NAMESPACE` for Enterprise). KV v2 by default — `vault://kv/alation/read-token`
    reads `GET {addr}/v1/kv/data/alation` and returns field `read-token` from `data.data`; set
    `VAULT_KV_VERSION=1` for KV v1. Local-dev fallback: when `VAULT_ADDR`/`VAULT_TOKEN` are absent,
    read an env var `VAULT_<PATH>` (path upper-cased, `/`->`_`). Non-`vault://` refs are returned
    literally. Resolved values are cached; the HTTP client is created lazily and reused.
    """
    def __init__(self, *, addr=None, token=None, namespace=None, kv_version=None, client=None):
        self._addr = addr if addr is not None else os.environ.get("VAULT_ADDR")
        self._token = token if token is not None else os.environ.get("VAULT_TOKEN")
        self._namespace = namespace if namespace is not None else os.environ.get("VAULT_NAMESPACE")
        self._kv_version = int(kv_version if kv_version is not None
                               else os.environ.get("VAULT_KV_VERSION", "2"))
        self._client = client                  # injectable httpx-like client (tests)
        self._cache: dict[str, str] = {}

    @property
    def vault_enabled(self) -> bool:
        return bool(self._addr and self._token)

    def resolve(self, ref: str) -> str:
        if not ref or not ref.startswith("vault://"):
            return ref                          # literal value, not a secret ref
        if ref in self._cache:
            return self._cache[ref]
        path = ref.removeprefix("vault://")
        val = self._read_vault(path) if self.vault_enabled else self._read_env(path)
        if not val:
            how = "Vault" if self.vault_enabled else "env (set VAULT_ADDR+VAULT_TOKEN or the VAULT_* var)"
            raise VaultError(f"could not resolve secret {ref!r} via {how}")
        self._cache[ref] = val
        return val

    def _read_env(self, path: str):
        return os.environ.get("VAULT_" + path.replace("/", "_").upper())

    def _read_vault(self, path: str):
        parts = [p for p in path.split("/") if p]
        if len(parts) < 3:
            raise VaultError(f"vault ref must be vault://<mount>/<path>/<field>: {path!r}")
        mount, field_name, secret_path = parts[0], parts[-1], "/".join(parts[1:-1])
        url = (f"/v1/{mount}/data/{secret_path}" if self._kv_version == 2
               else f"/v1/{mount}/{secret_path}")
        headers = {"X-Vault-Token": self._token}
        if self._namespace:
            headers["X-Vault-Namespace"] = self._namespace
        r = self._http().get(url, headers=headers)
        if r.status_code != 200:
            raise VaultError(f"Vault read {path!r} failed: HTTP {r.status_code} {r.text[:200]}")
        body = r.json()
        data = body["data"]["data"] if self._kv_version == 2 else body["data"]
        if field_name not in data:
            raise VaultError(f"field {field_name!r} not in Vault secret {mount}/{secret_path}")
        return data[field_name]

    def _http(self):
        if self._client is None:
            self._client = httpx.Client(base_url=self._addr, timeout=10.0)
        return self._client

def _with_credentials(dsn: str, user: str | None, password: str | None) -> str:
    """Inject user[:password]@ into a DSN that has no userinfo (creds come from Vault)."""
    p = urlsplit(dsn)
    auth = ""
    if user:
        auth = quote(user, safe="")
        if password:
            auth += ":" + quote(password, safe="")
        auth += "@"
    host = p.hostname or ""
    netloc = f"{auth}{host}" + (f":{p.port}" if p.port else "")
    return urlunsplit((p.scheme, netloc, p.path, p.query, p.fragment))

@dataclass
class Config:
    raw: dict
    resolver: SecretResolver = field(default_factory=SecretResolver)

    @classmethod
    def load(cls, path: str = "config.yaml") -> "Config":
        with open(path) as f:
            return cls(yaml.safe_load(f))

    def __getitem__(self, k):
        return self.raw[k]

    @property
    def read_token(self):
        return self.resolver.resolve(self.raw["alation"]["read_token_secret_ref"])

    @property
    def write_token(self):
        return self.resolver.resolve(self.raw["alation"]["write_token_secret_ref"])

    @property
    def pg_dsn(self):
        pg = self.raw["postgres"]
        dsn = pg["dsn"]
        if urlsplit(dsn).username:               # credentials already embedded -> use as-is
            return dsn
        user_ref, pass_ref = pg.get("user_secret_ref"), pg.get("pass_secret_ref")
        if not user_ref and not pass_ref:
            return dsn
        return _with_credentials(dsn,
                                 self.resolver.resolve(user_ref) if user_ref else None,
                                 self.resolver.resolve(pass_ref) if pass_ref else None)
