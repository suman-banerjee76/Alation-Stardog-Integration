"""SecretResolver: HashiCorp Vault (KV v2/v1) with an env-var fallback, plus DSN credential
injection. No live Vault — the HTTP client is injected."""
from __future__ import annotations
import pytest
from alation_rdf_sync.config import SecretResolver, Config, VaultError, _with_credentials


class FResp:
    def __init__(self, status=200, body=None, text=""):
        self.status_code = status
        self._body = body
        self.text = text
    def json(self): return self._body

class FClient:
    def __init__(self, *resps):
        self._resps = list(resps)
        self.calls = []
    def get(self, url, headers):
        self.calls.append((url, headers))
        return self._resps.pop(0) if self._resps else FResp(200, {"data": {"data": {}}})


# ---- env fallback (local dev) ----------------------------------------------

def test_env_fallback_when_no_vault_creds(monkeypatch):
    monkeypatch.delenv("VAULT_ADDR", raising=False)
    monkeypatch.delenv("VAULT_TOKEN", raising=False)
    monkeypatch.setenv("VAULT_KV_ALATION_READ-TOKEN", "from-env")
    r = SecretResolver()
    assert not r.vault_enabled
    assert r.resolve("vault://kv/alation/read-token") == "from-env"

def test_non_vault_ref_returned_literally():
    assert SecretResolver().resolve("postgresql://h/db") == "postgresql://h/db"

def test_unresolvable_raises(monkeypatch):
    monkeypatch.delenv("VAULT_ADDR", raising=False)
    monkeypatch.delenv("VAULT_TOKEN", raising=False)
    monkeypatch.delenv("VAULT_KV_ALATION_READ-TOKEN", raising=False)
    with pytest.raises(VaultError):
        SecretResolver().resolve("vault://kv/alation/read-token")


# ---- Vault KV v2 -----------------------------------------------------------

def test_vault_kv2_read_and_url():
    c = FClient(FResp(200, {"data": {"data": {"read-token": "S3CRET"}}}))
    r = SecretResolver(addr="http://v", token="tok", client=c)
    assert r.resolve("vault://kv/alation/read-token") == "S3CRET"
    url, headers = c.calls[0]
    assert url == "/v1/kv/data/alation" and headers["X-Vault-Token"] == "tok"
    assert "X-Vault-Namespace" not in headers

def test_vault_namespace_header():
    c = FClient(FResp(200, {"data": {"data": {"k": "v"}}}))
    SecretResolver(addr="http://v", token="t", namespace="team-a", client=c).resolve("vault://kv/p/k")
    assert c.calls[0][1]["X-Vault-Namespace"] == "team-a"

def test_vault_nested_secret_path():
    c = FClient(FResp(200, {"data": {"data": {"baz": "V"}}}))
    r = SecretResolver(addr="http://v", token="t", client=c)
    assert r.resolve("vault://kv/foo/bar/baz") == "V"
    assert c.calls[0][0] == "/v1/kv/data/foo/bar"

def test_vault_kv1_url_and_data():
    c = FClient(FResp(200, {"data": {"read-token": "x"}}))
    r = SecretResolver(addr="http://v", token="t", kv_version=1, client=c)
    assert r.resolve("vault://kv/alation/read-token") == "x"
    assert c.calls[0][0] == "/v1/kv/alation"

def test_vault_missing_field_raises():
    c = FClient(FResp(200, {"data": {"data": {"other": "x"}}}))
    with pytest.raises(VaultError):
        SecretResolver(addr="http://v", token="t", client=c).resolve("vault://kv/alation/read-token")

def test_vault_http_error_raises():
    c = FClient(FResp(403, None, "permission denied"))
    with pytest.raises(VaultError):
        SecretResolver(addr="http://v", token="t", client=c).resolve("vault://kv/alation/read-token")

def test_vault_result_is_cached():
    c = FClient(FResp(200, {"data": {"data": {"k": "v"}}}))
    r = SecretResolver(addr="http://v", token="t", client=c)
    r.resolve("vault://kv/p/k"); r.resolve("vault://kv/p/k")
    assert len(c.calls) == 1                      # second resolve served from cache


# ---- Config token/dsn wiring -----------------------------------------------

class StubResolver:
    def __init__(self, mapping): self.mapping = mapping
    def resolve(self, ref): return self.mapping.get(ref, ref)

def test_tokens_resolved_via_resolver():
    cfg = Config({"alation": {"read_token_secret_ref": "vault://r",
                              "write_token_secret_ref": "vault://w"}},
                 resolver=StubResolver({"vault://r": "READ", "vault://w": "WRITE"}))
    assert cfg.read_token == "READ" and cfg.write_token == "WRITE"

def test_pg_dsn_injects_resolved_credentials():
    cfg = Config({"postgres": {"dsn": "postgresql://host:5432/db",
                               "user_secret_ref": "vault://u", "pass_secret_ref": "vault://p"}},
                 resolver=StubResolver({"vault://u": "alice", "vault://p": "p@ss/word"}))
    # password special chars are percent-encoded
    assert cfg.pg_dsn == "postgresql://alice:p%40ss%2Fword@host:5432/db"

def test_pg_dsn_keeps_embedded_credentials():
    cfg = Config({"postgres": {"dsn": "postgresql://u:p@host/db", "user_secret_ref": "vault://u"}},
                 resolver=StubResolver({"vault://u": "ignored"}))
    assert cfg.pg_dsn == "postgresql://u:p@host/db"     # embedded creds win, no resolve

def test_pg_dsn_no_refs_returns_dsn():
    cfg = Config({"postgres": {"dsn": "postgresql://host/db"}}, resolver=StubResolver({}))
    assert cfg.pg_dsn == "postgresql://host/db"

def test_with_credentials_helper():
    assert _with_credentials("postgresql://h:5432/db", "u", "p") == "postgresql://u:p@h:5432/db"
    assert _with_credentials("postgresql://h/db", "u", None) == "postgresql://u@h/db"
