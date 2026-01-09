import pytest
from typing import Optional, Any, Dict

from vault_secret_service import VaultSecretService, VaultServiceConfig, SecretManagerError


# ----------------------------
# Tiny fakes (no real hvac calls)
# ----------------------------

class FakeAdapter:
    def __init__(self, client: "FakeHvacClient"):
        self.client = client

    def post(self, url: str, json: Dict[str, Any]):
        if url.endswith("v1/sys/wrapping/unwrap"):
            # returns fetch token
            return {"auth": {"client_token": "FETCH_TOKEN"}}
        if url.endswith("v1/auth/token/create-orphan"):
            return {"auth": {"client_token": "NEW_ORPHAN"}}
        raise RuntimeError(f"unexpected url: {url}")


class FakeSys:
    def __init__(self, client: "FakeHvacClient"):
        self.client = client

    def unwrap(self, token: str):
        return {"auth": {"client_token": "FETCH_TOKEN"}}


class FakeTokenAuth:
    def __init__(self, client: "FakeHvacClient"):
        self.client = client

    def create_orphan(self, **payload):
        return {"auth": {"client_token": "NEW_ORPHAN"}}


class FakeAppRoleAuth:
    def __init__(self, client: "FakeHvacClient"):
        self.client = client

    def read_role_id(self, role_name: str):
        return {"data": {"role_id": "ROLE_ID"}}

    def generate_secret_id(self, role_name: str):
        return {"data": {"secret_id": "SECRET_ID"}}

    def login(self, role_id: str, secret_id: str):
        return {"auth": {"client_token": "CLIENT_TOKEN"}}


class FakeAuth:
    def __init__(self, client: "FakeHvacClient"):
        self.client = client
        self.token = FakeTokenAuth(client)
        self.approle = FakeAppRoleAuth(client)


class FakeKvV2:
    def __init__(self, client: "FakeHvacClient"):
        self.client = client
        self.calls = 0

    def read_secret_version(self, path: str, mount_point: str, version: Optional[int] = None):
        self.calls += 1
        # simulate "token problem" if configured
        if self.client.fail_with_token_problem and self.calls == 1:
            raise Exception("permission denied: invalid token")
        return {"data": {"data": {"username": "u", "password": "p"}}}


class FakeKvV1:
    def read_secret(self, path: str, mount_point: str):
        return {"data": {"k": "v"}}


class FakeSecrets:
    def __init__(self, client: "FakeHvacClient"):
        self.client = client
        self.kv = type("KV", (), {})()
        self.kv.v2 = FakeKvV2(client)
        self.kv.v1 = FakeKvV1()


class FakeHvacClient:
    def __init__(self):
        self.token: Optional[str] = None
        self.fail_with_token_problem = False
        self.auth = FakeAuth(self)
        self.secrets = FakeSecrets(self)
        self.sys = FakeSys(self)
        self.adapter = FakeAdapter(self)

    def renew_self_token(self):
        # no-op for tests
        return {"auth": {"renewable": True}}


def fake_client_factory(addr: str, ns: Optional[str], verify: bool):
    return FakeHvacClient()


# ----------------------------
# Tests
# ----------------------------

def make_cfg():
    return VaultServiceConfig(
        address="https://vault-dev.uk.hsbc:8200",
        namespace="ITID/10671283_DSAQW/",
        verify=True,
        kv_mount="secrets/kv_v2",
        kv_version=2,
        cache_ttl_s=300,
        orphan_token="ORPHAN",
        orphan_policies=["default"],
        orphan_period="168h",
        orphan_renewable=True,
        wrapped_fetch_token="WRAPPED",
        approle_role_name="monitoring-uat",
        enable_background_renewal=False,
        orphan_rotate_interval_s=3600,
        fetch_token_renew_interval_s=3600,
    )


def test_happy_path_reads_secret_and_caches():
    now = 1000.0
    def time_fn():
        return now

    svc = VaultSecretService(cfg=make_cfg(), client_factory=fake_client_factory, time_fn=time_fn)
    data1 = svc.get_secret("dsecr/redis-creds")
    assert data1["username"] == "u"

    # second call should hit cache, so v2 read should still be only once
    data2 = svc.get_secret("dsecr/redis-creds")
    assert data2["password"] == "p"
    assert svc._app_client.secrets.kv.v2.calls == 1  # type: ignore[attr-defined]

    svc.close()


def test_cache_expires_and_refetches():
    now = 1000.0
    def time_fn():
        return now

    cfg = make_cfg()
    cfg = VaultServiceConfig(**{**cfg.__dict__, "cache_ttl_s": 10})  # short ttl
    svc = VaultSecretService(cfg=cfg, client_factory=fake_client_factory, time_fn=time_fn)

    svc.get_secret("dsecr/redis-creds")
    assert svc._app_client.secrets.kv.v2.calls == 1  # type: ignore[attr-defined]

    now = 1011.0  # past ttl
    svc.get_secret("dsecr/redis-creds")
    assert svc._app_client.secrets.kv.v2.calls == 2  # type: ignore[attr-defined]
    svc.close()


def test_token_problem_triggers_reauth_and_retry():
    svc = VaultSecretService(cfg=make_cfg(), client_factory=fake_client_factory, time_fn=lambda: 0.0)

    # First call will fail with token problem, then service should clear client token, reauth, retry
    svc._app_client.secrets.kv.v2.client.fail_with_token_problem = True  # type: ignore[attr-defined]
    data = svc.get_secret("dsecr/redis-creds")
    assert data["username"] == "u"

    svc.close()


def test_missing_orphan_token_raises():
    cfg = make_cfg()
    cfg = VaultServiceConfig(**{**cfg.__dict__, "orphan_token": None})
    svc = VaultSecretService(cfg=cfg, client_factory=fake_client_factory, time_fn=lambda: 0.0)

    with pytest.raises(SecretManagerError):
        svc.get_secret("dsecr/redis-creds")

    svc.close()
