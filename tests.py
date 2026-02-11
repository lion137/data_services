import pytest

import src.vault_secrets as vs


# -------------------------
# Fake hvac client structure for KV reads
# -------------------------

class FakeKVv2:
    def __init__(self, parent):
        self.parent = parent

    def read_secret_version(self, *, path, mount_point, version=None):
        self.parent.calls.append(("kv2.read", mount_point, path, version))
        return self.parent.read_impl(path=path, mount_point=mount_point, version=version)


class FakeKVv1:
    def __init__(self, parent):
        self.parent = parent

    def read_secret(self, *, path, mount_point):
        self.parent.calls.append(("kv1.read", mount_point, path))
        return self.parent.read_impl(path=path, mount_point=mount_point, version=None)


class FakeSecretsKV:
    def __init__(self, parent):
        self.v2 = FakeKVv2(parent)
        self.v1 = FakeKVv1(parent)


class FakeSecrets:
    def __init__(self, parent):
        self.kv = FakeSecretsKV(parent)


class FakeClient:
    """
    Minimal fake of hvac.Client used by VaultSecretManager:
      - .token
      - .secrets.kv.v2.read_secret_version / v1.read_secret
      - .renew_self_token()
    """
    def __init__(self, read_impl):
        self.token = None
        self.calls = []
        self.secrets = FakeSecrets(self)
        self._read_impl = read_impl

    def read_impl(self, *, path, mount_point, version):
        return self._read_impl(path=path, mount_point=mount_point, version=version)

    def renew_self_token(self):
        self.calls.append(("renew_self_token",))


class FakeAuthProvider:
    """
    Used to verify retry logic calls ensure_auth().
    """
    def __init__(self):
        self.ensure_calls = 0

    def ensure_auth(self, client):
        self.ensure_calls += 1
        client.token = f"TOKEN-{self.ensure_calls}"

    def can_reauth(self):
        return True


# -------------------------
# Tests
# -------------------------

def test_cache_hit_and_expiry(monkeypatch):
    # deterministic time
    t = {"now": 1000.0}
    def time_fn():
        return t["now"]

    state = {"reads": 0}

    def read_impl(*, path, mount_point, version):
        state["reads"] += 1
        # hvac kv2 response shape expected by _read_kv()
        return {"data": {"data": {"username": "u", "password": "p"}}}

    client = FakeClient(read_impl=read_impl)

    mgr = vs.VaultSecretManager(
        client=client,
        auth=vs.TokenAuth("STATIC"),
        kv_mount="secrets/kv_v2",
        default_kv_version=2,
        cache_ttl=60,
        auto_renew=False,
        time_fn=time_fn,
    )

    # first fetch hits Vault
    data1 = mgr.get_secret("dsecr/redis-creds")
    assert data1["username"] == "u"
    assert state["reads"] == 1

    # within TTL -> cache hit
    t["now"] = 1050.0
    data2 = mgr.get_secret("dsecr/redis-creds")
    assert data2["password"] == "p"
    assert state["reads"] == 1

    # after TTL -> hits Vault again
    t["now"] = 1061.0
    _ = mgr.get_secret("dsecr/redis-creds")
    assert state["reads"] == 2


def test_retry_on_token_problem_calls_reauth(monkeypatch):
    # patch VaultError in the module to a local fake, because manager catches vs.VaultError
    class FakeVaultError(Exception):
        pass

    monkeypatch.setattr(vs, "VaultError", FakeVaultError)

    auth = FakeAuthProvider()
    attempts = {"n": 0}

    def read_impl(*, path, mount_point, version):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise FakeVaultError("permission denied")  # should trigger retry
        return {"data": {"data": {"ok": True}}}

    client = FakeClient(read_impl=read_impl)

    mgr = vs.VaultSecretManager(
        client=client,
        auth=auth,
        kv_mount="secrets/kv_v2",
        default_kv_version=2,
        cache_ttl=0,
        auto_renew=False,
        time_fn=lambda: 0.0,
    )

    data = mgr.get_secret("dsecr/redis-creds")
    assert data["ok"] is True
    # ensure_auth called once at init + once on retry
    assert auth.ensure_calls == 2
    assert attempts["n"] == 2



# new test file

import src.vault_secrets as vs


# -------------------------
# Fake hvac auth surface
# -------------------------

class FakeAppRoleAPI:
    def __init__(self, client):
        self.client = client

    def generate_secret_id(self, role_name):
        self.client.calls.append(("generate_secret_id", role_name))
        return {"data": {"secret_id": "SID-123"}}

    def login(self, role_id, secret_id):
        self.client.calls.append(("login", role_id, secret_id))
        return {"auth": {"client_token": f"APP-TOKEN-{role_id}"}}


class FakeAuth:
    def __init__(self, client):
        self.approle = FakeAppRoleAPI(client)


class FakeClient:
    def __init__(self):
        self.token = None
        self.calls = []
        self.auth = FakeAuth(self)

    def renew_self_token(self):
        self.calls.append(("renew_self_token",))


def test_fetch_token_flow_sets_client_token():
    fetch_client = FakeClient()
    app_client = FakeClient()

    def client_factory():
        # in real use this returns a new hvac.Client each time,
        # in tests we return our fake for determinism
        return fetch_client

    auth = vs.FetchTokenAppRoleAuth(
        fetch_token="FETCH",
        role_name="my-role",
        role_id="RID-999",
        client_factory=client_factory,
        enable_fetch_token_renew=False,  # avoid thread in unit tests
    )

    auth.ensure_auth(app_client)

    assert fetch_client.token == "FETCH"
    assert app_client.token == "APP-TOKEN-RID-999"
    assert fetch_client.calls == [("generate_secret_id", "my-role")]
    assert app_client.calls == [("login", "RID-999", "SID-123")]


def test_fetch_token_flow_bad_secret_id_response_raises():
    fetch_client = FakeClient()
    app_client = FakeClient()

    # break the response shape
    fetch_client.auth.approle.generate_secret_id = lambda role_name: {"data": {}}

    def client_factory():
        return fetch_client

    auth = vs.FetchTokenAppRoleAuth(
        fetch_token="FETCH",
        role_name="my-role",
        role_id="RID-999",
        client_factory=client_factory,
        enable_fetch_token_renew=False,
    )

    try:
        auth.ensure_auth(app_client)
        assert False, "Expected SecretManagerError"
    except vs.SecretManagerError:
        pass
