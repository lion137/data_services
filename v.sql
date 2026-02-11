import pytest
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Callable

from hvac import exceptions as hvac_exc

# adjust to your real import
# from vault_secret_manager.secret_manager import VaultCredsClientCached, ClientConfig, RetryPolicy


# ---------------------------
# Helpers / fakes
# ---------------------------

class ScriptedError(Exception):
    """Non-hvac exception for testing unknown errors."""
    pass


class FakeKVv2:
    def __init__(self, parent: "FakeHVACClient"):
        self._p = parent

    def read_secret_version(self, mount_point: str, path: str) -> Dict[str, Any]:
        return self._p._pop("read_secret_v2")(mount_point, path)


class FakeKVv1:
    def __init__(self, parent: "FakeHVACClient"):
        self._p = parent

    def read_secret(self, mount_point: str, path: str) -> Dict[str, Any]:
        return self._p._pop("read_secret_v1")(mount_point, path)


class FakeKV:
    def __init__(self, parent: "FakeHVACClient"):
        self.v2 = FakeKVv2(parent)
        self.v1 = FakeKVv1(parent)


class FakeSecrets:
    def __init__(self, parent: "FakeHVACClient"):
        self.kv = FakeKV(parent)


class FakeAppRole:
    def __init__(self, parent: "FakeHVACClient"):
        self._p = parent

    def generate_secret_id(self, role_name: str) -> Dict[str, Any]:
        return self._p._pop("generate_secret_id")(role_name)

    def login(self, role_id: str, secret_id: str) -> Dict[str, Any]:
        return self._p._pop("approle_login")(role_id, secret_id)


class FakeAuth:
    def __init__(self, parent: "FakeHVACClient"):
        self.approle = FakeAppRole(parent)


class FakeHVACClient:
    """
    A fake hvac client that executes scripted callables for each operation.
    scripts: dict[str, list[callable]] keyed by operation name.
    Each callable returns a response OR raises.
    """
    def __init__(self, scripts: Dict[str, List[Callable[..., Any]]]):
        self.scripts = {k: list(v) for k, v in scripts.items()}
        self.token: Optional[str] = None

        self.secrets = FakeSecrets(self)
        self.auth = FakeAuth(self)

        self.calls: List[str] = []

    def _pop(self, op: str) -> Callable[..., Any]:
        self.calls.append(op)
        if op not in self.scripts or len(self.scripts[op]) == 0:
            raise AssertionError(f"No scripted behavior left for op={op}")
        return self.scripts[op].pop(0)


@dataclass(frozen=True)
class ClientConfig:
    addr: str = "https://vault.example"
    namespace: Optional[str] = "ns"
    verify: bool = True
    vault_base_token: str = "base-token"
    base_token_ttl_s: int = 300
    approle_role_name: str = "role-name"
    approle_role_id: str = "role-id"
    secrets_mount: str = "secret"
    secrets_kv_version: int = 2
    client_token_ttl_s: int = 60


class FakeTTLTime:
    def __init__(self):
        self.now = 0.0
    def time(self) -> float:
        return self.now
    def advance(self, s: float) -> None:
        self.now += s


# ---------------------------
# Fixtures
# ---------------------------

@pytest.fixture
def cfg_v2():
    return ClientConfig(secrets_kv_version=2)

@pytest.fixture
def cfg_v1():
    return ClientConfig(secrets_kv_version=1)

@pytest.fixture
def fake_time():
    return FakeTTLTime()


def no_sleep(_: float) -> None:
    return


def zero_backoff(attempt: int, base: float, mx: float) -> float:
    return 0.0


# ---------------------------
# Import the system under test
# ---------------------------

# IMPORTANT: replace these with your actual imports
# from vault_secret_manager.secret_manager import VaultCredsClientCached, RetryPolicy

# For this answer, assume your class supports injected sleep/backoff via refactor:
from vault_secret_manager.secret_manager import VaultCredsClientCached, RetryPolicy


# ---------------------------
# Tests: success paths
# ---------------------------

def test_fetch_vault_secret_kv2_success(cfg_v2, fake_time):
    scripts = {
        "generate_secret_id": [lambda role_name: {"data": {"secret_id": "sid"}}],
        "approle_login": [lambda role_id, secret_id: {"auth": {"client_token": "ct"}}],
        "read_secret_v2": [
            lambda mount, path: {"data": {"data": {"A": "1"}}}
        ],
    }
    client = FakeHVACClient(scripts)

    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=3),
        backoff_fn=zero_backoff,
    )

    out = sdk.fetch_vault_secret("myapp")
    assert out == {"A": "1"}
    assert client.calls == ["generate_secret_id", "approle_login", "read_secret_v2"]


def test_fetch_vault_secret_kv1_success(cfg_v1, fake_time):
    scripts = {
        "generate_secret_id": [lambda role_name: {"data": {"secret_id": "sid"}}],
        "approle_login": [lambda role_id, secret_id: {"auth": {"client_token": "ct"}}],
        "read_secret_v1": [
            lambda mount, path: {"data": {"B": "2"}}
        ],
    }
    client = FakeHVACClient(scripts)

    sdk = VaultCredsClientCached(
        cfg_v1,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=3),
        backoff_fn=zero_backoff,
    )

    out = sdk.fetch_vault_secret("myapp")
    assert out == {"B": "2"}
    assert client.calls == ["generate_secret_id", "approle_login", "read_secret_v1"]


def test_client_token_cached_second_call_does_not_relogin(cfg_v2, fake_time):
    scripts = {
        "generate_secret_id": [lambda _: {"data": {"secret_id": "sid"}}],
        "approle_login": [lambda *_: {"auth": {"client_token": "ct"}}],
        "read_secret_v2": [
            lambda *_: {"data": {"data": {"X": "1"}}},
            lambda *_: {"data": {"data": {"X": "2"}}},
        ],
    }
    client = FakeHVACClient(scripts)

    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=0),
        backoff_fn=zero_backoff,
    )

    assert sdk.fetch_vault_secret("p") == {"X": "1"}
    assert sdk.fetch_vault_secret("p") == {"X": "2"}

    # Only one approle flow, two reads
    assert client.calls.count("generate_secret_id") == 1
    assert client.calls.count("approle_login") == 1
    assert client.calls.count("read_secret_v2") == 2


# ---------------------------
# Tests: transient retries
# ---------------------------

@pytest.mark.parametrize(
    "exc_cls",
    [hvac_exc.RateLimitExceeded, hvac_exc.InternalServerError, hvac_exc.BadGateway, hvac_exc.VaultDown],
)
def test_transient_retry_then_success(cfg_v2, fake_time, exc_cls):
    transient = exc_cls(errors=["transient"], method="GET", url="u")
    scripts = {
        "generate_secret_id": [lambda _: {"data": {"secret_id": "sid"}}],
        "approle_login": [lambda *_: {"auth": {"client_token": "ct"}}],
        "read_secret_v2": [
            lambda *_: (_raise(transient)),
            lambda *_: {"data": {"data": {"OK": "y"}}},
        ],
    }
    client = FakeHVACClient(scripts)
    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=3),
        backoff_fn=zero_backoff,
    )

    assert sdk.fetch_vault_secret("p") == {"OK": "y"}
    assert client.calls.count("read_secret_v2") == 2


def test_transient_retry_exhaustion_raises(cfg_v2, fake_time):
    e = hvac_exc.VaultDown(errors=["down"], method="GET", url="u")
    scripts = {
        "generate_secret_id": [lambda _: {"data": {"secret_id": "sid"}}],
        "approle_login": [lambda *_: {"auth": {"client_token": "ct"}}],
        "read_secret_v2": [
            lambda *_: (_raise(e)),
            lambda *_: (_raise(e)),
            lambda *_: (_raise(e)),
            lambda *_: (_raise(e)),
        ],
    }
    client = FakeHVACClient(scripts)
    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=2),
        backoff_fn=zero_backoff,
    )

    with pytest.raises(hvac_exc.VaultDown):
        sdk.fetch_vault_secret("p")

    # initial + 2 retries = 3 attempts
    assert client.calls.count("read_secret_v2") == 3


# ---------------------------
# Tests: auth recovery behavior
# ---------------------------

def test_unauthorized_triggers_client_token_invalidate_and_recovers(cfg_v2, fake_time):
    unauth = hvac_exc.Unauthorized(errors=["invalid token"], method="GET", url="u")
    scripts = {
        # first login gives ct1, then after invalidation ct2
        "generate_secret_id": [
            lambda _: {"data": {"secret_id": "sid1"}},
            lambda _: {"data": {"secret_id": "sid2"}},
        ],
        "approle_login": [
            lambda *_: {"auth": {"client_token": "ct1"}},
            lambda *_: {"auth": {"client_token": "ct2"}},
        ],
        "read_secret_v2": [
            lambda *_: (_raise(unauth)),
            lambda *_: {"data": {"data": {"OK": "y"}}},
        ],
    }
    client = FakeHVACClient(scripts)

    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=0),
        backoff_fn=zero_backoff,
    )

    out = sdk.fetch_vault_secret("p")
    assert out == {"OK": "y"}

    # You should see a second approle flow due to invalidated client token.
    assert client.calls == [
        "generate_secret_id", "approle_login", "read_secret_v2",
        "generate_secret_id", "approle_login", "read_secret_v2"
    ]


def test_forbidden_policy_fails_fast_no_reauth(cfg_v2, fake_time):
    forbidden = hvac_exc.Forbidden(errors=["permission denied"], method="GET", url="u")
    scripts = {
        "generate_secret_id": [lambda _: {"data": {"secret_id": "sid"}}],
        "approle_login": [lambda *_: {"auth": {"client_token": "ct"}}],
        "read_secret_v2": [lambda *_: (_raise(forbidden))],
    }
    client = FakeHVACClient(scripts)

    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=0),
        backoff_fn=zero_backoff,
    )

    with pytest.raises(hvac_exc.Forbidden):
        sdk.fetch_vault_secret("p")

    # no extra relogin attempts
    assert client.calls == ["generate_secret_id", "approle_login", "read_secret_v2"]


def test_forbidden_invalid_token_is_treated_as_reauth_and_recovers(cfg_v2, fake_time):
    # This is the tricky real-world case: some envs return 403 with errors=["invalid token"]
    forbidden_invalid = hvac_exc.Forbidden(errors=["invalid token"], method="GET", url="u")

    scripts = {
        "generate_secret_id": [
            lambda _: {"data": {"secret_id": "sid1"}},
            lambda _: {"data": {"secret_id": "sid2"}},
        ],
        "approle_login": [
            lambda *_: {"auth": {"client_token": "ct1"}},
            lambda *_: {"auth": {"client_token": "ct2"}},
        ],
        "read_secret_v2": [
            lambda *_: (_raise(forbidden_invalid)),
            lambda *_: {"data": {"data": {"OK": "y"}}},
        ],
    }
    client = FakeHVACClient(scripts)

    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=0),
        backoff_fn=zero_backoff,
    )

    assert sdk.fetch_vault_secret("p") == {"OK": "y"}
    assert client.calls.count("approle_login") == 2


def test_invalid_path_fails_fast(cfg_v2, fake_time):
    e = hvac_exc.InvalidPath(errors=["no such path"], method="GET", url="u")
    scripts = {
        "generate_secret_id": [lambda _: {"data": {"secret_id": "sid"}}],
        "approle_login": [lambda *_: {"auth": {"client_token": "ct"}}],
        "read_secret_v2": [lambda *_: (_raise(e))],
    }
    client = FakeHVACClient(scripts)

    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=3),
        backoff_fn=zero_backoff,
    )

    with pytest.raises(hvac_exc.InvalidPath):
        sdk.fetch_vault_secret("p")


# ---------------------------
# Tests: failures inside AppRole chain
# ---------------------------

def test_generate_secret_id_unauthorized_causes_reauth_flow(cfg_v2, fake_time):
    # If base token is wrong/expired, generate_secret_id may return Unauthorized.
    unauth = hvac_exc.Unauthorized(errors=["invalid token"], method="POST", url="u")

    scripts = {
        "generate_secret_id": [
            lambda *_: (_raise(unauth)),
            lambda *_: {"data": {"secret_id": "sid"}},
        ],
        "approle_login": [
            lambda *_: {"auth": {"client_token": "ct"}},
        ],
        "read_secret_v2": [
            lambda *_: {"data": {"data": {"OK": "y"}}},
        ],
    }
    client = FakeHVACClient(scripts)
    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=0),
        backoff_fn=zero_backoff,
    )

    # Depending on your recovery strategy, you may raise SecretManagerError after base_token invalidation.
    # If you implement base-token invalidation + one retry at approle stage, assert success.
    out = sdk.fetch_vault_secret("p")
    assert out == {"OK": "y"}


def test_login_forbidden_fails_fast(cfg_v2, fake_time):
    forbidden = hvac_exc.Forbidden(errors=["permission denied"], method="POST", url="u")
    scripts = {
        "generate_secret_id": [lambda *_: {"data": {"secret_id": "sid"}}],
        "approle_login": [lambda *_: (_raise(forbidden))],
    }
    client = FakeHVACClient(scripts)
    sdk = VaultCredsClientCached(
        cfg_v2,
        client_factory=lambda: client,
        time_fn=fake_time.time,
        sleep_fn=no_sleep,
        retry_policy=RetryPolicy(max_transient_retries=0),
        backoff_fn=zero_backoff,
    )

    with pytest.raises(hvac_exc.Forbidden):
        sdk.fetch_vault_secret("p")


# ---------------------------
# Utility
# ---------------------------

def _raise(e: Exception):
    raise e




# first iteratoin
import random
import time
from typing import Any, Callable, Dict, Optional

import hvac
from hvac import exceptions as hvac_exc

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore


class SecretManagerError(Exception):
    pass


def _sleep_backoff(attempt: int, base_delay_s: float = 0.25, max_delay_s: float = 5.0) -> None:
    # exp backoff + jitter, capped
    delay = min(max_delay_s, base_delay_s * (2**attempt))
    delay *= (1.0 + random.random())
    time.sleep(delay)


def _errors_lower(e: Exception) -> str:
    # structured fallback only (NOT str(e) parsing)
    errs = getattr(e, "errors", None)
    if not errs:
        return ""
    return " ".join(str(x).lower() for x in errs)


def _is_reauth_needed(e: Exception) -> bool:
    # Best signal: 401 Unauthorized
    if isinstance(e, hvac_exc.Unauthorized):
        return True

    # Optional: some Vault setups return "invalid token" as 403 Forbidden.
    # This is still better than scanning full str(e); it uses Vault's structured errors list.
    if isinstance(e, hvac_exc.Forbidden):
        return "invalid token" in _errors_lower(e)

    # If you see 400 InvalidRequest for bad token in your environment, you can add:
    # if isinstance(e, hvac_exc.InvalidRequest) and "invalid token" in _errors_lower(e):
    #     return True

    return False


def _is_transient(e: Exception) -> bool:
    # hvac-transient statuses
    if isinstance(
        e,
        (
            hvac_exc.RateLimitExceeded,   # 429
            hvac_exc.InternalServerError, # 500
            hvac_exc.BadGateway,          # 502
            hvac_exc.VaultDown,           # 503
        ),
    ):
        return True

    # network layer (requests)
    if requests is not None and isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True

    return False


def _call_with_transient_retries(
    fn: Callable[[], Any],
    *,
    max_retries: int = 3,
    base_delay_s: float = 0.25,
) -> Any:
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as e:
            if _is_transient(e) and attempt < max_retries:
                _sleep_backoff(attempt, base_delay_s=base_delay_s)
                attempt += 1
                continue
            raise


class VaultCredsClientCached:
    """
    Vault client to fetch credentials.

    base_token (read from Vault KV, cached) ->
        read fetch token (from Vault KV, cached) ->
            generate_secret_id + read role_id ->
                approle login -> client token (cached) ->
                    read secret

    On auth error:
        invalidate client token and retry once.
        if still failing, invalidate fetch token and retry once.
        if still failing, invalidate base token and stop (bootstrap issue).
    """

    def __init__(
        self,
        cfg: "ClientConfig",
        client_factory: Optional[Callable[[], hvac.Client]] = None,
        time_fn: Callable[[], float] = time.time,
        get_variable: Callable[[str], Optional[str]] = None,
    ) -> None:
        self._cfg = cfg
        self._client_factory = client_factory or (
            lambda: hvac.Client(
                url=cfg.addr,
                namespace=cfg.namespace,
                verify=cfg.verify,
            )
        )
        self._cache = TTLCache(time_fn=time_fn)
        self._get_variable = get_variable

    def _read_kv_v2(self, client: hvac.Client, mount: str, path: str) -> Dict[str, Any]:
        resp = client.secrets.kv.v2.read_secret_version(mount_point=mount, path=path)
        return resp["data"]["data"]

    def _get_base_token(self) -> str:
        cached = self._cache.get("base_token")
        if cached:
            return cached

        tok = self._cfg.vault_base_token
        if not tok:
            raise SecretManagerError(
                f"Base token record missing key '{self._cfg.vault_base_token}'"
            )

        tok = str(tok)
        self._cache.set("base_token", tok, self._cfg.base_token_ttl_s)
        return tok

    def _get_client_token(self) -> str:
        cached = self._cache.get("client_token")
        if cached:
            return cached

        base = self._get_base_token()
        c = self._client_factory()
        c.token = base

        # generate secret_id using bootstrap/base token
        sid_resp = c.auth.approle.generate_secret_id(role_name=self._cfg.approle_role_name)
        secret_id = sid_resp["data"]["secret_id"]

        # login using role_id + generated secret_id
        login = c.auth.approle.login(role_id=self._cfg.approle_role_id, secret_id=secret_id)
        client_token = login["auth"]["client_token"]

        self._cache.set("client_token", client_token, self._cfg.client_token_ttl_s)
        return client_token

    def _read_secret_once(self, full_path: str) -> Dict[str, Any]:
        token = self._get_client_token()
        app = self._client_factory()
        app.token = token

        if self._cfg.secrets_kv_version == 2:
            resp = app.secrets.kv.v2.read_secret_version(
                mount_point=self._cfg.secrets_mount,
                path=full_path,
            )
            return resp["data"]["data"]

        resp = app.secrets.kv.v1.read_secret(
            mount_point=self._cfg.secrets_mount,
            path=full_path,
        )
        return resp["data"]

    def _get_kv_secret(self, path: str) -> Dict[str, Any]:
        # keep your role_name prefix behavior
        full_path = f"{self._cfg.approle_role_name}/{path}"

        def read_once_with_transient_retries() -> Dict[str, Any]:
            return _call_with_transient_retries(self._read_secret_once, max_retries=3)

        # 1) try with current cache
        try:
            return read_once_with_transient_retries()
        except hvac_exc.VaultError as e:
            # Forbidden is usually policy/namespace/path: fail fast unless it's token-invalid.
            if isinstance(e, hvac_exc.Forbidden) and not _is_reauth_needed(e):
                raise

            if not _is_reauth_needed(e):
                raise

        # 2) auth recovery step 1: invalidate client token
        self._cache.invalidate("client_token")
        try:
            return read_once_with_transient_retries()
        except hvac_exc.VaultError as e:
            if isinstance(e, hvac_exc.Forbidden) and not _is_reauth_needed(e):
                raise
            if not _is_reauth_needed(e):
                raise

        # 3) auth recovery step 2: invalidate fetch token (if you actually use it elsewhere)
        self._cache.invalidate("fetch_token")
        self._cache.invalidate("client_token")
        try:
            return read_once_with_transient_retries()
        except hvac_exc.VaultError as e:
            if isinstance(e, hvac_exc.Forbidden) and not _is_reauth_needed(e):
                raise
            if not _is_reauth_needed(e):
                raise

        # 4) auth recovery step 3: invalidate base token and fail (bootstrap issue)
        self._cache.invalidate("base_token")
        raise SecretManagerError(
            "Vault auth failed after invalidating client_token and fetch_token; "
            "base token likely invalid/expired or policy/namespace misconfigured."
        )

    # your public helper stays the same
    def fetch_vault_secret(self, name: str) -> Dict[str, Any]:
        return self._get_kv_secret(name)



# after refacor

from dataclasses import dataclass
from typing import Callable, Optional
import time
import random
from hvac import exceptions as hvac_exc

@dataclass(frozen=True)
class RetryPolicy:
    max_transient_retries: int = 3
    base_delay_s: float = 0.25
    max_delay_s: float = 5.0


def default_backoff(attempt: int, base_delay_s: float, max_delay_s: float) -> float:
    delay = min(max_delay_s, base_delay_s * (2**attempt))
    return delay * (1.0 + random.random())


class VaultCredsClientCached:
    def __init__(
        self,
        cfg: "ClientConfig",
        client_factory: Optional[Callable[[], "hvac.Client"]] = None,
        time_fn: Callable[[], float] = time.time,
        sleep_fn: Callable[[float], None] = time.sleep,
        retry_policy: RetryPolicy = RetryPolicy(),
        backoff_fn: Callable[[int, float, float], float] = default_backoff,
        is_transient_fn: Callable[[Exception], bool] = _is_transient,
        is_reauth_needed_fn: Callable[[Exception], bool] = _is_reauth_needed,
    ) -> None:
        self._cfg = cfg
        self._client_factory = client_factory or (lambda: hvac.Client(...))
        self._cache = TTLCache(time_fn=time_fn)

        self._sleep = sleep_fn
        self._retry_policy = retry_policy
        self._backoff_fn = backoff_fn
        self._is_transient = is_transient_fn
        self._is_reauth_needed = is_reauth_needed_fn

    def _call_with_transient_retries(self, fn):
        attempt = 0
        while True:
            try:
                return fn()
            except Exception as e:
                if self._is_transient(e) and attempt < self._retry_policy.max_transient_retries:
                    delay = self._backoff_fn(
                        attempt, self._retry_policy.base_delay_s, self._retry_policy.max_delay_s
                    )
                    self._sleep(delay)
                    attempt += 1
                    continue
                raise

