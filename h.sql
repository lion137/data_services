# ============================================================
# MODULE 1: vault_secret_manager.py
# ============================================================

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Protocol, Tuple

import hvac
from hvac.exceptions import InvalidPath, VaultError


# ----------------------------
# Errors & contracts
# ----------------------------

class SecretManagerError(Exception):
    """Domain-level error for Vault secret handling."""
    pass


class AuthProvider(Protocol):
    """
    Something that can ensure the hvac client has a valid token.
    """
    def ensure_auth(self, client: hvac.Client) -> None: ...
    def can_reauth(self) -> bool: ...


def _looks_like_token_problem(e: Exception) -> bool:
    """
    Best-effort heuristic for expired/invalid token errors.
    """
    msg = str(e).lower()
    return (
        "invalid token" in msg
        or "permission denied" in msg
        or "forbidden" in msg
    )


# ----------------------------
# Simple auth providers
# ----------------------------

@dataclass(frozen=True)
class TokenAuth(AuthProvider):
    """
    Static token auth (no re-auth possible).
    """
    token: str

    def ensure_auth(self, client: hvac.Client) -> None:
        client.token = self.token

    def can_reauth(self) -> bool:
        return False


@dataclass(frozen=True)
class AppRoleAuth(AuthProvider):
    """
    AppRole with fixed role_id + secret_id.
    """
    role_id: str
    secret_id: str

    def ensure_auth(self, client: hvac.Client) -> None:
        resp = client.auth.approle.login(
            role_id=self.role_id,
            secret_id=self.secret_id,
        )
        try:
            client.token = resp["auth"]["client_token"]
        except Exception as e:
            raise SecretManagerError(
                f"Unexpected AppRole login response: {resp}"
            ) from e

    def can_reauth(self) -> bool:
        return True


# ----------------------------
# Env config + address resolution
# ----------------------------

def _resolve_vault_addr() -> str:
    """
    Resolve Vault address either explicitly or via ENV=dev|uat|prod.
    """
    explicit = (os.getenv("VAULT_ADDR") or "").strip()
    if explicit:
        return explicit

    env = (os.getenv("ENV") or "").strip().lower()
    if env not in {"dev", "uat", "prod"}:
        raise SecretManagerError(
            "Either VAULT_ADDR or ENV (dev|uat|prod) must be set"
        )

    return f"https://vault-{env}.uk.hsbc:8200"


@dataclass
class VaultEnvConfig:
    address: str
    namespace: Optional[str]
    verify: bool
    auth_method: str
    token: Optional[str]
    role_id: Optional[str]
    secret_id: Optional[str]
    kv_mount: str
    kv_version: int
    cache_ttl: int

    @staticmethod
    def from_env() -> "VaultEnvConfig":
        addr = _resolve_vault_addr()

        namespace = os.getenv("VAULT_NAMESPACE") or None
        skip_verify = os.getenv("VAULT_SKIP_VERIFY", "false").lower() in (
            "1", "true", "yes"
        )

        auth_method = os.getenv("VAULT_AUTH_METHOD", "token").lower().strip()
        token = os.getenv("VAULT_TOKEN")
        role_id = os.getenv("VAULT_ROLE_ID")
        secret_id = os.getenv("VAULT_SECRET_ID")

        kv_mount = os.getenv("VAULT_KV_MOUNT", "secret").strip()
        kv_version = int(os.getenv("VAULT_KV_VERSION", "2"))
        cache_ttl = int(os.getenv("VAULT_CACHE_TTL", "300"))

        return VaultEnvConfig(
            address=addr,
            namespace=namespace,
            verify=not skip_verify,
            auth_method=auth_method,
            token=token,
            role_id=role_id,
            secret_id=secret_id,
            kv_mount=kv_mount,
            kv_version=kv_version,
            cache_ttl=cache_ttl,
        )


# ----------------------------
# VaultSecretManager
# ----------------------------

class VaultSecretManager:
    """
    Vault KV reader with:
      - in-memory secret-value cache
      - retry-on-token-expiry logic
      - optional background token renewal
    """

    def __init__(
        self,
        *,
        client: hvac.Client,
        auth: AuthProvider,
        kv_mount: str,
        default_kv_version: int = 2,
        cache_ttl: int = 300,
        auto_renew: bool = False,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._auth = auth
        self._kv_mount = kv_mount
        self._default_kv_version = default_kv_version
        self._cache_ttl = cache_ttl
        self._time = time_fn

        self._cache: Dict[
            Tuple[str, str, Optional[int]],
            Tuple[float, Dict[str, Any]],
        ] = {}

        self._stop_event = threading.Event()
        self._renew_thread: Optional[threading.Thread] = None

        # Initial authentication
        self._auth.ensure_auth(self._client)

        if auto_renew:
            self._start_auto_renew()

    @staticmethod
    def from_env(
        *,
        auto_renew: bool = False,
        client_factory: Callable[
            [str, Optional[str], bool], hvac.Client
        ] = lambda addr, ns, verify: hvac.Client(
            url=addr, namespace=ns, verify=verify
        ),
    ) -> "VaultSecretManager":
        cfg = VaultEnvConfig.from_env()
        client = client_factory(cfg.address, cfg.namespace, cfg.verify)

        if cfg.auth_method == "token":
            if not cfg.token:
                raise SecretManagerError("VAULT_TOKEN must be set")
            auth: AuthProvider = TokenAuth(cfg.token)

        elif cfg.auth_method == "approle":
            if not (cfg.role_id and cfg.secret_id):
                raise SecretManagerError(
                    "VAULT_ROLE_ID and VAULT_SECRET_ID required"
                )
            auth = AppRoleAuth(cfg.role_id, cfg.secret_id)

        else:
            raise SecretManagerError(
                "VAULT_AUTH_METHOD must be 'token' or 'approle'"
            )

        return VaultSecretManager(
            client=client,
            auth=auth,
            kv_mount=cfg.kv_mount,
            default_kv_version=cfg.kv_version,
            cache_ttl=cfg.cache_ttl,
            auto_renew=auto_renew,
        )

    def get_secret(
        self,
        path: str,
        *,
        key: Optional[str] = None,
        mount_point: Optional[str] = None,
        version: Optional[int] = None,
    ) -> Any:
        mp = mount_point or self._kv_mount
        cache_key = (mp, path, version)
        now = self._time()

        if cache_key in self._cache:
            expires_at, data = self._cache[cache_key]
            if now < expires_at:
                return data.get(key) if key else data
            self._cache.pop(cache_key, None)

        try:
            data = self._read_kv(path, mp, version)
        except VaultError as e:
            if self._auth.can_reauth() and _looks_like_token_problem(e):
                self._auth.ensure_auth(self._client)
                data = self._read_kv(path, mp, version)
            else:
                raise

        self._cache[cache_key] = (now + self._cache_ttl, data)
        return data.get(key) if key else data

    def _read_kv(
        self,
        path: str,
        mount_point: str,
        version: Optional[int],
    ) -> Dict[str, Any]:
        if self._default_kv_version == 2:
            try:
                resp = self._client.secrets.kv.v2.read_secret_version(
                    path=path,
                    mount_point=mount_point,
                    version=version,
                )
                return resp["data"]["data"]
            except InvalidPath:
                pass

        resp = self._client.secrets.kv.v1.read_secret(
            path=path,
            mount_point=mount_point,
        )
        return resp["data"]

    def _start_auto_renew(self) -> None:
        def worker() -> None:
            interval = 60.0
            while not self._stop_event.wait(interval):
                try:
                    self._client.renew_self_token()
                except Exception:
                    interval = min(interval * 2, 600)
                else:
                    interval = 60.0

        self._renew_thread = threading.Thread(
            target=worker,
            name="vault-auto-renew",
            daemon=True,
        )
        self._renew_thread.start()


# ============================================================
# MODULE 2: fetch_token_approle_auth.py
# ============================================================

class FetchTokenAppRoleAuth(AuthProvider):
    """
    Fetch-token → secret_id → AppRole login auth flow.

    Used when:
      - secret_id must NOT live in env
      - a long-lived periodic token is allowed
    """

    def __init__(
        self,
        *,
        fetch_token: str,
        role_name: str,
        role_id: str,
        client_factory: Callable[[], hvac.Client],
        renew_interval_s: float = 3600.0,
        enable_fetch_token_renew: bool = True,
    ) -> None:
        self._fetch_token = fetch_token
        self._role_name = role_name
        self._role_id = role_id
        self._client_factory = client_factory

        self._renew_interval_s = renew_interval_s
        self._stop = threading.Event()
        self._renew_thread: Optional[threading.Thread] = None

        if enable_fetch_token_renew:
            self._start_fetch_token_renewal()

    def can_reauth(self) -> bool:
        return True

    def close(self) -> None:
        self._stop.set()
        if self._renew_thread and self._renew_thread.is_alive():
            self._renew_thread.join(timeout=1)

    def _start_fetch_token_renewal(self) -> None:
        def worker() -> None:
            interval = self._renew_interval_s
            while not self._stop.wait(interval):
                try:
                    c = self._client_factory()
                    c.token = self._fetch_token
                    c.renew_self_token()
                except Exception:
                    interval = min(interval * 2, 6 * 3600)
                else:
                    interval = self._renew_interval_s

        self._renew_thread = threading.Thread(
            target=worker,
            name="vault-fetch-token-renew",
            daemon=True,
        )
        self._renew_thread.start()

    def ensure_auth(self, client: hvac.Client) -> None:
        fetch_client = self._client_factory()
        fetch_client.token = self._fetch_token

        sid_resp = fetch_client.auth.approle.generate_secret_id(
            role_name=self._role_name
        )
        try:
            secret_id = sid_resp["data"]["secret_id"]
        except Exception as e:
            raise SecretManagerError(
                f"Unexpected secret-id response: {sid_resp}"
            ) from e

        login_resp = client.auth.approle.login(
            role_id=self._role_id,
            secret_id=secret_id,
        )
        try:
            client.token = login_resp["auth"]["client_token"]
        except Exception as e:
            raise SecretManagerError(
                f"Unexpected login response: {login_resp}"
            ) from e


#  test manually ipython and env

export ENV=DEV
export VAULT_NAMESPACE="ITID/10671283_DSAQW"
export VAULT_AUTH_METHOD="approle"
export VAULT_ROLE_ID="...your role_id..."
export VAULT_SECRET_ID="...your secret_id..."
export VAULT_KV_MOUNT="secrets/kv_v2"
export VAULT_KV_VERSION=2
export VAULT_CACHE_TTL=60

from src.vault_secrets import VaultSecretManager

mgr = VaultSecretManager.from_env(auto_renew=False)
mgr.get_secret("dsecr/redis-creds")                 # dict
mgr.get_secret("dsecr/redis-creds", key="password") # single value
