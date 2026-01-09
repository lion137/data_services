from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Protocol, Tuple

import hvac
from hvac.exceptions import InvalidPath, VaultError


class SecretManagerError(Exception):
    pass


# ----------------------------
# Dependency injection contracts
# ----------------------------

class AuthProvider(Protocol):
    """Something that can ensure the hvac client has a valid token."""
    def ensure_auth(self, client: hvac.Client) -> None: ...
    def can_reauth(self) -> bool: ...


@dataclass(frozen=True)
class TokenAuth(AuthProvider):
    token: str

    def ensure_auth(self, client: hvac.Client) -> None:
        client.token = self.token

    def can_reauth(self) -> bool:
        return False  # token is static; reauth doesn't change anything


@dataclass(frozen=True)
class AppRoleAuth(AuthProvider):
    role_id: str
    secret_id: str

    def ensure_auth(self, client: hvac.Client) -> None:
        resp = client.auth.approle.login(role_id=self.role_id, secret_id=self.secret_id)
        # IMPORTANT: set the client token from response
        try:
            client.token = resp["auth"]["client_token"]
        except Exception as e:
            raise SecretManagerError(f"Unexpected AppRole login response: {resp}") from e

    def can_reauth(self) -> bool:
        return True


# ----------------------------
# Env config + address resolution
# ----------------------------

def _resolve_vault_addr() -> str:
    explicit = (os.getenv("VAULT_ADDR") or "").strip()
    if explicit:
        return explicit

    env = (os.getenv("ENV") or "").strip().lower()
    if not env:
        raise SecretManagerError("Either VAULT_ADDR or ENV must be set")

    if env not in {"dev", "uat", "prod"}:
        raise SecretManagerError("ENV must be one of: DEV, UAT, PROD")

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

        ns = os.getenv("VAULT_NAMESPACE") or None
        skip_verify = (
            os.getenv("VAULT_SKIP_VERIFY", "false").lower() in ("1", "true", "yes")
        )

        auth_method = (os.getenv("VAULT_AUTH_METHOD", "token").strip().lower() or "token")
        token = os.getenv("VAULT_TOKEN")
        role_id = os.getenv("VAULT_ROLE_ID")
        secret_id = os.getenv("VAULT_SECRET_ID")

        kv_mount = os.getenv("VAULT_KV_MOUNT", "secret").strip() or "secret"

        kv_version_str = os.getenv("VAULT_KV_VERSION", "2").strip()
        try:
            kv_version = int(kv_version_str)
        except ValueError:
            kv_version = 2

        cache_ttl_str = os.getenv("VAULT_CACHE_TTL", "300").strip()
        try:
            cache_ttl = int(cache_ttl_str)
        except ValueError:
            cache_ttl = 300

        return VaultEnvConfig(
            address=addr,
            namespace=ns,
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
# VaultSecretManager (retry + cache + renew)
# ----------------------------

def _looks_like_token_problem(e: Exception) -> bool:
    """
    Heuristic: hvac raises VaultError (or derived) with messages like:
      - 'permission denied'
      - 'invalid token'
      - 'permission denied: invalid token'
    In practice, token expiry often shows up as "permission denied" or "invalid token"
    depending on gateway / namespace handling.
    """
    msg = str(e).lower()
    return ("invalid token" in msg) or ("permission denied" in msg) or ("forbidden" in msg)


class VaultSecretManager:
    """
    Read secrets from Vault KV with:
      - in-memory caching of secret VALUES
      - optional token renewal thread
      - optional re-auth retry (AppRole) on token problems
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
        # DI for tests
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._auth = auth
        self._kv_mount = kv_mount
        self._default_kv_version = default_kv_version if default_kv_version in (1, 2) else 2
        self._cache_ttl = cache_ttl
        self._time = time_fn

        self._cache: Dict[
            Tuple[str, str, Optional[int]],
            Tuple[float, Dict[str, Any]],
        ] = {}

        self._renew_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Ensure client is authenticated once at construction.
        self._auth.ensure_auth(self._client)

        if auto_renew:
            self._start_auto_renew()

    @staticmethod
    def from_env(
        *,
        optional: bool = False,
        auto_renew: bool = False,
        # DI hook: in tests you can pass a fake
        client_factory: Callable[[str, Optional[str], bool], hvac.Client] = (
            lambda addr, ns, verify: hvac.Client(url=addr, namespace=ns, verify=verify)
        ),
    ) -> Optional["VaultSecretManager"]:
        try:
            cfg = VaultEnvConfig.from_env()
        except SecretManagerError:
            if optional:
                return None
            raise

        client = client_factory(cfg.address, cfg.namespace, cfg.verify)

        if cfg.auth_method == "approle":
            if not (cfg.role_id and cfg.secret_id):
                raise SecretManagerError("AppRole auth requires VAULT_ROLE_ID and VAULT_SECRET_ID")
            auth: AuthProvider = AppRoleAuth(role_id=cfg.role_id, secret_id=cfg.secret_id)
        elif cfg.auth_method == "token":
            if not cfg.token:
                raise SecretManagerError("Token auth requires VAULT_TOKEN to be set")
            auth = TokenAuth(token=cfg.token)
        else:
            raise SecretManagerError("VAULT_AUTH_METHOD must be 'token' or 'approle'")

        return VaultSecretManager(
            client=client,
            auth=auth,
            kv_mount=cfg.kv_mount,
            default_kv_version=cfg.kv_version,
            cache_ttl=cfg.cache_ttl,
            auto_renew=auto_renew,
        )

    def close(self) -> None:
        self._stop_event.set()
        if self._renew_thread and self._renew_thread.is_alive():
            self._renew_thread.join(timeout=1)

    def get_secret(
        self,
        path: str,
        *,
        key: Optional[str] = None,
        mount_point: Optional[str] = None,
        version: Optional[int] = None,
        raise_on_missing: bool = False,
    ) -> Any:
        mp = mount_point or self._kv_mount
        cache_key = (mp, path, version)
        now = self._time()

        # Cache hit
        if cache_key in self._cache:
            expires_at, data = self._cache[cache_key]
            if now < expires_at:
                return data.get(key) if key else data
            self._cache.pop(cache_key, None)

        # 1st attempt
        try:
            data = self._read_kv(path=path, mount_point=mp, version=version)
        except VaultError as ve:
            # Retry once if it looks like token expired/invalid and we can reauth
            if self._auth.can_reauth() and _looks_like_token_problem(ve):
                try:
                    self._auth.ensure_auth(self._client)  # re-login / refresh token
                    data = self._read_kv(path=path, mount_point=mp, version=version)
                except VaultError as ve2:
                    if raise_on_missing:
                        raise SecretManagerError(str(ve2)) from ve2
                    return None

            if raise_on_missing:
                raise SecretManagerError(str(ve)) from ve
            return None

        self._cache[cache_key] = (now + self._cache_ttl, data)
        return data.get(key) if key else data

    def clear_cache(self) -> None:
        self._cache.clear()

    def _read_kv(
        self,
        *,
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
        """
        Best-effort renew of the CURRENT token.
        Note: if token TTL is hard-limited and not renewable, this won't help.
        """
        def _worker() -> None:
            interval = 60.0
            while not self._stop_event.wait(interval):
                try:
                    self._client.renew_self_token()
                except Exception:
                    interval = min(interval * 2, 600)
                else:
                    interval = 60.0

        t = threading.Thread(target=_worker, name="vault-auto-renew", daemon=True)
        t.start()
        self._renew_thread = t


__all__ = ["VaultSecretManager", "SecretManagerError", "VaultEnvConfig", "TokenAuth", "AppRoleAuth"]
