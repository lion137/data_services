from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple, List

import hvac
from hvac.exceptions import InvalidPath, VaultError


class SecretManagerError(Exception):
    pass


# ----------------------------
# Helpers
# ----------------------------

def _truthy(v: str) -> bool:
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _split_csv(v: str) -> List[str]:
    return [x.strip() for x in v.split(",") if x.strip()]


def resolve_vault_addr() -> str:
    """
    If VAULT_ADDR is set, use it.
    Else use ENV in {dev, uat, prod} -> https://vault-<env>.uk.hsbc:8200
    """
    explicit = (os.getenv("VAULT_ADDR") or "").strip()
    if explicit:
        return explicit

    env = (os.getenv("ENV") or "").strip().lower()
    if not env:
        raise SecretManagerError("Set VAULT_ADDR or ENV (DEV/UAT/PROD).")

    if env not in {"dev", "uat", "prod"}:
        raise SecretManagerError("ENV must be one of: DEV, UAT, PROD")

    return f"https://vault-{env}.uk.hsbc:8200"


def looks_like_token_problem(e: Exception) -> bool:
    msg = str(e).lower()
    return ("invalid token" in msg) or ("permission denied" in msg) or ("forbidden" in msg)


# ----------------------------
# Env config
# ----------------------------

@dataclass(frozen=True)
class VaultServiceConfig:
    # Connection
    address: str
    namespace: Optional[str]
    verify: bool

    # KV
    kv_mount: str
    kv_version: int
    cache_ttl_s: int

    # Auth chain
    # 1) long-lived orphan token (rotated via create-orphan)
    orphan_token: Optional[str]
    orphan_policies: List[str]
    orphan_period: Optional[str]     # e.g. "168h" or "192h" depending on your setup
    orphan_renewable: bool

    # 2) wrapped fetch token to unwrap into a "fetch token"
    wrapped_fetch_token: Optional[str]  # wrapping token used at /sys/wrapping/unwrap

    # 3) approle role name (role_id is read from vault using fetch token if allowed)
    approle_role_name: str

    # Background housekeeping (optional)
    enable_background_renewal: bool
    orphan_rotate_interval_s: int
    fetch_token_renew_interval_s: int

    @staticmethod
    def from_env() -> "VaultServiceConfig":
        address = resolve_vault_addr()
        namespace = (os.getenv("VAULT_NAMESPACE") or "").strip() or None
        verify = not _truthy(os.getenv("VAULT_SKIP_VERIFY", "false"))

        kv_mount = (os.getenv("VAULT_KV_MOUNT") or "secret").strip() or "secret"
        kv_version = int((os.getenv("VAULT_KV_VERSION") or "2").strip() or "2")
        if kv_version not in (1, 2):
            kv_version = 2

        cache_ttl_s = int((os.getenv("VAULT_CACHE_TTL") or "300").strip() or "300")

        orphan_token = (os.getenv("VAULT_ORPHAN_TOKEN") or "").strip() or None
        orphan_policies = _split_csv(os.getenv("VAULT_ORPHAN_POLICIES", "default"))
        orphan_period = (os.getenv("VAULT_ORPHAN_PERIOD") or "").strip() or None
        orphan_renewable = _truthy(os.getenv("VAULT_ORPHAN_RENEWABLE", "true"))

        wrapped_fetch_token = (os.getenv("VAULT_WRAPPED_FETCH_TOKEN") or "").strip() or None

        approle_role_name = (os.getenv("VAULT_APPROLE_ROLE_NAME") or "").strip()
        if not approle_role_name:
            raise SecretManagerError("VAULT_APPROLE_ROLE_NAME is required")

        enable_bg = _truthy(os.getenv("VAULT_ENABLE_BG_RENEWAL", "false"))
        orphan_rotate_interval_s = int((os.getenv("VAULT_ORPHAN_ROTATE_INTERVAL_S") or "3600").strip())
        fetch_renew_interval_s = int((os.getenv("VAULT_FETCH_TOKEN_RENEW_INTERVAL_S") or "3600").strip())

        return VaultServiceConfig(
            address=address,
            namespace=namespace,
            verify=verify,
            kv_mount=kv_mount,
            kv_version=kv_version,
            cache_ttl_s=cache_ttl_s,
            orphan_token=orphan_token,
            orphan_policies=orphan_policies,
            orphan_period=orphan_period,
            orphan_renewable=orphan_renewable,
            wrapped_fetch_token=wrapped_fetch_token,
            approle_role_name=approle_role_name,
            enable_background_renewal=enable_bg,
            orphan_rotate_interval_s=orphan_rotate_interval_s,
            fetch_token_renew_interval_s=fetch_renew_interval_s,
        )


# ----------------------------
# Service
# ----------------------------

class VaultSecretService:
    """
    Minimal, stable service that:
      - keeps an in-memory cache of KV secret VALUES
      - can (optionally) run background threads:
          * rotate orphan token (create-orphan)
          * renew fetch token (renew-self)
      - on demand:
          orphan token -> unwrap wrapped token -> fetch token -> (role_id + secret_id) -> client token -> read KV

    Notes:
      - This stores tokens in memory only (safer than constantly re-exporting env).
      - If your org requires persisting the rotated orphan token, do that in your deployment pipeline.
    """

    def __init__(
        self,
        *,
        cfg: VaultServiceConfig,
        client_factory: Callable[[str, Optional[str], bool], hvac.Client] = (
            lambda addr, ns, verify: hvac.Client(url=addr, namespace=ns, verify=verify)
        ),
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self._cfg = cfg
        self._client_factory = client_factory
        self._time = time_fn

        # Tokens held in memory (NOT env)
        self._orphan_token: Optional[str] = cfg.orphan_token
        self._fetch_token: Optional[str] = None
        self._client_token: Optional[str] = None

        # KV cache: (mount, path, version) -> (expires_at, data_dict)
        self._cache: Dict[Tuple[str, str, Optional[int]], Tuple[float, Dict[str, Any]]] = {}

        # threads
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []

        # One client used for reading secrets (client token set as needed)
        self._app_client = self._client_factory(cfg.address, cfg.namespace, cfg.verify)

        if cfg.enable_background_renewal:
            self._start_background_tasks()

    # ------------- lifecycle -------------

    def close(self) -> None:
        self._stop.set()
        for t in self._threads:
            if t.is_alive():
                t.join(timeout=2)

    # ------------- public API -------------

    def get_secret(
        self,
        path: str,
        *,
        key: Optional[str] = None,
        mount_point: Optional[str] = None,
        version: Optional[int] = None,
        raise_on_missing: bool = False,
    ) -> Any:
        """
        Read a KV secret with cache + auto-reauth on token problems.
        `path` is relative to mount, e.g. "dsecr/redis-creds"
        """
        mp = mount_point or self._cfg.kv_mount
        cache_key = (mp, path, version)
        now = self._time()

        if cache_key in self._cache:
            expires_at, data = self._cache[cache_key]
            if now < expires_at:
                return data.get(key) if key else data
            self._cache.pop(cache_key, None)

        # Ensure we have a valid client token
        self._ensure_client_token()

        try:
            data = self._read_kv(path=path, mount_point=mp, version=version)
        except VaultError as ve:
            # If it smells like auth, re-auth once and retry
            if looks_like_token_problem(ve):
                self._client_token = None
                self._ensure_client_token()
                try:
                    data = self._read_kv(path=path, mount_point=mp, version=version)
                except VaultError as ve2:
                    if raise_on_missing:
                        raise SecretManagerError(str(ve2)) from ve2
                    return None
            else:
                if raise_on_missing:
                    raise SecretManagerError(str(ve)) from ve
                return None

        self._cache[cache_key] = (now + self._cfg.cache_ttl_s, data)
        return data.get(key) if key else data

    def clear_cache(self) -> None:
        self._cache.clear()

    # ------------- auth chain -------------

    def _ensure_client_token(self) -> None:
        if self._client_token:
            # quick sanity check (optional). if it fails, we will reauth on read anyway.
            self._app_client.token = self._client_token
            return

        # Ensure fetch token exists (unwrap if needed)
        self._ensure_fetch_token()

        # Use fetch token to generate secret_id and read role_id, then login to get client token
        fetch_client = self._client_factory(self._cfg.address, self._cfg.namespace, self._cfg.verify)
        fetch_client.token = self._fetch_token

        # If allowed by policy, read role_id by role_name (so role_id not needed in env)
        try:
            role_id_resp = fetch_client.auth.approle.read_role_id(role_name=self._cfg.approle_role_name)
            role_id = role_id_resp["data"]["role_id"]
        except Exception as e:
            raise SecretManagerError(
                "Failed to read role_id. Policy may not allow read_role_id. "
                "Either grant capability or provide role_id via a secure injection path."
            ) from e

        sid_resp = fetch_client.auth.approle.generate_secret_id(role_name=self._cfg.approle_role_name)
        try:
            secret_id = sid_resp["data"]["secret_id"]
        except Exception as e:
            raise SecretManagerError(f"Unexpected secret_id response: {sid_resp}") from e

        login_resp = self._app_client.auth.approle.login(role_id=role_id, secret_id=secret_id)
        try:
            self._client_token = login_resp["auth"]["client_token"]
        except Exception as e:
            raise SecretManagerError(f"Unexpected AppRole login response: {login_resp}") from e

        self._app_client.token = self._client_token

    def _ensure_fetch_token(self) -> None:
        if self._fetch_token:
            return

        # unwrap wrapped fetch token using the orphan token (or any token that has unwrap permission)
        if not self._orphan_token:
            raise SecretManagerError("VAULT_ORPHAN_TOKEN is required (orphan token not loaded).")

        if not self._cfg.wrapped_fetch_token:
            raise SecretManagerError("VAULT_WRAPPED_FETCH_TOKEN is required to unwrap fetch token.")

        unwrap_client = self._client_factory(self._cfg.address, self._cfg.namespace, self._cfg.verify)
        unwrap_client.token = self._orphan_token

        # hvac has sys.unwrap(token=...) in many versions; to be robust, do raw POST if needed.
        try:
            unwrap_resp = unwrap_client.sys.unwrap(token=self._cfg.wrapped_fetch_token)  # type: ignore[attr-defined]
        except Exception:
            # fallback to raw API
            unwrap_resp = unwrap_client.adapter.post(
                url="v1/sys/wrapping/unwrap",
                json={"token": self._cfg.wrapped_fetch_token},
            )

        # unwrap response is typically {"auth": {"client_token": "..."}}
        try:
            self._fetch_token = unwrap_resp["auth"]["client_token"]
        except Exception as e:
            raise SecretManagerError(f"Unexpected unwrap response: {unwrap_resp}") from e

    # ------------- orphan token rotation / renewal -------------

    def rotate_orphan_token(self) -> str:
        """
        Create a new orphan token using the current orphan token.
        This is what your internal doc calls "renewing orphan token" via create-orphan.
        """
        if not self._orphan_token:
            raise SecretManagerError("No orphan token available to rotate (VAULT_ORPHAN_TOKEN).")

        c = self._client_factory(self._cfg.address, self._cfg.namespace, self._cfg.verify)
        c.token = self._orphan_token

        payload: Dict[str, Any] = {
            "policies": self._cfg.orphan_policies,
            "renewable": self._cfg.orphan_renewable,
        }
        if self._cfg.orphan_period:
            payload["period"] = self._cfg.orphan_period

        # Prefer hvac if available, else raw API.
        try:
            resp = c.auth.token.create_orphan(**payload)  # type: ignore[attr-defined]
        except Exception:
            resp = c.adapter.post(url="v1/auth/token/create-orphan", json=payload)

        try:
            new_token = resp["auth"]["client_token"]
        except Exception as e:
            raise SecretManagerError(f"Unexpected create-orphan response: {resp}") from e

        # Update in memory
        self._orphan_token = new_token
        return new_token

    def renew_fetch_token(self) -> None:
        """
        Best-effort renew of the fetch token (must be renewable).
        """
        if not self._fetch_token:
            return

        c = self._client_factory(self._cfg.address, self._cfg.namespace, self._cfg.verify)
        c.token = self._fetch_token
        c.renew_self_token()

    # ------------- KV read -------------

    def _read_kv(self, *, path: str, mount_point: str, version: Optional[int]) -> Dict[str, Any]:
        if self._cfg.kv_version == 2:
            try:
                resp = self._app_client.secrets.kv.v2.read_secret_version(
                    path=path,
                    mount_point=mount_point,
                    version=version,
                )
                return resp["data"]["data"]
            except InvalidPath:
                # If your mount is actually KV v1 or the path doesn't exist in v2 format
                pass

        resp = self._app_client.secrets.kv.v1.read_secret(path=path, mount_point=mount_point)
        return resp["data"]

    # ------------- background tasks -------------

    def _start_background_tasks(self) -> None:
        # rotate orphan token periodically
        def orphan_worker() -> None:
            interval = float(self._cfg.orphan_rotate_interval_s)
            while not self._stop.wait(interval):
                try:
                    self.rotate_orphan_token()
                except Exception:
                    # don't kill the thread; backoff
                    interval = min(interval * 2.0, 6 * 3600.0)
                else:
                    interval = float(self._cfg.orphan_rotate_interval_s)

        # renew fetch token periodically
        def fetch_worker() -> None:
            interval = float(self._cfg.fetch_token_renew_interval_s)
            while not self._stop.wait(interval):
                try:
                    # ensure fetch token exists first
                    self._ensure_fetch_token()
                    self.renew_fetch_token()
                except Exception:
                    interval = min(interval * 2.0, 6 * 3600.0)
                else:
                    interval = float(self._cfg.fetch_token_renew_interval_s)

        t1 = threading.Thread(target=orphan_worker, name="vault-orphan-rotate", daemon=True)
        t2 = threading.Thread(target=fetch_worker, name="vault-fetch-renew", daemon=True)
        t1.start()
        t2.start()
        self._threads.extend([t1, t2])


__all__ = ["VaultSecretService", "VaultServiceConfig", "SecretManagerError"]
'''
ENV=dev|uat|prod (or set VAULT_ADDR directly)

VAULT_NAMESPACE=ITID/10671283_DSAQW/ (from your UI screenshot)

VAULT_APPROLE_ROLE_NAME=monitoring-uat (example)

VAULT_ORPHAN_TOKEN=... (the long-lived token you rotate with create-orphan)

VAULT_WRAPPED_FETCH_TOKEN=... (the wrapping token you POST to /sys/wrapping/unwrap)

KV:

VAULT_KV_MOUNT=secrets/kv_v2 (your UI shows mount path like secrets/kv_v2)

VAULT_KV_VERSION=2

Recommended:

VAULT_SKIP_VERIFY=true (only if your internal TLS requires it; otherwise keep false)

VAULT_CACHE_TTL=300

Optional background renewal (if you want threads):

VAULT_ENABLE_BG_RENEWAL=true

VAULT_ORPHAN_POLICIES=ansible_approle_fetch_pol,default (whatever your doc requires)

VAULT_ORPHAN_PERIOD=168h (or whatever you use)

VAULT_ORPHAN_ROTATE_INTERVAL_S=3600

VAULT_FETCH_TOKEN_RENEW_INTERVAL_S=3600

from vault_secret_service import VaultSecretService, VaultServiceConfig

cfg = VaultServiceConfig.from_env()
svc = VaultSecretService(cfg=cfg)

# Read whole secret dict
data = svc.get_secret("dsecr/redis-creds")
print(data)

# Read a single key inside the secret (example: "password")
pw = svc.get_secret("dsecr/redis-creds", key="password")
print(pw)

svc.close()

/v1/ITID/10671283_DSAQW/secrets/kv_v2/data/dsecr/redis-creds
'''