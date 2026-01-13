from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Callable, Tuple

import hvac
from hvac.exceptions import VaultError


class SecretManagerError(Exception):
    pass


def _truthy(v: str) -> bool:
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def resolve_vault_addr() -> str:
    explicit = (os.getenv("VAULT_ADDR") or "").strip()
    if explicit:
        return explicit
    env = (os.getenv("ENV") or "").strip().lower()
    if env not in {"dev", "uat", "prod"}:
        raise SecretManagerError("Set VAULT_ADDR or ENV (DEV/UAT/PROD)")
    return f"https://vault-{env}.uk.hsbc:8200"


def looks_like_auth_error(e: Exception) -> bool:
    msg = str(e).lower()
    return ("permission denied" in msg) or ("invalid token" in msg) or ("forbidden" in msg)


@dataclass(frozen=True)
class ClientConfig:
    addr: str
    namespace: Optional[str]
    verify: bool

    # Where we read the BASE token from (managed elsewhere)
    base_token_mount: str
    base_token_path: str
    base_token_key: str
    base_token_ttl_s: int

    # Where we read the FETCH token from (managed elsewhere)
    fetch_token_mount: str
    fetch_token_path: str
    fetch_token_key: str
    fetch_token_ttl_s: int

    # AppRole role name used to mint secret_id (+ role_id)
    approle_role_name: str

    # Where app secrets live
    secrets_mount: str
    secrets_kv_version: int

    # Cache TTL for client token (should be <= actual token ttl)
    client_token_ttl_s: int

    @staticmethod
    def from_env() -> "ClientConfig":
        addr = resolve_vault_addr()
        namespace = (os.getenv("VAULT_NAMESPACE") or "").strip() or None
        verify = not _truthy(os.getenv("VAULT_SKIP_VERIFY", "false"))

        # base token store
        base_token_mount = (os.getenv("VAULT_BASE_TOKEN_MOUNT") or "secret").strip()
        base_token_path = (os.getenv("VAULT_BASE_TOKEN_PATH") or "").strip()
        base_token_key = (os.getenv("VAULT_BASE_TOKEN_KEY") or "token").strip()
        base_token_ttl_s = int((os.getenv("VAULT_BASE_TOKEN_CACHE_TTL") or "300").strip())

        if not base_token_path:
            raise SecretManagerError("VAULT_BASE_TOKEN_PATH is required")

        # fetch token store
        fetch_token_mount = (os.getenv("VAULT_FETCH_TOKEN_MOUNT") or "secret").strip()
        fetch_token_path = (os.getenv("VAULT_FETCH_TOKEN_PATH") or "").strip()
        fetch_token_key = (os.getenv("VAULT_FETCH_TOKEN_KEY") or "token").strip()
        fetch_token_ttl_s = int((os.getenv("VAULT_FETCH_TOKEN_CACHE_TTL") or "300").strip())

        if not fetch_token_path:
            raise SecretManagerError("VAULT_FETCH_TOKEN_PATH is required")

        approle_role_name = (os.getenv("VAULT_APPROLE_ROLE_NAME") or "").strip()
        if not approle_role_name:
            raise SecretManagerError("VAULT_APPROLE_ROLE_NAME is required")

        secrets_mount = (os.getenv("VAULT_KV_MOUNT") or "secret").strip()
        secrets_kv_version = int((os.getenv("VAULT_KV_VERSION") or "2").strip() or "2")
        if secrets_kv_version not in (1, 2):
            secrets_kv_version = 2

        client_token_ttl_s = int((os.getenv("VAULT_CLIENT_TOKEN_CACHE_TTL") or "60").strip())

        return ClientConfig(
            addr=addr,
            namespace=namespace,
            verify=verify,
            base_token_mount=base_token_mount,
            base_token_path=base_token_path,
            base_token_key=base_token_key,
            base_token_ttl_s=base_token_ttl_s,
            fetch_token_mount=fetch_token_mount,
            fetch_token_path=fetch_token_path,
            fetch_token_key=fetch_token_key,
            fetch_token_ttl_s=fetch_token_ttl_s,
            approle_role_name=approle_role_name,
            secrets_mount=secrets_mount,
            secrets_kv_version=secrets_kv_version,
            client_token_ttl_s=client_token_ttl_s,
        )


class TTLCache:
    """
    Minimal TTL cache:
      key -> (expires_at_epoch, value)
    """
    def __init__(self, time_fn: Callable[[], float] = time.time) -> None:
        self._time = time_fn
        self._data: Dict[str, Tuple[float, str]] = {}

    def get(self, key: str) -> Optional[str]:
        item = self._data.get(key)
        if not item:
            return None
        exp, val = item
        if self._time() >= exp:
            self._data.pop(key, None)
            return None
        return val

    def set(self, key: str, val: str, ttl_s: int) -> None:
        self._data[key] = (self._time() + max(0, ttl_s), val)

    def invalidate(self, key: str) -> None:
        self._data.pop(key, None)


class VaultCredsClientCached:
    """
    Simplified but resilient:

    base token (read from Vault KV, cached) ->
      read fetch token (from Vault KV, cached) ->
        generate secret_id + read role_id ->
          approle login -> client token (cached) ->
            read secret

    On auth error:
      invalidate client token and retry once.
      if still failing, invalidate fetch token and retry once.
      if still failing, invalidate base token and stop (bootstrap issue).
    """

    def __init__(
        self,
        cfg: ClientConfig,
        client_factory: Optional[Callable[[], hvac.Client]] = None,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self._cfg = cfg
        self._client_factory = client_factory or (lambda: hvac.Client(
            url=cfg.addr,
            namespace=cfg.namespace,
            verify=cfg.verify,
        ))
        self._cache = TTLCache(time_fn=time_fn)

    # ---------- token fetchers (cached) ----------

    def _read_kv_v2(self, client: hvac.Client, mount: str, path: str) -> Dict[str, Any]:
        resp = client.secrets.kv.v2.read_secret_version(mount_point=mount, path=path)
        return resp["data"]["data"]

    def _get_base_token(self) -> str:
        cached = self._cache.get("base_token")
        if cached:
            return cached

        # Bootstrap token must exist for reading base token record
        bootstrap = (os.getenv("VAULT_BOOTSTRAP_TOKEN") or "").strip()
        if not bootstrap:
            raise SecretManagerError("VAULT_BOOTSTRAP_TOKEN is required to read base token record")

        c = self._client_factory()
        c.token = bootstrap

        data = self._read_kv_v2(c, self._cfg.base_token_mount, self._cfg.base_token_path)
        tok = data.get(self._cfg.base_token_key)
        if not tok:
            raise SecretManagerError(f"Base token record missing key '{self._cfg.base_token_key}'")

        tok = str(tok)
        self._cache.set("base_token", tok, self._cfg.base_token_ttl_s)
        return tok

    def _get_fetch_token(self) -> str:
        cached = self._cache.get("fetch_token")
        if cached:
            return cached

        base = self._get_base_token()
        c = self._client_factory()
        c.token = base

        data = self._read_kv_v2(c, self._cfg.fetch_token_mount, self._cfg.fetch_token_path)
        tok = data.get(self._cfg.fetch_token_key)
        if not tok:
            raise SecretManagerError(f"Fetch token record missing key '{self._cfg.fetch_token_key}'")

        tok = str(tok)
        self._cache.set("fetch_token", tok, self._cfg.fetch_token_ttl_s)
        return tok

    def _get_client_token(self) -> str:
        cached = self._cache.get("client_token")
        if cached:
            return cached

        fetch = self._get_fetch_token()
        c = self._client_factory()
        c.token = fetch

        # role_id (if allowed by fetch token policy)
        role_id_resp = c.auth.approle.read_role_id(role_name=self._cfg.approle_role_name)
        role_id = role_id_resp["data"]["role_id"]

        sid_resp = c.auth.approle.generate_secret_id(role_name=self._cfg.approle_role_name)
        secret_id = sid_resp["data"]["secret_id"]

        login = c.auth.approle.login(role_id=role_id, secret_id=secret_id)
        client_token = login["auth"]["client_token"]

        self._cache.set("client_token", client_token, self._cfg.client_token_ttl_s)
        return client_token

    # ---------- public secret read ----------

    def get_kv_secret(self, path: str) -> Dict[str, Any]:
        """
        Reads application secret from KV. Retries once on auth error.
        """
        def read_once() -> Dict[str, Any]:
            token = self._get_client_token()
            app = self._client_factory()
            app.token = token

            if self._cfg.secrets_kv_version == 2:
                resp = app.secrets.kv.v2.read_secret_version(
                    mount_point=self._cfg.secrets_mount,
                    path=path,
                )
                return resp["data"]["data"]

            resp = app.secrets.kv.v1.read_secret(
                mount_point=self._cfg.secrets_mount,
                path=path,
            )
            return resp["data"]

        try:
            return read_once()
        except VaultError as ve:
            if not looks_like_auth_error(ve):
                raise

            # Step 1: invalidate client token and retry
            self._cache.invalidate("client_token")
            try:
                return read_once()
            except VaultError as ve2:
                if not looks_like_auth_error(ve2):
                    raise

                # Step 2: invalidate fetch token (forces refetch) + client token and retry
                self._cache.invalidate("fetch_token")
                self._cache.invalidate("client_token")
                try:
                    return read_once()
                except VaultError as ve3:
                    # Step 3: bootstrap/base token likely bad or permissions changed
                    self._cache.invalidate("base_token")
                    raise
'''
export ENV=DEV
export VAULT_NAMESPACE="ITID/10671283_DSAQW"
export VAULT_SKIP_VERIFY=false

# bootstrap token only needs READ on base token KV path
export VAULT_BOOTSTRAP_TOKEN="..."

# base token record in Vault KV (written by your token microservice)
export VAULT_BASE_TOKEN_MOUNT="secrets/kv_v2"
export VAULT_BASE_TOKEN_PATH="system/vault/base-token"
export VAULT_BASE_TOKEN_KEY="token"
export VAULT_BASE_TOKEN_CACHE_TTL=300

# fetch token record in Vault KV (written by your token microservice)
export VAULT_FETCH_TOKEN_MOUNT="secrets/kv_v2"
export VAULT_FETCH_TOKEN_PATH="system/vault/fetch-token"
export VAULT_FETCH_TOKEN_KEY="token"
export VAULT_FETCH_TOKEN_CACHE_TTL=300

# approle role used to mint secret_id
export VAULT_APPROLE_ROLE_NAME="monitoring-uat"

# where your app secrets are
export VAULT_KV_MOUNT="secrets/kv_v2"
export VAULT_KV_VERSION=2

# short-lived cache for client tokens
export VAULT_CLIENT_TOKEN_CACHE_TTL=60


from vault_creds_client_cached import ClientConfig, VaultCredsClientCached

cfg = ClientConfig.from_env()
vc = VaultCredsClientCached(cfg)

vc.get_kv_secret("dsecr/redis-creds")
'''