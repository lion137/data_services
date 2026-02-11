from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Protocol

import hvac
from hvac.exceptions import VaultError


class SecretManagerError(Exception):
    pass


class AuthProvider(Protocol):
    def ensure_auth(self, client: hvac.Client) -> None: ...
    def can_reauth(self) -> bool: ...


def _looks_like_token_problem(e: Exception) -> bool:
    msg = str(e).lower()
    return ("invalid token" in msg) or ("permission denied" in msg) or ("forbidden" in msg)


@dataclass(frozen=True)
class AppRoleLoginConfig:
    role_id: str
    role_name: str  # needed to generate secret_id
    kv_mount: str


class FetchTokenAppRoleAuth(AuthProvider):
    """
    Long-lived periodic 'fetch token' -> generate secret_id -> approle login -> short-lived client token.

    Responsibilities:
      - keep fetch token renewed (optional background thread)
      - on demand, generate a fresh secret_id and login to get a fresh client token
    """

    def __init__(
        self,
        *,
        fetch_token: str,
        role_name: str,
        role_id: str,
        namespace: Optional[str],
        verify: bool,
        vault_addr: str,
        # DI
        client_factory: Callable[[], hvac.Client],
        time_fn: Callable[[], float] = time.time,
        renew_interval_s: float = 3600.0,   # renew fetch token hourly by default
        enable_fetch_token_renew: bool = True,
    ) -> None:
        self._fetch_token = fetch_token
        self._role_name = role_name
        self._role_id = role_id
        self._time = time_fn
        self._renew_interval_s = renew_interval_s
        self._enable_fetch_token_renew = enable_fetch_token_renew

        self._client_factory = client_factory

        self._renew_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

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
                    # backoff, but don't die
                    interval = min(interval * 2, 6 * 3600)  # up to 6h
                else:
                    interval = self._renew_interval_s

        t = threading.Thread(target=worker, name="vault-fetch-token-renew", daemon=True)
        t.start()
        self._renew_thread = t

    def ensure_auth(self, client: hvac.Client) -> None:
        """
        Called when we need an app client token:
          1) use fetch token to generate secret_id
          2) login with role_id + secret_id
          3) set client.token
        """
        # 1) generate secret_id using fetch token
        fetch_client = self._client_factory()
        fetch_client.token = self._fetch_token

        sid_resp = fetch_client.auth.approle.generate_secret_id(role_name=self._role_name)
        try:
            secret_id = sid_resp["data"]["secret_id"]
        except Exception as e:
            raise SecretManagerError(f"Unexpected secret-id response: {sid_resp}") from e

        # 2) login (can use the same 'client' passed in)
        login_resp = client.auth.approle.login(role_id=self._role_id, secret_id=secret_id)
        try:
            client.token = login_resp["auth"]["client_token"]
        except Exception as e:
            raise SecretManagerError(f"Unexpected login response: {login_resp}") from e


