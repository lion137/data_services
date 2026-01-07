from __future__ import annotations

import os
import hvac
from hvac.exceptions import VaultError


VAULT_ADDR = os.getenv("VAULT_ADDR", "https://vault-dev.uk.hsbc:8200")
NAMESPACE = "ITID/10671283_DSAQW"
KV_MOUNT = "secrets/kv_v2"          # <-- mount shown in UI
SECRET_PATH = "dsecr/redis-creds"   # <-- secret path shown in UI


def make_client(token: str, verify: bool = True) -> hvac.Client:
    """
    1:1 mapping to sending headers:
      X-Vault-Token: <token>
      X-Vault-Namespace: ITID/10671283_DSAQW
    """
    client = hvac.Client(
        url=VAULT_ADDR,
        token=token,
        namespace=NAMESPACE,
        verify=verify,
    )
    return client


def unwrap_if_needed(wrapping_token: str, verify: bool = True) -> dict:
    """
    HTTP:
      POST /v1/sys/wrapping/unwrap
      X-Vault-Token: <wrapping_token>
      X-Vault-Namespace: <namespace>

    hvac:
      client.sys.unwrap()
    """
    c = make_client(wrapping_token, verify=verify)
    return c.sys.unwrap()


def approle_create_secret_id(privileged_token: str, role_name: str, verify: bool = True) -> str:
    """
    HTTP:
      POST /v1/auth/approle/role/<role_name>/secret-id

    hvac:
      client.auth.approle.generate_secret_id()
    """
    c = make_client(privileged_token, verify=verify)

    # Equivalent to POST body {} (you can pass optional kwargs like ttl, num_uses, metadata)
    resp = c.auth.approle.generate_secret_id(
        role_name=role_name,
        # ttl="10m",
        # num_uses=1,
        # metadata={"issued_by": "hvac", "env": "dev"},
    )
    return resp["data"]["secret_id"]


def approle_read_role_id(privileged_token: str, role_name: str, verify: bool = True) -> str:
    """
    HTTP:
      GET /v1/auth/approle/role/<role_name>/role-id

    hvac:
      client.auth.approle.read_role_id()
    """
    c = make_client(privileged_token, verify=verify)
    resp = c.auth.approle.read_role_id(role_name=role_name)
    return resp["data"]["role_id"]


def approle_login(role_id: str, secret_id: str, verify: bool = True) -> str:
    """
    HTTP:
      POST /v1/auth/approle/login
      Body: { "role_id": "...", "secret_id": "..." }

    hvac:
      client.auth.approle.login()
    """
    c = hvac.Client(url=VAULT_ADDR, namespace=NAMESPACE, verify=verify)

    resp = c.auth.approle.login(role_id=role_id, secret_id=secret_id)
    return resp["auth"]["client_token"]


def kv2_read(app_token: str, verify: bool = True) -> dict:
    """
    HTTP (matches UI API path):
      GET /v1/ITID/10671283_DSAQW/secrets/kv_v2/data/dsecr/redis-creds

    hvac:
      client.secrets.kv.v2.read_secret_version(path=..., mount_point=...)
    """
    c = make_client(app_token, verify=verify)
    resp = c.secrets.kv.v2.read_secret_version(
        path=SECRET_PATH,
        mount_point=KV_MOUNT,
    )
    # KV v2 stores actual keys under resp["data"]["data"]
    return resp["data"]["data"]


def kv2_write(app_token: str, data: dict, verify: bool = True) -> None:
    """
    HTTP (matches UI API path):
      POST /v1/ITID/10671283_DSAQW/secrets/kv_v2/data/dsecr/redis-creds
      Body: { "data": { ... } }

    hvac:
      client.secrets.kv.v2.create_or_update_secret(path=..., mount_point=..., secret=data)
    """
    c = make_client(app_token, verify=verify)
    c.secrets.kv.v2.create_or_update_secret(
        path=SECRET_PATH,
        mount_point=KV_MOUNT,
        secret=data,  # hvac wraps this into {"data": ...} for KV v2
    )


if __name__ == "__main__":
    # ---- Fill these in ----
    # If you have a wrapped token, unwrap it first, then use the returned auth.client_token as PRIVILEGED_TOKEN.
    PRIVILEGED_TOKEN = os.getenv("VAULT_TOKEN", "")  # token that can create secret-id
    ROLE_NAME = os.getenv("VAULT_ROLE_NAME", "your-approle-name")

    # 1) Create secret_id + get role_id (needs privileged token)
    try:
        role_id = approle_read_role_id(PRIVILEGED_TOKEN, ROLE_NAME)
        secret_id = approle_create_secret_id(PRIVILEGED_TOKEN, ROLE_NAME)
    except VaultError as e:
        raise SystemExit(f"Failed to read role_id / create secret_id: {e}") from e

    # 2) Login with AppRole -> app token
    try:
        app_token = approle_login(role_id=role_id, secret_id=secret_id)
    except VaultError as e:
        raise SystemExit(f"AppRole login failed: {e}") from e

    # 3) Read KV secret (exact path from UI)
    try:
        secret_data = kv2_read(app_token)
        print("Read secret keys:", list(secret_data.keys()))
        print("Secret data:", secret_data)
    except VaultError as e:
        raise SystemExit(f"KV read failed: {e}") from e

    # 4) Optional: write/update secret
    # kv2_write(app_token, {"host": "redis.internal", "port": 6379, "password": "new-secret"})
    # print("Secret updated.")
