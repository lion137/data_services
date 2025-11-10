import unittest
from unittest.mock import MagicMock, Mock, patch
from functools import wraps

# --- anvil shim kept from your original test file ---
anvil = Mock()
DEFAULT_ANVIL = {
    "anvil": anvil,
    "anvil.users": MagicMock(),
    "anvil.server": MagicMock(),
    "anvil.secrets": MagicMock(),
    "anvil.saml.auth": MagicMock(),
    "Shared_Library.alerting": MagicMock(),
    "Shared_Library.proxies": MagicMock(),
    "server_code.mssql.sql_connector": MagicMock(),
    "azure.identity": MagicMock(),
    "databricks": MagicMock(),
    "databricks.sql.client": MagicMock(),
    "databricks.sql.types": MagicMock(),
    "pymemcache": MagicMock(),
    "pymemcache.client.base": MagicMock(),
}

# ---- System under test ----
# Note: we import inside each test after patching sys.modules so the shim is visible.
# from server_code.attestation import save_attestation, myself

# ==========
# Utilities
# ==========

def build_service(*, attested=True, service_id="service_1", db_id="db_service_1", ba_id="BA0001"):
    """Return a realistic Service mock with nested db_service and required attrs/methods."""
    service = MagicMock(name="ServiceMock")
    service.id = service_id
    service.attested = attested

    # nested db_service with id and ba_id (your code uses these)
    db_service = MagicMock(name="DBService")
    db_service.id = db_id
    db_service.ba_id = ba_id
    service.db_service = db_service

    # methods used later in save path
    service.save = MagicMock(name="service.save")
    service.delete = MagicMock(name="service.delete")

    return service


def build_db(*, attestation_urn=123):
    """
    Build a DB mock behaving like:
      with _db.connect() as conn:
          with conn.cursor() as cursor:
              cursor.fetchone() -> [attestation_urn]
    Returns (_db, conn, cursor).
    """
    _db = MagicMock(name="db_module")
    conn = MagicMock(name="conn")
    cursor = MagicMock(name="cursor")

    # Context manager plumbing
    _db.connect.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cursor

    # default fetchone() result – not strictly needed if _save_attestation_fn returns the URN
    cursor.fetchone.return_value = [attestation_urn]

    return _db


def build_deps(
    *,
    user_staff_id="12345",
    service=None,
    remediation=False,
    remediationReason=None,
    remediationCategory=None,
    attestation_urn=123,
):
    """
    Build a full set of DI dependencies for save_attestation().
    Individual pieces can be overridden via kwargs.
    Returns (deps_dict, internals_dict) so tests can assert on internals.
    """
    # service & DB
    service = service or build_service(attested=True)
    _db = build_db(attestation_urn=attestation_urn)

    # injectable functions
    _get_service = MagicMock(name="_get_service", return_value=service)
    _service_attested = MagicMock(name="_service_attested")
    _get_user = MagicMock(name="_get_user", return_value={"staff_id": user_staff_id, "email": "test@example.com"})
    _save_attestation_fn = MagicMock(name="_save_attestation_fn", return_value=attestation_urn)
    _save_server_estate_fn = MagicMock(name="_save_server_estate_fn")
    _save_connections_fn = MagicMock(name="_save_connections_fn")
    _save_missing_connections_fn = MagicMock(name="_save_missing_connections_fn")
    _save_shared_connections_fn = MagicMock(name="_save_shared_connections_fn")
    _info = MagicMock(name="_info")
    _error = MagicMock(name="_error")
    _send_alert = MagicMock(name="_send_alert")
    _get_new_service = MagicMock(name="_get_new_service", return_value=build_service(attested=False))
    _myself = MagicMock(name="_myself", return_value="save_attestation")
    _session = {}

    deps = dict(
        _get_service=_get_service,
        _service_attested=_service_attested,
        _db=_db,
        _save_attestation_fn=_save_attestation_fn,
        _save_server_estate_fn=_save_server_estate_fn,
        _save_connections_fn=_save_connections_fn,
        _save_missing_connections_fn=_save_missing_connections_fn,
        _save_shared_connections_fn=_save_shared_connections_fn,
        _get_user=_get_user,
        _info=_info,
        _error=_error,
        _send_alert=_send_alert,
        _get_new_service=_get_new_service,
        _myself=_myself,
        _session=_session,
    )

    internals = dict(
        service=service,
        db=_db,
        user_staff_id=user_staff_id,
        remediation=remediation,
        remediationReason=remediationReason,
        remediationCategory=remediationCategory,
        attestation_urn=attestation_urn,
    )

    return deps, internals


class TestAttestation(unittest.TestCase):
    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_save_attestation_success(self):
        # Import with anvil shim active
        from server_code.attestation import save_attestation

        deps, internals = build_deps()

        # execute
        save_attestation("test_service_id", **deps)

        # verify critical calls
        deps["_service_attested"].assert_called_once_with("test_service_id")
        deps["_get_user"].assert_called_once_with()
        deps["_get_service"].assert_called_once_with(service_id="test_service_id", staff_id=internals["user_staff_id"])
        deps["_save_attestation_fn"].assert_called_once()
        deps["_save_server_estate_fn"].assert_called_once()
        deps["_save_connections_fn"].assert_called_once()
        deps["_save_missing_connections_fn"].assert_called_once()
        deps["_save_shared_connections_fn"].assert_called_once()
        deps["_info"].assert_called()  # final info log after new_service.save()

        # sanity: new service created (confirmed but not attested)
        deps["_get_new_service"].assert_called_once()
        internals["service"].delete.assert_called_once()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_save_attestation_no_service_found(self):
        from server_code.attestation import save_attestation

        deps, internals = build_deps()
        deps["_get_service"].return_value = None  # simulate not found

        with self.assertRaises(ValueError) as ctx:
            save_attestation("test_service_id", **deps)

        self.assertEqual(str(ctx.exception), f"No services found for {internals['user_staff_id']}")

        # called up to the point of failure
        deps["_service_attested"].assert_called_once_with("test_service_id")
        deps["_get_user"].assert_called_once_with()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_save_attestation_with_remediation(self):
        from server_code.attestation import save_attestation

        deps, internals = build_deps(attestation_urn=456)
        # execute with remediation flags (non-default args)
        save_attestation(
            "test_service_id",
            remediation=True,
            remediationReason="Because reasons",
            remediationCategory=7,
            **deps,
        )

        # ensure remediation args flowed into insert fn
        # The SUT passes them as keyword args to _save_attestation_fn
        called_kwargs = deps["_save_attestation_fn"].call_args.kwargs
        self.assertEqual(called_kwargs["remediation"], True)
        self.assertEqual(called_kwargs["remediationReason"], "Because reasons")
        self.assertEqual(called_kwargs["remediationCategory"], 7)

        # and our URN made it through downstream save steps
        deps["_save_server_estate_fn"].assert_called()
        deps["_save_connections_fn"].assert_called()
        deps["_save_missing_connections_fn"].assert_called()
        deps["_save_shared_connections_fn"].assert_called()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_myself_helper(self):
        from server_code.attestation import myself

        # calling from here should return the function name that invoked it inside SUT;
        # our DI replaces _myself in other tests, but here we exercise the real helper.
        # We can at least assert it returns a string.
        result = myself()
        self.assertIsInstance(result, str)

