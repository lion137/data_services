# tests/test_attestation.py
import unittest
from unittest.mock import MagicMock, Mock, patch

# --- anvil shim (fake modules so imports succeed) ---
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

# ==========
# Test utils
# ==========

def build_service(*, attested=True, service_id="service_1", db_id="db_service_1", ba_id="BA0001"):
    """Realistic Service double with nested db_service + methods used by SUT."""
    svc = MagicMock(name="Service")
    svc.id = service_id
    svc.attested = attested

    db_svc = MagicMock(name="DBService")
    db_svc.id = db_id
    db_svc.ba_id = ba_id
    db_svc.ba_name = "BA-NAME"
    svc.db_service = db_svc

    svc.save = MagicMock(name="service.save")
    svc.delete = MagicMock(name="service.delete")
    return svc


def build_db(*, attestation_urn=123):
    """
    DB double that behaves like:

      with _db.connect() as conn:
          with conn.cursor() as cursor:
              cursor.fetchone() -> [attestation_urn]
    """
    _db = MagicMock(name="db_module")
    conn = MagicMock(name="conn")
    cursor = MagicMock(name="cursor")
    _db.connect.return_value.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cursor
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
    """Create a complete DI bundle for save_attestation()."""
    service = service or build_service(attested=True)
    _db = build_db(attestation_urn=attestation_urn)

    _get_service = MagicMock(name="_get_service", return_value=service)
    _service_attested = MagicMock(name="_service_attested")
    _get_user = MagicMock(name="_get_user", return_value={"staff_id": user_staff_id, "email": "dev@example.com"})
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


# =========================
# Happy-path + validation
# =========================

class TestAttestation(unittest.TestCase):
    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_save_attestation_success(self):
        from server_code.attestation import save_attestation

        deps, internals = build_deps()
        save_attestation("test_service_id", **deps)

        deps["_service_attested"].assert_called_once_with("test_service_id")
        deps["_get_user"].assert_called_once_with()
        deps["_get_service"].assert_called_once_with(service_id="test_service_id", staff_id=internals["user_staff_id"])
        deps["_save_attestation_fn"].assert_called_once()
        deps["_save_server_estate_fn"].assert_called_once()
        deps["_save_connections_fn"].assert_called_once()
        deps["_save_missing_connections_fn"].assert_called_once()
        deps["_save_shared_connections_fn"].assert_called_once()
        deps["_info"].assert_called()

        deps["_get_new_service"].assert_called_once()
        internals["service"].delete.assert_called_once()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_save_attestation_no_service_found(self):
        from server_code.attestation import save_attestation

        deps, internals = build_deps()
        deps["_get_service"].return_value = None

        with self.assertRaises(ValueError) as ctx:
            save_attestation("test_service_id", **deps)

        self.assertEqual(str(ctx.exception), f"No services found for {internals['user_staff_id']}")
        deps["_service_attested"].assert_called_once_with("test_service_id")
        deps["_get_user"].assert_called_once_with()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_save_attestation_with_remediation(self):
        from server_code.attestation import save_attestation

        deps, _ = build_deps(attestation_urn=456)
        save_attestation(
            "test_service_id",
            remediation=True,
            remediationReason="Because reasons",
            remediationCategory=7,
            **deps,
        )

        called_kwargs = deps["_save_attestation_fn"].call_args.kwargs
        self.assertEqual(called_kwargs["remediation"], True)
        self.assertEqual(called_kwargs["remediationReason"], "Because reasons")
        self.assertEqual(called_kwargs["remediationCategory"], 7)

        deps["_save_server_estate_fn"].assert_called()
        deps["_save_connections_fn"].assert_called()
        deps["_save_missing_connections_fn"].assert_called()
        deps["_save_shared_connections_fn"].assert_called()


# =========================
# Exception & edge branches
# =========================

class TestAttestationExceptions(unittest.TestCase):
    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_error_from_inner_helper_is_re_raised_without_double_logging(self):
        from server_code.attestation import save_attestation, SaveAttestationException

        deps, _ = build_deps()
        deps["_save_connections_fn"].side_effect = SaveAttestationException("boom")

        with self.assertRaises(SaveAttestationException):
            save_attestation("svc-1", **deps)

        deps["_error"].assert_not_called()
        deps["_send_alert"].assert_not_called()
        deps["_save_attestation_fn"].assert_called_once()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_generic_exception_triggers_top_level_logging_and_propagation(self):
        from server_code.attestation import save_attestation

        deps, _ = build_deps()
        deps["_save_attestation_fn"].side_effect = RuntimeError("db down")

        with self.assertRaises(RuntimeError):
            save_attestation("svc-2", **deps)

        deps["_error"].assert_called()
        deps["_send_alert"].assert_called()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_valueerror_from_insert_is_logged_and_propagated(self):
        from server_code.attestation import save_attestation

        deps, _ = build_deps()
        deps["_save_attestation_fn"].side_effect = ValueError("Invalid Owner ID")

        with self.assertRaises(ValueError) as ctx:
            save_attestation("svc-3", **deps)

        self.assertIn("Invalid Owner ID", str(ctx.exception))
        deps["_error"].assert_called()
        deps["_send_alert"].assert_called()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_not_attested_path_skips_db_and_downstream_saves(self):
        from server_code.attestation import save_attestation

        service = build_service(attested=False)
        deps, _ = build_deps(service=service)
        # keep attested False through the call
        deps["_service_attested"].side_effect = lambda _sid: None

        save_attestation("svc-4", **deps)

        deps["_db"].connect.assert_not_called()
        deps["_save_attestation_fn"].assert_not_called()
        deps["_save_server_estate_fn"].assert_not_called()
        deps["_save_connections_fn"].assert_not_called()
        deps["_save_missing_connections_fn"].assert_not_called()
        deps["_save_shared_connections_fn"].assert_not_called()

        service.delete.assert_not_called()
        deps["_get_new_service"].assert_not_called()
        deps["_info"].assert_not_called()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_session_cleanup_and_new_service_created_on_success(self):
        from server_code.attestation import save_attestation

        deps, internals = build_deps()
        ba_key = internals["service"].db_service.ba_id
        deps["_session"][ba_key] = {"connections": ["x"]}

        save_attestation("svc-5", **deps)

        self.assertNotIn(ba_key, deps["_session"])
        internals["service"].delete.assert_called_once()
        deps["_get_new_service"].assert_called_once()
        deps["_get_new_service"].return_value.save.assert_called_once()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_exception_from_save_server_estate_is_wrapped_and_re_raised(self):
        from server_code.attestation import save_attestation, SaveAttestationException

        deps, _ = build_deps()
        deps["_save_server_estate_fn"].side_effect = SaveAttestationException("estate failed")

        with self.assertRaises(SaveAttestationException):
            save_attestation("svc-6", **deps)

        deps["_error"].assert_not_called()
        deps["_send_alert"].assert_not_called()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_exception_from_missing_connections_is_wrapped_and_re_raised(self):
        from server_code.attestation import save_attestation, SaveAttestationException

        deps, _ = build_deps()
        deps["_save_missing_connections_fn"].side_effect = SaveAttestationException("missing failed")

        with self.assertRaises(SaveAttestationException):
            save_attestation("svc-7", **deps)

        deps["_error"].assert_not_called()
        deps["_send_alert"].assert_not_called()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_exception_from_shared_connections_is_wrapped_and_re_raised(self):
        from server_code.attestation import save_attestation, SaveAttestationException

        deps, _ = build_deps()
        deps["_save_shared_connections_fn"].side_effect = SaveAttestationException("shared failed")

        with self.assertRaises(SaveAttestationException):
            save_attestation("svc-8", **deps)

        deps["_error"].assert_not_called()
        deps["_send_alert"].assert_not_called()


if __name__ == "__main__":
    unittest.main()
