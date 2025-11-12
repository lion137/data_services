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


def make_cursor(*, fetch=[999]):
    cur = MagicMock(name="cursor")
    cur.execute = MagicMock()
    cur.fetchone = MagicMock(return_value=fetch)
    return cur


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
# Exception & edge branches (public)
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
        # reflect contract: it returns True or raises; service.attested stays False
        deps["_service_attested"].return_value = True

        save_attestation("svc-4", **deps)

        deps["_db"].connect.assert_not_called()
        deps["_save_attestation_fn"].assert_not_called()
        deps["_save_server_estate_fn"].assert_not_called()
        deps["_save_connections_fn"].assert_not_called()
        deps["_save_missing_connections_fn"].assert_not_called()
        deps["_save_shared_connections_fn"].assert_not_called()
        service.delete.assert_not_called()
        deps["_get_new_service"].assert_not_called()

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


# =========================
# Private helpers coverage
# =========================

class TestPrivateHelpers(unittest.TestCase):
    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test__save_attestation_success_and_background_task(self):
        import server_code.attestation as mod
        cursor = make_cursor(fetch=[777])
        _error = MagicMock()
        _send_alert = MagicMock()

        urn = mod._save_attestation(
            staff_id="U123",
            service=build_service(),
            cursor=cursor,
            remediation=False,
            remediationReason=None,
            remediationCategory=None,
            _error=_error,
            _send_alert=_send_alert,
            _myself=lambda: "test",
        )

        self.assertEqual(urn, 777)
        cursor.execute.assert_called()
        anvil.server.launch_background_task.assert_called()
        _error.assert_not_called()
        _send_alert.assert_not_called()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test__save_attestation_validations_raise_valueerror(self):
        import server_code.attestation as mod
        cursor = make_cursor()

        # invalid staff id
        with self.assertRaises(ValueError):
            mod._save_attestation(
                staff_id=" ",
                service=build_service(),
                cursor=cursor,
                remediation=False,
                remediationReason=None,
                remediationCategory=None,
                _error=MagicMock(),
                _send_alert=MagicMock(),
                _myself=lambda: "x",
            )

        # remediation requires reason & category
        with self.assertRaises(ValueError):
            mod._save_attestation(
                staff_id="U1",
                service=build_service(),
                cursor=cursor,
                remediation=True,
                remediationReason=None,
                remediationCategory=None,
                _error=MagicMock(),
                _send_alert=MagicMock(),
                _myself=lambda: "x",
            )

        # invalid BA info
        bad = build_service()
        bad.db_service.ba_id = None
        with self.assertRaises(ValueError):
            mod._save_attestation(
                staff_id="U1",
                service=bad,
                cursor=cursor,
                remediation=False,
                remediationReason=None,
                remediationCategory=None,
                _error=MagicMock(),
                _send_alert=MagicMock(),
                _myself=lambda: "x",
            )

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test__save_attestation_insert_failure_logs_and_wraps(self):
        import server_code.attestation as mod
        cursor = make_cursor()
        cursor.execute.side_effect = Exception("sql blew up")
        _error = MagicMock()
        _send_alert = MagicMock()

        with self.assertRaises(mod.SaveAttestationException):
            mod._save_attestation(
                staff_id="U1",
                service=build_service(),
                cursor=cursor,
                remediation=False,
                remediationReason=None,
                remediationCategory=None,
                _error=_error,
                _send_alert=_send_alert,
                _myself=lambda: "here",
            )
        _error.assert_called()
        _send_alert.assert_called()

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test__save_connections_success_and_empty_shortcut(self):
        import server_code.attestation as mod
        cursor = make_cursor()
        _error, _send_alert = MagicMock(), MagicMock()

        # empty → return early
        with patch.object(mod, "Connection") as Conn:
            Conn.searchAll.return_value = []
            mod._save_connections(build_service(), 111, cursor, _error=_error, _send_alert=_send_alert, _myself=lambda:"m")
            cursor.execute.assert_not_called()

        # non-empty → insert rows
        cursor = make_cursor()
        c1 = MagicMock(direction="inbound", required="Yes", reason="why")
        c1.db_service = MagicMock(ba_name="BA1", ba_id="BA001")
        c2 = MagicMock(direction="outbound", required="No", reason=None)
        c2.db_service = MagicMock(ba_name="BA2", ba_id="BA002")
        with patch.object(mod, "Connection") as Conn:
            Conn.searchAll.return_value = [c1, c2]
            mod._save_connections(build_service(), 222, cursor, _error=_error, _send_alert=_send_alert, _myself=lambda:"m")
            self.assertEqual(cursor.execute.call_count, 2)

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test__save_connections_failure_wraps(self):
        import server_code.attestation as mod
        cursor = make_cursor()
        _error, _send_alert = MagicMock(), MagicMock()

        c = MagicMock(direction="inbound", required="Yes", reason="why")
        c.db_service = MagicMock(ba_name="BA", ba_id="BA1")
        with patch.object(mod, "Connection") as Conn:
            Conn.searchAll.return_value = [c]
            cursor.execute.side_effect = Exception("nope")
            with self.assertRaises(mod.SaveAttestationException):
                mod._save_connections(build_service(), 333, cursor, _error=_error, _send_alert=_send_alert, _myself=lambda:"m")

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test__save_server_estate_success_and_failure(self):
        import server_code.attestation as mod
        _error, _send_alert = MagicMock(), MagicMock()

        # success
        s1 = MagicMock(hostname="h1", environment="prod", lifecycle="live", shared_status="dedicated")
        s2 = MagicMock(hostname="h2", environment="dev", lifecycle="test", shared_status="shared")
        with patch.object(mod, "Server") as Srv:
            Srv.searchAll.return_value = [s1, s2]
            cursor = make_cursor()
            mod._save_server_estate(build_service(), 444, cursor, _error=_error, _send_alert=_send_alert, _myself=lambda:"m")
            self.assertEqual(cursor.execute.call_count, 2)

        # failure
        with patch.object(mod, "Server") as Srv:
            Srv.searchAll.return_value = [s1]
            cursor = make_cursor()
            cursor.execute.side_effect = Exception("estate bad")
            with self.assertRaises(mod.SaveAttestationException):
                mod._save_server_estate(build_service(), 555, cursor, _error=_error, _send_alert=_send_alert, _myself=lambda:"m")

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test__save_missing_connections_success_both_dirs(self):
        import server_code.attestation as mod
        _error, _send_alert = MagicMock(), MagicMock()
        cursor = make_cursor(fetch=[900])  # first fetchone → Issue URN

        m_in = MagicMock(inbound=True, outbound=False, reason="need inbound")
        m_in.db_service = MagicMock(ba_name="BAX", ba_id="BA10")
        m_out = MagicMock(inbound=False, outbound=True, reason="need outbound")
        m_out.db_service = MagicMock(ba_name="BAY", ba_id="BA11")

        with patch.object(mod, "MissingConnection") as MC:
            MC.searchAll.return_value = [m_in, m_out]
            mod._save_missing_connections(build_service(), 888, cursor, _error=_error, _send_alert=_send_alert, _myself=lambda:"m")
            # 1 insert into Issue + 2 inserts into Connection
            self.assertGreaterEqual(cursor.execute.call_count, 3)

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test__save_missing_connections_failure_wraps(self):
        import server_code.attestation as mod
        _error, _send_alert = MagicMock(), MagicMock()
        cursor = make_cursor(fetch=[901])
        m = MagicMock(inbound=True, outbound=False, reason="x")
        m.db_service = MagicMock(ba_name="BA", ba_id="BA1")
        with patch.object(mod, "MissingConnection") as MC:
            MC.searchAll.return_value = [m]
            cursor.execute.side_effect = Exception("insert fail")
            with self.assertRaises(mod.SaveAttestationException):
                mod._save_missing_connections(build_service(), 777, cursor, _error=_error, _send_alert=_send_alert, _myself=lambda:"m")

    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test__save_shared_connections_success_and_failure(self):
        import server_code.attestation as mod
        _error, _send_alert = MagicMock(), MagicMock()

        # success
        s1 = MagicMock(direction="inbound", required="Yes", reason="r1", host="h1")
        s1.db_service = MagicMock(ba_name="BA1", ba_id="B1")
        s2 = MagicMock(direction="outbound", required="No", reason=None, host="h2")
        s2.db_service = MagicMock(ba_name="BA2", ba_id="B2")
        with patch.object(mod, "SharedConnection") as SC:
            SC.searchAll.return_value = [s1, s2]
            cursor = make_cursor()
            mod._save_shared_connections(build_service(), 999, cursor, _error=_error, _send_alert=_send_alert, _myself=lambda:"m")
            self.assertEqual(cursor.execute.call_count, 2)

        # failure
        with patch.object(mod, "SharedConnection") as SC:
            SC.searchAll.return_value = [s1]
            cursor = make_cursor()
            cursor.execute.side_effect = Exception("shared bad")
            with self.assertRaises(mod.SaveAttestationException):
                mod._save_shared_connections(build_service(), 1000, cursor, _error=_error, _send_alert=_send_alert, _myself=lambda:"m")


# =========================
# Tiny unit for debug helper
# =========================

class TestMyselfHelper(unittest.TestCase):
    @patch.dict("sys.modules", DEFAULT_ANVIL)
    def test_myself_returns_caller_name(self):
        from server_code.attestation import myself
        def wrapper():
            return myself()
        self.assertEqual(wrapper(), "wrapper")


if __name__ == "__main__":
    unittest.main()
