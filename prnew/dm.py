import inspect
from datetime import import datetime

import anvil.server

try:
    from psycopg2.errors import UniqueViolation as _UniqueViolation
    if not isinstance(_UniqueViolation, type) or not issubclass(
        _UniqueViolation, BaseException
    ):
        class UniqueViolation(Exception):
            pass
else:
    UniqueViolation = _UniqueViolation
except Exception:
    class UniqueViolation(Exception):
        pass


from .databricks_api import (
    get_business_applications_from_databricks, get_connection_data_from_databricks
)
from .logger import error, warning
from .make_mermaid import make_full_mermaid, make_mermaid_connections, make_shared_mermaid
from . import globals as pg
from .pg.database.database import Database as PgDatabase
from .pg.database.exceptions import (
    DoesNotExistException, MoreThanOneException, NothingToUpdateException
)
from .pg.models.models import (
    Attestation,
    BowTask,
    Connection,
    DatabricksService,
    MissingConnection,
    Server,
    Service,
    SharedConnection,
    SingleSidedConnection
)
from .utilities.alerts import send_alert
from .utilities.users import get_user

MODULE_NAME = __name__


def myself():
    return inspect.stack()[1][3]


if pg.db is None:
    pg.db = PgDatabase.createDb()  # pragma: no cover


def _extract_business_services(data: list[dict]) -> list[dict]:
    if len(data) == 1:
        data = data[0]
    elif len(data) == 0:
        return data
    else:
        raise ValueError("Invalid data format")

    business_services = data.get("business_services", [])
    return business_services


@anvil.server.callable()
def update_attested_service_confirmation_status(
    dbServiceId: str, confirmation_status: str
) -> None:
    """this is used on the first page, and updates the confirmation
    status of attested rows based on user selection"""
    user = get_user()
    if not user:
        msg = "No user found. Session likely expired."
        warning(msg, MODULE_NAME, myself())
        raise ValueError(msg)
    try:
        db_service = DatabricksService.searchOne(id=dbServiceId)
        attested_service = Service.searchOne(
            service_id=db_service.id, staff_id=user["staff_id"]
        )
        attested_service.update({"confirmed": confirmation_status})
    except DoesNotExistException:
        msg = f"""Service could not be confirmed. The row was
        most likely marked as demised and removed from the database.
        Please review Databricks Service: {dbServiceId}, user {user}"""
        warning(msg, MODULE_NAME, myself())
        send_alert(f"{MODULE_NAME}.{myself()}", msg, "WARNING")
        raise


@anvil.server.callable()
def get_confirmed_services() -> list[dict]:
    """returns the list of confirmed services where the user has answered
    'Yes' they are the ITSO."""
    current_user = get_user()
    current_user_staff_id = current_user["staff_id"]
    databricks_data = get_business_applications_from_databricks()
    attestable_services = _extract_business_services(databricks_data)

    next_attestation_map = {
        service["ba_id"]: service.get("next_attestation_date")
        for service in attestable_services
    }

    services_from_cache = Service.searchAll(
        staff_id=current_user_staff_id, confirmed=True
    )
    serialised_services = Service.serialiseAll(services_from_cache)
    services_with_dates = _get_last_and_next_attestation_for_services(
        serialised_services, next_attestation_map
    )

    return services_with_dates


def _get_last_and_next_attestation_for_services(
    data: list[dict], next_attestation_map: dict
) -> list[dict]:
    """Enriches service data with last attestation and due
    date information from Databricks."""
    if not data:
        return data

    for service in data:
        ba_id = service["db_service"]["ba_id"]
        if last := Attestation.last(ba_id):
            next_attestation_str = next_attestation_map.get(ba_id)
            next_attestation_date = None
            service["last_attestation"] = last.submitted_at
            try:
                next_attestation_date = datetime.fromisoformat(
                    next_attestation_str.replace("+00:00", "")
                )
            except (ValueError, AttributeError):
                next_attestation_date = None
            else:
                service["due_date"] = next_attestation_date
                service["last_attestation"] = None

    return data


def _get_last_attested_connections(ba_id: str) -> list[dict]:
    if not ba_id:
        return

    sql = """
SELECT DISTINCT '' as Host, ServiceId, Direction, Required, ReasonRequired, 'shared' as Type
FROM [upp.Connection] WHERE AttestationURN = (
SELECT TOP (1) URN FROM [dbo].[upp.Attestation]
WHERE BusinessAppId = ?
ORDER BY [Created] DESC )
UNION ALL
SELECT DISTINCT Host, ServiceId, Direction, Required, ReasonRequired, 'shared' as Type
FROM [upp.SharedConnection] WHERE AttestationURN = (
SELECT TOP (1) URN FROM [dbo].[upp.Attestation]
WHERE BusinessAppId = ?
ORDER BY [Created] DESC )
"""

    with mssql.db.connect() as conn:
        with conn.cursor() as cursor:
            params = (ba_id, ba_id)
            cursor.execute(sql, params)
            fields = [field_md[0] for field_md in cursor.description]
            res = [dict(zip(fields, row)) for row in cursor.fetchall()]

    return res


def _get_last_attested_shared_connections(ba_id: str) -> list[dict]:
    if not ba_id:
        return

    sql = """
SELECT DISTINCT Host, ServiceId, Direction, Required, ReasonRequired, 'shared' as Type
FROM [upp.SharedConnection] WHERE AttestationURN = (
SELECT TOP (1) URN FROM [dbo].[upp.Attestation]
WHERE BusinessAppId = ?
ORDER BY [Created] DESC )
"""

    with mssql.db.connect() as conn:
        with conn.cursor() as cursor:
            params = ba_id
            cursor.execute(sql, params)
            fields = [field_md[0] for field_md in cursor.description]
            res = [dict(zip(fields, row)) for row in cursor.fetchall()]

    return res


def _check_shared_connection_reason(connections: list[dict]):
    if connections:
        if connections.get("shared_connections"):
            with_reasons = _get_last_attested_shared_connections(connections["busapp_id"])
            for item in connections["shared_connections"]:
                for res in with_reasons:
                    if str(item["host"]) and str(item["direction"]) == res["Direction"]:
                        item["reason"] = res["ReasonRequired"]
                        item["required"] = None


def canRemediateService(serviceId: str) -> bool:
    tasks = BowTask.searchAll(orderClause="databricks_id DESC LIMIT 1", service_id=serviceId)
    if len(tasks) > 0 and tasks[0].key_used:
        return False
    else:
        return True


@anvil.server.callable()
def load_services_data_from_databricks() -> list[dict]:
    """load the services from databricks for the current user and save them to cache"""
    current_user = get_user()
    current_user_email = current_user["email"]
    current_user_staff_id = current_user["staff_id"]

    data = get_business_applications_from_databricks(current_user_staff_id)

    # extract and validate business services
    attestable_services = _extract_business_services(data)

    services_ids = set()

    # make sure all the services are added to cache
    for row in attestable_services:
        services_ids.add(row["ba_id"])
        try:
            db_service = DatabricksService.searchOne(active=True, ba_id=row["ba_id"])
        except DoesNotExistException:
            # this service was not in the cache, so add it
            attested_service = Service(
                service_id=db_service.id, confirmed=False,
                staff_id=current_user_staff_id, attested=False
            )
            attested_service.save()

    # Clean up services that are no longer valid
    services_in_cache = Service.searchAll(staff_id=current_user_staff_id)
    valid_services = []

    for row in services_in_cache:
        should_delete = False
        db_service = row.db_service

        if not db_service:
            # no db service - should be deleted
            should_delete = True
            try:
                row.delete()
            except Exception as e:
                msg = f"Row could not be deleted. error: {e}, user:\
                {current_user_email} {current_user_staff_id} {current_user_email}"
                warning(msg, MODULE_NAME, myself())
                send_alert(f"{MODULE_NAME}.{myself()}", msg, "MAJOR")

        elif db_service.ba_id not in services_ids:
            # Service has been removed from databricks
            should_delete = True
            try:
                row.delete()
            except Exception as e:
                msg = f"Row could not be deleted. error: {e}, Service: \
                {db_service.ba_name} user: {current_user_staff_id} {current_user_email}"
                warning(msg, MODULE_NAME, myself())
                send_alert(f"{MODULE_NAME}.{myself()}", msg, "MAJOR")

        elif not db_service.active:
            # This service was marked as inactive while user was attesting it,
            # we do not attest to demised services.
            should_delete = True
            try:
                row.delete()
            except Exception as e:
                msg = f"Row could not be deleted. error: {e}, Service: {db_service.ba_id}\
                {db_service.ba_name} user: {current_user_staff_id} {current_user_email}"
                warning(msg, MODULE_NAME, myself())
                send_alert(f"{MODULE_NAME}.{myself()}", msg, "MAJOR")

        # Keep track of services that weren't deleted
        if not should_delete:
            valid_services.append(row)

    # Serialize only the valid services (optimization: no second database query)
    services = Service.serialiseAll(valid_services)

    # Since we cannot effectively call this function from the client side, we do it here
    for s in services:
        s["canRemediate"] = canRemediateService(s["service_id"])

    return services


@anvil.server.callable()
def mark_service_as_attested(dbServiceId: str) -> bool:
    user = get_user()
    staff_id = user["staff_id"]
    service_attested = Service.searchOne(service_id=dbServiceId, staff_id=staff_id)
    service_attested.update({"attested": True})
    return True


@anvil.server.callable(require_user=True)
def save_all_estate(server_estate: list[dict], attested_service_id: str) -> None:
    """saves estate"""
    for server in server_estate:
        hostname = server["o_host"]
        Environment = server["o_environment"]
        Lifecycle = server["o_lifecyclestate"]
        SharedStatus = server["o_o_shared_server"]
        add_server_estate(attested_service_id, hostname, Environment, Lifecycle, SharedStatus)


def add_server_estate(
    connection_ba_id: int, hostname: str, environment: str, lifecycle: str, sharedStatus: str
) -> None:
    """Add server estate to postgres database"""
    assert connection_ba_id
    assert hostname

    user = get_user()
    staff_id = user["staff_id"]

    connection_service = DatabricksService.searchOne(ba_id=connection_ba_id)
    attested_service = Service.searchOne(
        service_id=connection_service.id, staff_id=staff_id
    )

    try:
        Server.searchOne(
            attested_service_id=attested_service.id, hostname=hostname,
            environment=environment, lifecycle=lifecycle, shared_status=sharedStatus
        )
    except DoesNotExistException:
        try:
            server = Server(
                attested_service_id=attested_service.id, hostname=hostname,
                environment=environment, lifecycle=lifecycle, shared_status=sharedStatus
            )
            server.save()
            return True
        except Exception as e:
            msg = f"Could not save server estate of {connection_ba_id} to Postgres DB due to: {e}"
            error(msg, MODULE_NAME, myself())
            send_alert(f"{MODULE_NAME}.{myself()}", msg, "MAJOR")
            return False
    else:
        # server estate already existed in cache
        return True


@anvil.server.callable()
# TODO: Later when we can join PyArrow dataframe(s)
def get_server_estate(service_id: str) -> list[dict]:
    """Return the fetched server estate from databricks at the beginning of user session."""

    # Uses session caching to avoid repeated DataService queries.
    """
    if service_id not in anvil.server.session:
        connections = get_connection_data_from_databricks(service_id)
        connections = _validate_connections(connections)
        anvil.server.session[service_id] = connections

    if anvil.server.session.get(service_id):
        return anvil.server.session.get(service_id)["server_estate"]
    else:
        return None


@anvil.server.callable(require_user=True)
def getSingleSidedConnections(ba_id: str, staffId: str) -> list[dict]:
    res = SingleSidedConnection.connections(ba_id, staffId)
    return res


@anvil.server.callable(require_user=True)
def addSingleSidedConnections(
    serviceId: str, staff_id: str, connections: list[dict]
) -> None:
    for connection in connections:
        if connId := connection.get("id"):
            # Existing connection
            values = dict(required=connection["required"], reason=connection["reason"])
            SingleSidedConnection.updateWhere(values, f"id = {connId}")
        else:
            # New connection
            ds = DatabricksService.searchOne(ba_id=serviceId)
            service = Service.searchOne(service_id=ds.id, staff_id=staff_id)
            c = SingleSidedConnection(
                attested_service_id=service.id,
                direction=connection["direction"],
                type=connection["type"],
                connection=connection["connection"],
                required=connection["required"],
                reason=connection["reason"],
            )

            c.save()


@anvil.server.callable(require_user=True)
def get_connections(service_id: str) -> dict:
    """return the fetched connections from databricks at the beginning of user session"""

    # if service_id not in anvil.server.session:
    if service_id not in anvil.server.session:
        data = get_connection_data_from_databricks(service_id)
        connections = _validate_connections(data)
        _check_shared_connection_reason(connections)
        anvil.server.session[service_id] = connections

    return anvil.server.session.get(service_id)


@anvil.server.callable()
def get_confirmed_connections(service_id: str) -> list[dict]:
    """return the connections that the user said Yes to"""

    res = []
    connections = get_connections(service_id)

    if connections:
        for row in connections.get("connections", []):
            if row.get("required") == "Yes":
                row["type"] = "dedicated"
                res.append(row)

        for row in connections.get("shared_connections", []):
            connection_direction = row.get("direction")
            connection_reason = row.get("reason")
            if row.get("required") == "Yes":
                for service in row["services"]:
                    if service.get("selected"):
                        shared_row = {
                            "direction": connection_direction,
                            "type": "shared",
                            "busapp_id": service["busapp_id"],
                            "busapp_name": service["busapp_name"],
                            "busapp_description": service["busapp_description"],
                            "data_interface": service.get("data_interface"),
                            "reason": connection_reason,
                            "destination": service["destination"],
                        }
                        res.append(shared_row)

    ssc = SingleSidedConnection.summaryConnections(ba_id=service_id, onlyYes=True)
    for c in ssc:
        connection = {"type": "single-sided", "destination": ""} | c
        res.append(connection)

    return res


@anvil.server.callable()
def get_all_connections(
    service_id: str, _get_last_attested_connections_fn=None
) -> list[dict]:
    """
    Aggregates all connection types (dedicated, shared, missing) for a given service
    and enriches them with historical data from the last attestation.

    Logic breakdown:
    1. Fetches current connections, last attestation data, and a list of missing connections.
    2. Creates a lookup map (dictionary) of historical reasons for fast access.
    3. Processes dedicated connections.
    4. "Unpacks" shared connections, creating a separate entry for each service.
    5. Fills in 'reason' fields in the processed connections using the map.
    6. Adds missing connections to the final list.
    7. Returns a single, complete list of all connections.

    Args:
        service_id (str): The identifier (BA_ID) of the service for which data is
            being fetched.

    Returns:
        list[dict]: A complete, "flattened" list of connections.
    """
    if _get_last_attested_connections_fn is None:
        _get_last_attested_connections_fn = _get_last_attested_connections

    all_connections = []
    connections = get_connections(service_id)
    last_attested_connections = _get_last_attested_connections_fn(service_id)
    missing = get_missing_connections(service_id)

    attested_reasons_map = {}
    if last_attested_connections:
        for attested_conn in last_attested_connections:
            key = (
                attested_conn.get("ServiceId"),
                attested_conn.get("Direction"),
                attested_conn.get("type"),
            )
            attested_reasons_map[key] = attested_conn.get("ReasonRequired")

    if connections:
        if connections.get("connections"):
            for row in connections.get("connections"):
                row["type"] = "dedicated"
                all_connections.append(row)

        if connections.get("shared_connections"):
            for group_row in connections.get("shared_connections"):
                host = group_row.get("host")
                connection_direction = group_row.get("direction")
                connection_reason = group_row.get("reason")
                connection_required = group_row.get("required")

                for service in group_row.get("services", []):
                    shared_row = {
                        "host": host,
                        "direction": connection_direction,
                        "type": "shared",
                        "busapp_id": service.get("busapp_id"),
                        "busapp_name": service.get("busapp_name"),
                        "busapp_description": service.get("busapp_description"),
                        "data_interface": service.get("data_interface"),
                        "reason": connection_reason,
                        "destination": service.get("destination"),
                    }
                    if connection_required == "Yes" and not service.get("selected"):
                        shared_row["required"] = "No"
                    else:
                        shared_row["required"] = connection_required

                    all_connections.append(shared_row)

    for row in all_connections:
        if not row.get("reason"):
            lookup_key = (
                row.get("busapp_id"),
                row.get("direction"),
                row.get("type"),
            )
            if lookup_key in attested_reasons_map:
                row["reason"] = attested_reasons_map[lookup_key]

    if missing:
        for row in missing:
            row["type"] = "missing"
            row["required"] = "Yes"
            all_connections.append(row)

    ssc = SingleSidedConnection.summaryConnections(ba_id=service_id)
    for c in ssc:
        connection = {"type": "single-sided", "destination": ""} | c
        all_connections.append(connection)

    return all_connections


def _validate_connections(data: list[dict]) -> dict:
    """
    Validate and transform connection data from Databricks into frontend-ready format.

    Args:
        data: List containing a single dict with connection data from Databricks.

    Returns:
        Dictionary with validated and formatted connection data including:
        - busapp_name, attestation_type, description
        - server_estate, connections, shared_connections
        - mermaid diagrams for visualization

    Raises:
        ValueError: If data format is invalid (not exactly one element)

    Note: Uses plain dicts for flexibility with dynamic Databricks schema.
          Consider TypedDict or dataclass for stronger typing in future refactor.
    """
    if not data:
        return {}

    if len(data) == 1:
        data = data[0]
    else:
        raise ValueError("Invalid data format")

    all_connections_and_estate = {}

    if data:
        service_ba_id = data["ba_id"]
        service_name = data["name"]
        all_connections_and_estate["busapp_id"] = service_ba_id
