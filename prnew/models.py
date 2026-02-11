# just apart of real models module with the relevant classes

import json
from datetime import datetime

from databricks.sql.client import Cursor
from psycopg2._json import Json
from typing_extensions import Self

from ...utilities import DateTimeEncoder, classproperty
from ..dbCachedObject import DbCachedObject
from ..dbObject import DbObject
from ..dbObjectEnabled import DbObjectEnabled

# ============================================================================
# CONSTANTS AND DATABRICKS TABLES
# ============================================================================

try:
    from client_code.constants import (
        DATABRICKS_TABLE_ALL_BAS,
        DATABRICKS_TABLE_ATTESTATION,
        DATABRICKS_TABLE_CONNECTIONS_AND_ESTATE,
        DATABRICKS_TABLE_LEADERBOARD_DETAILED_REPORT,
        DATABRICKS_TABLE_LEADERBOARD_GBGF,
        DATABRICKS_TABLE_LEADERBOARD_OVERVIEW,
        DATABRICKS_TABLE_SINGLE_SIDED_CONNECTIONS,
        DATABRICKS_TABLE_USER_TO_SERVICE_MAPPING,
    )
except ModuleNotFoundError:
    from ...constants import (
        DATABRICKS_TABLE_ALL_BAS,
        DATABRICKS_TABLE_ATTESTATION,
        DATABRICKS_TABLE_CONNECTIONS_AND_ESTATE,
        DATABRICKS_TABLE_LEADERBOARD_DETAILED_REPORT,
        DATABRICKS_TABLE_LEADERBOARD_GBGF,
        DATABRICKS_TABLE_LEADERBOARD_OVERVIEW,
        DATABRICKS_TABLE_SINGLE_SIDED_CONNECTIONS,
        DATABRICKS_TABLE_USER_TO_SERVICE_MAPPING,
    )

# ============================================================================
# DATABRICKS SERVICE
# ============================================================================

class DatabricksService(DbObject):
    __tablename__ = "databricks_service"

    def __init__(self, ba_id: int, ba_name: str, description: str, active: bool, id: int = None):  # @ReservedAssignment
        self.id = id
        self.ba_id = ba_id
        self.ba_name = ba_name
        self.description = description
        self.active = active

    def __str__(self):
        return self.ba_name


# ============================================================================
# REMEDIATION CATEGORY ATTRIBUTE
# ============================================================================

class RemediationCategoryAtt(DbObjectEnabled):
    __tablename__ = "remediation_category"

    @classproperty
    def schemaSuffix(self):
        return "_att"


# ============================================================================
# BOW STATUS ATTRIBUTE
# ============================================================================

class BowStatusAtt(DbObjectEnabled):
    __tablename__ = "bow_status"

    @classproperty
    def schemaSuffix(self):
        return "_att"


# ============================================================================
# ATTESTATION
# ============================================================================

class Attestation(DbObject):
    __tablename__ = "attestation"

    def __init__(
        self,
        ba_id: int,
        ba_name: str,
        submitted_by: str,
        submitted_at: datetime,
        updated_at: datetime,
        bow_status_id: int,
        bow_details: str,
        remediation: bool,
        remediation_reason: str,
        remediation_category_id: int,
        id: int = None
    ):  # @ReservedAssignment
        self.id = id
        self.ba_id = ba_id
        self.ba_name = ba_name
        self.submitted_by = submitted_by
        self.submitted_at = submitted_at
        self.updated_at = updated_at
        self.bow_status_id = bow_status_id
        self.bow_details = bow_details
        self.remediation = remediation
        self.remediation_reason = remediation_reason
        self.remediation_category_id = remediation_category_id

    def __str__(self):
        return self.ba_name

    @classproperty
    def schemaSuffix(self):
        return "_att"

    @classmethod
    def last(cls, ba_id: str) -> Self:
        """Returns last available attestation against a particular ba_id"""
        last = Attestation.searchAll(ba_id=ba_id, orderClause="submitted_at DESC LIMIT 1")
        return last[0] if last else None


# ============================================================================
# ESTATE ATTRIBUTE
# ============================================================================

class EstateAtt(DbObject):
    __tablename__ = "estate"

    def __init__(
        self,
        attestation_id: int,
        host: str,
        environment: str,
        lifecycle: str,
        shared_status: str,
        id: int = None
    ):  # @ReservedAssignment
        self.id = id
        self.attestation_id = attestation_id
        self.host = host
        self.environment = environment
        self.lifecycle = lifecycle
        self.shared_status = shared_status

    def __str__(self):
        return self.attestation_id

    @classproperty
    def schemaSuffix(self):
        return "_att"


# ============================================================================
# CONNECTION ATTRIBUTE
# ============================================================================

class ConnectionAtt(DbObject):
    __tablename__ = "connection"

    def __init__(
        self,
        attestation_id: int,
        ba_id: str,
        ba_name: str,
        inbound: bool,
        required: str,
        reason: str,
        id: int = None
    ):  # @ReservedAssignment
        self.id = id
        self.attestation_id = attestation_id
        self.ba_id = ba_id
        self.ba_name = ba_name
        self.inbound = inbound
        self.required = required
        self.reason = reason

    def __str__(self):
        return self.ba_name

    @classproperty
    def schemaSuffix(self):
        return "_att"


# ============================================================================
# SHARED CONNECTION ATTRIBUTE
# ============================================================================

class SharedConnectionAtt(ConnectionAtt):
    __tablename__ = "shared_connection"

    def __init__(self, host: str, **kwargs):
        super(SharedConnectionAtt, self).__init__(**kwargs)
        self.host = host


# ============================================================================
# MISSING CONNECTION ATTRIBUTE
# ============================================================================

class MissingConnectionAtt(ConnectionAtt):
    __tablename__ = "missing_connection"

    def __init__(self, **kwargs):
        kwargs["required"] = None
        super(MissingConnectionAtt, self).__init__(**kwargs)
        del self.required


# ============================================================================
# SINGLE SIDED CONNECTION ATTRIBUTE
# ============================================================================

class SingleSidedConnectionAtt(ConnectionAtt):
    __tablename__ = "single_sided_connection"

    def __init__(self, type: str, connection: str, **kwargs):
        kwargs["ba_id"] = None
        kwargs["ba_name"] = None
        super(SingleSidedConnectionAtt, self).__init__(**kwargs)
        self.type = type
        self.connection = connection
        del self.ba_id
        del self.ba_name