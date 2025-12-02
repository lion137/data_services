"""
Reattestation Module

This module encapsulates logic for retrieving previously attested connections
from PostgreSQL database. It replaces the old MSSQL-based queries with
PostgreSQL-native queries using the attestation schema.

The main public function is get_all_connections() which aggregates all
previously attested connection types for a given service.
"""

from typing import Optional

from .models import Attestation, ConnectionAtt, SharedConnectionAtt, SingleSidedConnectionAtt


def _get_last_attested_connections(ba_id: str) -> list[dict]:
    """
    Retrieve the last attested connections (both regular and shared) for a service.

    This function replaces the old MSSQL query that used [upp.Connection] and
    [upp.SharedConnection] tables. Now it queries PostgreSQL dev_att schema.

    Args:
        ba_id: The business application ID

    Returns:
        List of dictionaries containing connection information with keys:
        - Host: hostname (empty string for regular connections)
        - ServiceId: the ba_id of the connected service
        - Direction: 'Inbound' or 'Outbound'
        - Required: 'Yes' or 'No'
        - ReasonRequired: reason text
        - Type: 'dedicated' or 'shared'
    """
    if not ba_id:
        return []

    # Get the last attestation for this ba_id
    last_attestation = Attestation.last(ba_id)
    if not last_attestation:
        return []

    attestation_id = last_attestation.id

    # Get regular connections for this attestation
    regular_connections = ConnectionAtt.searchAll(attestation_id=attestation_id)

    # Get shared connections for this attestation
    shared_connections = SharedConnectionAtt.searchAll(attestation_id=attestation_id)

    # Format results to match the old MSSQL format
    result = []

    # Process regular connections (Type = 'dedicated')
    for conn in regular_connections:
        result.append({
            'Host': '',
            'ServiceId': conn.ba_id,
            'Direction': 'Inbound' if conn.inbound else 'Outbound',
            'Required': conn.required,
            'ReasonRequired': conn.reason,
            'Type': 'dedicated'
        })

    # Process shared connections (Type = 'shared')
    for conn in shared_connections:
        result.append({
            'Host': conn.host,
            'ServiceId': conn.ba_id,
            'Direction': 'Inbound' if conn.inbound else 'Outbound',
            'Required': conn.required,
            'ReasonRequired': conn.reason,
            'Type': 'shared'
        })

    return result


def _get_last_attested_shared_connections(ba_id: str) -> list[dict]:
    """
    Retrieve only the last attested shared connections for a service.

    This function replaces the old MSSQL query that queried [upp.SharedConnection].
    Now it queries PostgreSQL dev_att.shared_connection table.

    Args:
        ba_id: The business application ID

    Returns:
        List of dictionaries containing shared connection information with keys:
        - Host: hostname
        - ServiceId: the ba_id of the connected service
        - Direction: 'Inbound' or 'Outbound'
        - Required: 'Yes' or 'No'
        - ReasonRequired: reason text
        - Type: 'shared'
    """
    if not ba_id:
        return []

    # Get the last attestation for this ba_id
    last_attestation = Attestation.last(ba_id)
    if not last_attestation:
        return []

    attestation_id = last_attestation.id

    # Get shared connections for this attestation
    shared_connections = SharedConnectionAtt.searchAll(attestation_id=attestation_id)

    # Format results to match the old MSSQL format
    result = []
    for conn in shared_connections:
        result.append({
            'Host': conn.host,
            'ServiceId': conn.ba_id,
            'Direction': 'Inbound' if conn.inbound else 'Outbound',
            'Required': conn.required,
            'ReasonRequired': conn.reason,
            'Type': 'shared'
        })

    return result


def get_all_connections(ba_id: str) -> list[dict]:
    """
    Public function to get all previously attested connections for a service.

    This is the main entry point for retrieving reattestation data. It retrieves
    all connection types (dedicated, shared, and single-sided) from the last
    attestation.

    Args:
        ba_id: The business application ID

    Returns:
        List of dictionaries containing all connection information from the
        last attestation. Each dict has keys appropriate to its connection type.
    """
    if not ba_id:
        return []

    # Get the last attestation for this ba_id
    last_attestation = Attestation.last(ba_id)
    if not last_attestation:
        return []

    attestation_id = last_attestation.id

    # Get all connection types
    regular_connections = ConnectionAtt.searchAll(attestation_id=attestation_id)
    shared_connections = SharedConnectionAtt.searchAll(attestation_id=attestation_id)
    single_sided_connections = SingleSidedConnectionAtt.searchAll(attestation_id=attestation_id)

    result = []

    # Process regular connections
    for conn in regular_connections:
        result.append({
            'Host': '',
            'ServiceId': conn.ba_id,
            'ba_id': conn.ba_id,
            'ba_name': conn.ba_name,
            'Direction': 'Inbound' if conn.inbound else 'Outbound',
            'direction': 'inbound' if conn.inbound else 'outbound',
            'inbound': conn.inbound,
            'Required': conn.required,
            'required': conn.required,
            'ReasonRequired': conn.reason,
            'reason': conn.reason,
            'Type': 'dedicated',
            'type': 'dedicated'
        })

    # Process shared connections
    for conn in shared_connections:
        result.append({
            'Host': conn.host,
            'host': conn.host,
            'ServiceId': conn.ba_id,
            'ba_id': conn.ba_id,
            'ba_name': conn.ba_name,
            'Direction': 'Inbound' if conn.inbound else 'Outbound',
            'direction': 'inbound' if conn.inbound else 'outbound',
            'inbound': conn.inbound,
            'Required': conn.required,
            'required': conn.required,
            'ReasonRequired': conn.reason,
            'reason': conn.reason,
            'Type': 'shared',
            'type': 'shared'
        })

    # Process single-sided connections
    for conn in single_sided_connections:
        result.append({
            'Direction': 'Inbound' if conn.inbound else 'Outbound',
            'direction': 'inbound' if conn.inbound else 'outbound',
            'inbound': conn.inbound,
            'Required': conn.required,
            'required': conn.required,
            'ReasonRequired': conn.reason,
            'reason': conn.reason,
            'Type': 'single-sided',
            'type': 'single-sided',
            'connection_type': conn.type,
            'connection': conn.connection
        })

    return result
