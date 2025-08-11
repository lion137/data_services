from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.types import NVARCHAR, BigInteger

try:
    import config  # type: ignore
except Exception:  # pragma: no cover
    class _Cfg:
        mssql_conn = "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
        env = "DEV"
        OVR_PICKUP_PATH = "./ovr_pickup"
        OVR_BATCH_SIZE = 500_000
    config = _Cfg()  # type: ignore

from .core import ColumnMapping, transform_chunk, DEFAULT_TARGET_ORDER
from .io import CSVReadOptions, iter_zip_files, iter_csv_members, iter_csv_chunks_from_zip

logger = logging.getLogger(__name__)


@dataclass
class DBOptions:
    table_name: str = "DIRaw"
    chunksize: int = 5_000  # to_sql internal chunking


# Default SQLAlchemy dtype mapping, aligned with hr_loader
SQL_DTYPE = {
    "Path_ID": BigInteger(),
    "Full_Path": NVARCHAR(length=1600),
    "Directory_Structure": NVARCHAR(length=700),
    "Document_Name": NVARCHAR(length=1600),
    "DFS": NVARCHAR(length=1600),
    "Created_Date": NVARCHAR(length=255),
    "Modified_Date": NVARCHAR(length=255),
    "Accessed_Date": NVARCHAR(length=255),
    "creator_name": NVARCHAR(length=255),
    "Owner_Name": NVARCHAR(length=255),
    "Owner_Login": NVARCHAR(length=255),
    "Modifier_Name": NVARCHAR(length=255),
    "Modifier_Login": NVARCHAR(length=255),
    "Accessor_Name": NVARCHAR(length=255),
    "Accessor_Login": NVARCHAR(length=255),
    "Classify_Time": NVARCHAR(length=255),
    "Tags": NVARCHAR(length=255),
    "Ownership": NVARCHAR(length=255),
    "Inferred_Owner_Level": NVARCHAR(length=50),
    "Load_For": NVARCHAR(length=50),
}


def default_engine_factory() -> Engine:
    return create_engine(f"{config.mssql_conn}")


class OVRProcessor:
    """
    Imperative shell orchestrator: scans zips, streams CSVs, transforms chunks, writes to DB.
    """

    def __init__(
        self,
        mapping: ColumnMapping,
        engine_factory: Callable[[], Engine] = default_engine_factory,
        pickup_path: Optional[str] = None,
        csv_batch_rows: Optional[int] = None,
        db_options: Optional[DBOptions] = None,
        load_for_value: str = "OVR",
    ) -> None:
        self.mapping = mapping
        self.engine_factory = engine_factory
        self.pickup_path = pickup_path or getattr(config, "OVR_PICKUP_PATH", "./ovr_pickup")
        self.csv_batch_rows = csv_batch_rows or getattr(config, "OVR_BATCH_SIZE", 500_000)
        self.db_options = db_options or DBOptions()
        self.load_for_value = load_for_value

    def process_all(self) -> int:
        """Process all .zip files found in pickup path. Returns number of processed zip files."""
        processed = 0
        for zippath in iter_zip_files(self.pickup_path):
            try:
                self._process_zip(zippath)
                processed += 1
                logger.info(f"Processed OVR zip: {zippath}")
            except Exception as e:  # log and continue to next zip
                logger.exception(f"Failed processing zip {zippath}: {e}")
        return processed

    def _process_zip(self, zippath: str) -> None:
        read_opts = CSVReadOptions(chunksize=self.csv_batch_rows, usecols=self._source_columns())
        for member in iter_csv_members(zippath):
            for chunk in iter_csv_chunks_from_zip(zippath, member, read_opts):
                self._process_chunk(chunk)

    def _source_columns(self) -> Optional[Iterable[str]]:
        # Only read needed source columns from CSV for efficiency
        return list(self.mapping.source_to_target.keys())

    def _process_chunk(self, chunk: pd.DataFrame) -> None:
        transformed = transform_chunk(chunk, self.mapping, load_for_value=self.load_for_value)
        # Write to DB
        engine = self.engine_factory()
        transformed.to_sql(
            self.db_options.table_name,
            engine,
            if_exists="append",
            index=False,
            dtype=SQL_DTYPE,
            chunksize=self.db_options.chunksize,
        )


# Example mapping for OVR files: adjust source column names as needed
DEFAULT_MAPPING = ColumnMapping(
    source_to_target={
        # If OVR source already matches target names, map 1:1
        "Path_ID": "Path_ID",
        "Full_Path": "Full_Path",
        "Directory_Structure": "Directory_Structure",
        "Document_Name": "Document_Name",
        "DFS": "DFS",
        "Created_Date": "Created_Date",
        "Modified_Date": "Modified_Date",
        "Accessed_Date": "Accessed_Date",
        "creator_name": "creator_name",
        "Owner_Name": "Owner_Name",
        "Owner_Login": "Owner_Login",
        "Modifier_Name": "Modifier_Name",
        "Modifier_Login": "Modifier_Login",
        "Accessor_Name": "Accessor_Name",
        "Accessor_Login": "Accessor_Login",
        "Classify_Time": "Classify_Time",
        "Tags": "Tags",
        # Ownership, Inferred_Owner_Level, Load_For are computed/added
    },
    target_order=DEFAULT_TARGET_ORDER,
    string_columns=[
        "Full_Path",
        "Directory_Structure",
        "Document_Name",
        "DFS",
        "Created_Date",
        "Modified_Date",
        "Accessed_Date",
        "creator_name",
        "Owner_Name",
        "Owner_Login",
        "Modifier_Name",
        "Modifier_Login",
        "Accessor_Name",
        "Accessor_Login",
        "Classify_Time",
        "Tags",
        "Ownership",
        "Inferred_Owner_Level",
        "Load_For",
    ],
)
