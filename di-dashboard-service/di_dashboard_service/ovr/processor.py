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
except Exception:
    class _Cfg:
        mssql_conn = (
            "mysql+pymysql://dev:devpass@localhost:3306/DIDashboard"
            "?charset=utf8mb4&allow_public_key_retrieval=true"
        )
        env = "DEV"
        OVR_PICKUP_PATH = "./ovr_pickup"
        OVR_BATCH_SIZE = 500_000
    config = _Cfg()  # type: ignore

from di_dashboard_service.ovr.core import ColumnMapping, transform_chunk, DEFAULT_TARGET_ORDER
from di_dashboard_service.services.io import (
    CSVReadOptions,
    iter_zip_files,
    iter_csv_members,
    iter_csv_chunks_from_zip,
)

logger = logging.getLogger(__name__)


@dataclass
class DBOptions:
    table_name: str = "DIRaw"
    chunksize: int = 5_000


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
        self.rows_written: int = 0

    def process_all(self) -> int:
        """Process all .zip files found in pickup path.
        Returns number of processed zip files."""
        processed = 0
        seen_any_zip = False
        for zippath in iter_zip_files(self.pickup_path):
            seen_any_zip = True
            try:
                self._process_zip(zippath)
                processed += 1
                logger.info(f"Processed OVR zip: {zippath}")
            except Exception as e:  # log and continue to next zip
                logger.exception(f"Failed processing zip {zippath}: {e}")
        if not seen_any_zip:
            logger.error(f"No .zip files found in pickup path: {self.pickup_path}")
            return 0
        return processed

    def _process_zip(self, zippath: str) -> None:
        read_opts = CSVReadOptions(chunksize=self.csv_batch_rows, usecols=self._source_columns())
        found_member = False
        for member in iter_csv_members(zippath):
            found_member = True
            found_rows = False
            for chunk in iter_csv_chunks_from_zip(zippath, member, read_opts):
                found_rows = True
                self._process_chunk(chunk)
            if not found_rows:
                logger.error(f"CSV member has no rows: zip={zippath}, member={member}")
        if not found_member:
            logger.error(f"No CSV files found inside zip: {zippath}")

    def _source_columns(self) -> Optional[Iterable[str]]:
        return list(self.mapping.source_to_target.keys())

    def _process_chunk(self, chunk: pd.DataFrame) -> None:
        transformed = transform_chunk(chunk, self.mapping, load_for_value=self.load_for_value)
        engine = self.engine_factory()
        row_count = len(transformed)
        if row_count == 0:
            logger.error("Transformed chunk is empty; skipping write")
            return
        transformed.to_sql(
            self.db_options.table_name,
            engine,
            if_exists="append",
            index=False,
            dtype=SQL_DTYPE,
            chunksize=self.db_options.chunksize,
        )
        self.rows_written += row_count


DEFAULT_MAPPING = ColumnMapping(
    source_to_target={
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
