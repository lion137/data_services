from __future__ import annotations

# Re-export commonly used services APIs for convenient imports
from .io import (
    CSVReadOptions,
    iter_zip_files,
    iter_csv_members,
    iter_csv_chunks_from_zip,
)
from .db_io import (
    get_mails_to_send,
    update_after_mails_send_bulk,
)

__all__ = [
    # io
    "CSVReadOptions",
    "iter_zip_files",
    "iter_csv_members",
    "iter_csv_chunks_from_zip",
    # db_io
    "get_mails_to_send",
    "update_after_mails_send_bulk",
]
