from __future__ import annotations

import io
import os
import zipfile
from dataclasses import dataclass
from typing import Generator, Iterable, Optional

import pandas as pd


@dataclass(frozen=True)
class CSVReadOptions:
    chunksize: int = 500_000
    encoding: str = "utf-8"
    usecols: Optional[Iterable[str]] = None


def iter_zip_files(pickup_dir: str) -> Generator[str, None, None]:
    """Yield absolute paths of .zip files in pickup_dir."""
    if not os.path.isdir(pickup_dir):
        return
    for name in sorted(os.listdir(pickup_dir)):
        if name.lower().endswith(".zip"):
            yield os.path.join(pickup_dir, name)


def iter_csv_members(zippath: str) -> Generator[str, None, None]:
    """Yield member names of CSV files inside the zip without extracting."""
    with zipfile.ZipFile(zippath, mode="r") as zf:
        for info in zf.infolist():
            if not info.is_dir() and info.filename.lower().endswith(".csv"):
                yield info.filename


def iter_csv_chunks_from_zip(
    zippath: str,
    member: str,
    read_opts: Optional[CSVReadOptions] = None,
) -> Generator[pd.DataFrame, None, None]:
    """
    Stream a CSV member from a zip file into pandas DataFrame chunks using read_csv with chunksize.
    Never extracts entire zip or whole CSV into memory.
    """
    opts = read_opts or CSVReadOptions()
    with zipfile.ZipFile(zippath, mode="r") as zf:
        with zf.open(member, mode="r") as bf:
            text_stream = io.TextIOWrapper(bf, encoding=opts.encoding, newline="")
            reader = pd.read_csv(
                text_stream,
                chunksize=opts.chunksize,
                usecols=list(opts.usecols) if opts.usecols is not None else None,
            )
            for chunk in reader:  # type: ignore
                yield chunk
