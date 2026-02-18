from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import requests
from requests.auth import HTTPBasicAuth


@dataclass(frozen=True, slots=True)
class DownloadResult:
    path: Path
    bytes_written: int
    content_type: str | None
    status_code: int


def repo_root(start: Path | None = None) -> Path:
    here = (start or Path(__file__)).resolve()
    for p in (here, *here.parents):
        if (p / "pyproject.toml").exists():
            return p
    raise RuntimeError("Could not locate repo root (pyproject.toml not found).")


def data_dir() -> Path:
    return repo_root() / "data"


def download_stream_to_file(
    *,
    url: str,
    username: str,
    password: str,
    out_path: Path,
    verify_tls: bool = True,
    timeout: tuple[float, float] = (10.0, 600.0),
    chunk_size: int = 1024 * 1024,
    headers: Mapping[str, str] | None = None,
) -> DownloadResult:
    out_path = out_path.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not verify_tls:
        requests.packages.urllib3.disable_warnings(  # type: ignore[attr-defined]
            requests.packages.urllib3.exceptions.InsecureRequestWarning  # type: ignore[attr-defined]
        )

    req_headers: dict[str, str] = {"Accept": "application/xml"}
    if headers:
        req_headers.update(headers)

    auth = HTTPBasicAuth(username, password)

    with requests.get(
        url,
        auth=auth,
        headers=req_headers,
        stream=True,
        verify=verify_tls,
        timeout=timeout,
    ) as resp:
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type")
        bytes_written = 0

        with tempfile.NamedTemporaryFile(
            mode="wb",
            delete=False,
            dir=str(out_path.parent),
            prefix=f"{out_path.name}.",
            suffix=".part",
        ) as tmp:
            tmp_path = Path(tmp.name)
            try:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    tmp.write(chunk)
                    bytes_written += len(chunk)
                tmp.flush()
                os.fsync(tmp.fileno())
            except Exception:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                raise

    os.replace(tmp_path, out_path)

    return DownloadResult(
        path=out_path,
        bytes_written=bytes_written,
        content_type=content_type,
        status_code=resp.status_code,
    )


def download_trellix_xml_backup(
    *,
    url: str,
    username: str,
    password: str,
    filename: str = "trellix.xml",
    verify_tls: bool = True,
) -> DownloadResult:
    out_path = data_dir() / "trellix" / filename
    return download_stream_to_file(
        url=url,
        username=username,
        password=password,
        out_path=out_path,
        verify_tls=verify_tls,
    )


# usage

from pathlib import Path
from services.trellix.download import download_trellix_xml_backup

res = download_trellix_xml_backup(
    url="https://trellix.example/export.xml",
    username="user",
    password="pass",
    filename="policies_export.xml",
    verify_tls=False,
)

print(res.path, res.bytes_written)

# init in trelix dir

from .download import DownloadResult, download_stream_to_file, download_trellix_xml_backup

__all__ = [
    "DownloadResult",
    "download_stream_to_file",
    "download_trellix_xml_backup",
]
