from __future__ import annotations

import argparse
import io
import random
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
import pandas as pd


try:
    import config  # type: ignore
except Exception:
    class _Cfg:
        OVR_PICKUP_PATH = "./ovr_pickup"

    config = _Cfg()  # type: ignore


SOURCE_COLUMNS = [
    # Must match keys used in DEFAULT_MAPPING in ovr/processor.py
    "Path_ID",
    "Full_Path",
    "Directory_Structure",
    "Document_Name",
    "DFS",
    "Created_Date",
    "Modified_Date",
    "Accessed_Date",
    "creator_name",  # note: lowercase as in mapping
    "Owner_Name",
    "Owner_Login",
    "Modifier_Name",
    "Modifier_Login",
    "Accessor_Name",
    "Accessor_Login",
    "Classify_Time",
    "Tags",
]


OWNERS = [
    ("Alice Smith", "asmith"),
    ("Bob Johnson", "bjohnson"),
    ("Carol Diaz", "cdiaz"),
    ("David Lee", "dlee"),
    ("Eve Patel", "epatel"),
]

CREATORS = [
    "scanner", "ingest-bot", "system", "archiver", "user-import",
]

EXTS = [".docx", ".xlsx", ".pptx", ".pdf", ".txt", ".csv"]


def _random_date(base: datetime) -> str:
    # Return ISO-ish string like '2024-07-21T13:22:11'
    dt = base + timedelta(minutes=random.randint(-60 * 24 * 90, 60 * 24 * 90))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _random_name(n: int = 8) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def _random_path() -> tuple[str, str, str, str]:
    # Returns (dfs, directory_structure, document_name, full_path)
    server = random.choice(["//files1", "//files2", "//nas01"])  # DFS root
    parts = [
        random.choice(["dept", "team", "project", "shared"]),
        random.choice(["finance", "hr", "it", "marketing", "sales", "legal"]),
        _random_name(6),
    ]
    directory_structure = "/".join(parts)
    base = random.choice(["report", "summary", "presentation", "notes", "data"]) + "_" + _random_name(4)
    ext = random.choice(EXTS)
    document_name = base + ext
    dfs = f"{server}/{parts[0]}/{parts[1]}"
    full_path = f"{server}/{directory_structure}/{document_name}"
    return dfs, directory_structure, document_name, full_path


def make_dataframe(rows: int = 1000, seed: int | None = 42) -> pd.DataFrame:
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)

    base = datetime.now()
    data = {c: [] for c in SOURCE_COLUMNS}

    for i in range(rows):
        path_id = i + 1
        dfs, directory_structure, document_name, full_path = _random_path()

        owner_name, owner_login = random.choice(OWNERS)
        # Occasionally leave owner empty to exercise ownership inference
        if random.random() < 0.1:
            owner_name_use, owner_login_use = "", ""
        else:
            owner_name_use, owner_login_use = owner_name, owner_login

        modifier_name, modifier_login = random.choice(OWNERS)
        accessor_name, accessor_login = random.choice(OWNERS)
        creator = random.choice(CREATORS)

        created = _random_date(base)
        modified = _random_date(base)
        accessed = _random_date(base)

        tags = ",".join(random.sample(["confidential", "internal", "public", "pii", "finance", "hr"], k=random.randint(0, 3)))
        classify_time = str(random.randint(1, 120))

        row = {
            "Path_ID": path_id,
            "Full_Path": full_path,
            "Directory_Structure": directory_structure,
            "Document_Name": document_name,
            "DFS": dfs,
            "Created_Date": created,
            "Modified_Date": modified,
            "Accessed_Date": accessed,
            "creator_name": creator,
            "Owner_Name": owner_name_use,
            "Owner_Login": owner_login_use,
            "Modifier_Name": modifier_name,
            "Modifier_Login": modifier_login,
            "Accessor_Name": accessor_name,
            "Accessor_Login": accessor_login,
            "Classify_Time": classify_time,
            "Tags": tags,
        }
        for k, v in row.items():
            data[k].append(v)

    df = pd.DataFrame(data, columns=SOURCE_COLUMNS)
    return df


def write_zip_with_csv(df: pd.DataFrame, zip_path: Path, csv_name: str = "ovr_sample.csv") -> Path:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, buf.getvalue())
    return zip_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic OVR file metadata")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate")
    parser.add_argument(
        "--pickup", type=Path, default=Path(getattr(config, "OVR_PICKUP_PATH", "./ovr_pickup")),
        help="Pickup directory where zip(s) will be written",
    )
    parser.add_argument("--zip-name", default=f"ovr_{uuid.uuid4().hex[:8]}.zip", help="Output zip file name")
    parser.add_argument("--csv-name", default="ovr_sample.csv", help="CSV member name inside zip")
    parser.add_argument("--no-zip", action="store_true", help="Only print sample DataFrame; do not write files")
    args = parser.parse_args()

    df = make_dataframe(rows=args.rows)
    if args.no_zip:
        print(df.head(10))
        print(f"Generated rows: {len(df)}")
        return

    pickup = Path(args.pickup)
    pickup.mkdir(parents=True, exist_ok=True)
    out_zip = pickup / args.zip_name
    write_zip_with_csv(df, out_zip, csv_name=args.csv_name)
    print(f"Wrote zip: {out_zip}")
    print(f"Rows: {len(df)} | CSV member: {args.csv_name}")


if __name__ == "__main__":
    main()

