"""Storage abstraction over the data lake.

The same code writes to a local folder during development and to Azure Data Lake
Storage Gen2 in the cloud. fsspec picks the backend from the URL scheme:
    ./lake/bronze/...                         -> local filesystem
    abfss://lake@acct.dfs.core.windows.net/... -> adlfs (Azure), auth from env:
        AZURE_STORAGE_ACCOUNT_NAME + (AZURE_STORAGE_ACCOUNT_KEY | service principal vars)

Bronze layout (immutable, append-only, partitioned by load date):
    bronze/<source>/<dataset>/ingest_date=YYYY-MM-DD/<name>.<ext>
"""

from __future__ import annotations

import io
import json
from datetime import UTC, date, datetime
from typing import Any

import fsspec
import pandas as pd

from mra.config import Settings, get_settings


def _fs_and_path(url: str) -> tuple[fsspec.AbstractFileSystem, str]:
    fs, path = fsspec.core.url_to_fs(url)
    return fs, path


def bronze_path(source: str, dataset: str, name: str, ingest_date: date | None = None,
                settings: Settings | None = None) -> str:
    s = settings or get_settings()
    d = (ingest_date or date.today()).isoformat()
    return s.path("bronze", source, dataset, f"ingest_date={d}", name)


def write_json(url: str, payload: Any, source_url: str | None = None) -> str:
    """Write an API response wrapped in a lineage envelope (when + where it came from)."""
    fs, path = _fs_and_path(url)
    fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
    envelope = {
        "_ingested_at": datetime.now(UTC).isoformat(),
        "_source_url": source_url,
        "data": payload,
    }
    with fs.open(path, "w") as f:
        json.dump(envelope, f)
    return url


def write_parquet(url: str, df: pd.DataFrame) -> str:
    fs, path = _fs_and_path(url)
    fs.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    with fs.open(path, "wb") as f:
        f.write(buf.getvalue())
    return url


def exists(url: str) -> bool:
    fs, path = _fs_and_path(url)
    return fs.exists(path)
