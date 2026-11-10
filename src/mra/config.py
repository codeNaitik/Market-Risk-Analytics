"""Central configuration: settings.yaml + environment variables.

Environment variables (see .env.example):
    LAKE_ROOT          ./lake (local) or abfss://lake@<account>.dfs.core.windows.net
    FRED_API_KEY       free key from fred.stlouisfed.org
    SEC_USER_AGENT     SEC requires "Name email@domain" on every request
    KAFKA_BOOTSTRAP    localhost:9092 (docker-compose) or an Event Hubs endpoint
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SETTINGS = REPO_ROOT / "config" / "settings.yaml"


def _load_dotenv(path: Path) -> None:
    """Tiny .env loader so we don't need python-dotenv. Existing env vars win."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class Settings:
    raw: dict[str, Any]
    lake_root: str
    fred_api_key: str | None
    sec_user_agent: str
    kafka_bootstrap: str
    tickers: list[dict[str, str]] = field(default_factory=list)

    # -- convenience accessors -------------------------------------------------
    @property
    def start_date(self) -> str:
        return self.raw["history"]["start_date"]

    @property
    def ticker_symbols(self) -> list[str]:
        return [t["ticker"] for t in self.tickers]

    @property
    def benchmark(self) -> str:
        return self.raw["universe"]["benchmark"]

    @property
    def portfolio(self) -> dict[str, Any]:
        return self.raw["portfolio"]

    @property
    def is_cloud(self) -> bool:
        return self.lake_root.startswith(("abfss://", "s3://", "dbfs:/"))

    def path(self, *parts: str) -> str:
        """Join a lake path, working for local folders and abfss:// URLs alike."""
        root = self.lake_root.rstrip("/")
        return "/".join([root, *[p.strip("/") for p in parts]])


def _validate(raw: dict[str, Any]) -> None:
    weights = raw["portfolio"]["weights"]
    total = round(sum(weights.values()), 6)
    if total != 1.0:
        raise ValueError(f"portfolio weights must sum to 1.0, got {total}")
    known = {t["ticker"] for t in raw["universe"]["tickers"]}
    unknown = set(weights) - known
    if unknown:
        raise ValueError(f"portfolio holds tickers not in the universe: {sorted(unknown)}")


@lru_cache(maxsize=1)
def get_settings(settings_path: str | None = None) -> Settings:
    _load_dotenv(REPO_ROOT / ".env")
    path = Path(settings_path or os.getenv("MRA_SETTINGS", DEFAULT_SETTINGS))
    raw = yaml.safe_load(path.read_text())
    _validate(raw)

    lake_root = os.getenv("LAKE_ROOT", str(REPO_ROOT / "lake"))
    return Settings(
        raw=raw,
        lake_root=lake_root,
        fred_api_key=os.getenv("FRED_API_KEY") or None,
        sec_user_agent=os.getenv("SEC_USER_AGENT", "market-risk-analytics research contact@example.com"),
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        tickers=raw["universe"]["tickers"],
    )
