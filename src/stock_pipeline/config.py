from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigError(Exception):
    """Raised when a required environment variable is missing or malformed."""


@dataclass(frozen=True)
class PipelineConfig:
    """Validated configuration for one pipeline run."""

    finnhub_api_key: str
    symbols: tuple[str, ...]
    request_timeout: float
    max_retries: int
    retry_backoff: float
    inter_request_delay: float

    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(
            f"Missing required environment variable '{name}'. "
            f"Set it in your .env file (see .env.example) and restart with: "
            f"docker compose up -d --build"
        )
    return value


def _require_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip() or str(default)
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(
            f"Environment variable '{name}' must be an integer, got {raw!r}"
        ) from None


def _require_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip() or str(default)
    try:
        return float(raw)
    except ValueError:
        raise ConfigError(
            f"Environment variable '{name}' must be a number, got {raw!r}"
        ) from None


def _parse_symbols(raw: str) -> tuple[str, ...]:
    """Parse comma-separated tickers, tolerating messy input
    (" aapl , msft ,,TSLA " -> AAPL, MSFT, TSLA) but refusing an empty list."""
    symbols = tuple(
        s.strip().upper() for s in raw.split(",") if s.strip()
    )
    if not symbols:
        raise ConfigError(
            "Environment variable 'STOCK_SYMBOLS' is empty or contains no "
            "valid symbols. Example: STOCK_SYMBOLS=AAPL,MSFT,GOOGL"
        )
    return symbols


def load_config() -> PipelineConfig:
    return PipelineConfig(
        finnhub_api_key=_require("FINNHUB_API_KEY"),
        symbols=_parse_symbols(_require("STOCK_SYMBOLS")),
        request_timeout=_require_float("FINNHUB_TIMEOUT", 10.0),
        max_retries=_require_int("FINNHUB_MAX_RETRIES", 3),
        retry_backoff=_require_float("FINNHUB_RETRY_BACKOFF", 1.0),
        inter_request_delay=_require_float("FINNHUB_INTER_REQUEST_DELAY", 1.1),
        db_host=_require("POSTGRES_HOST"),
        db_port=_require_int("POSTGRES_PORT", 5432),
        db_name=os.environ.get("STOCK_DB_NAME", "stockdata"),
        db_user=_require("POSTGRES_USER"),
        db_password=_require("POSTGRES_PASSWORD"),
    )
