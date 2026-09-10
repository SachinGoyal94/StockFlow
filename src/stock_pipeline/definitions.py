"""Dagster orchestration: fetch_quotes -> load_quotes, one job, hourly schedule."""

import logging

from dagster import (
    Config,
    ConfigurableResource,
    DefaultScheduleStatus,
    Definitions,
    EnvVar,
    RetryPolicy,
    ScheduleDefinition,
    SkipReason,
    job,
    op,
)
from pydantic import PrivateAttr

from . import db
from .config import load_config
from .finnhub_client import FinnhubClient
from .validation import InvalidQuoteError, Quote

logger = logging.getLogger(__name__)

SCHEDULE_CRON = "0 * * * *"


class FinnhubApiResource(ConfigurableResource):
    """Finnhub client as a Dagster resource; api_key resolves from EnvVar."""

    api_key: str
    timeout: float = 10.0
    max_retries: int = 3
    retry_backoff: float = 1.0

    _client: FinnhubClient | None = PrivateAttr(default=None)

    def get_client(self) -> FinnhubClient:
        if self._client is None:
            self._client = FinnhubClient(
                api_key=self.api_key,
                timeout=self.timeout,
                max_retries=self.max_retries,
                retry_backoff=self.retry_backoff,
            )
        return self._client


class FetchQuotesConfig(Config):
    """Per-run options, editable in the Launchpad."""

    # non-empty value replaces the STOCK_SYMBOLS list for this one run
    symbols_override: str = ""


@op(
    description=(
        "Fetch current quotes for every configured symbol from Finnhub's "
        "free /quote endpoint. Symbols with no data (invalid ticker, or "
        "all-zero response) are skipped with a warning; transient API "
        "failures are retried with backoff."
    )
)
def fetch_quotes(
    context, finnhub: FinnhubApiResource, config: FetchQuotesConfig
) -> list[Quote]:
    """Fetch quotes for all configured symbols, skipping ones with no data."""
    pipeline_config = load_config()

    symbols = list(pipeline_config.symbols)
    if config.symbols_override.strip():
        symbols = [
            s.strip().upper() for s in config.symbols_override.split(",") if s.strip()
        ]
        if not symbols:
            raise ValueError("symbols_override is set but contains no valid symbols")
        context.log.info("Symbol list overridden for this run: %s", symbols)

    context.log.info(
        "Fetching quotes for %d symbol(s): %s", len(symbols), ", ".join(symbols)
    )

    client = finnhub.get_client()
    quotes: list[Quote] = []
    skipped: list[str] = []

    for symbol, result in client.fetch_quotes(
        symbols, inter_request_delay=pipeline_config.inter_request_delay
    ):
        if isinstance(result, InvalidQuoteError):
            context.log.warning("Skipping %s: %s", symbol, result)
            skipped.append(symbol)
        else:
            quotes.append(result)
            context.log.info(
                "%s: $%.2f (%+.2f%%)", symbol, result.current, result.percent_change
            )

    if skipped:
        context.log.warning(
            "No data for %d/%d symbol(s): %s; continuing with the rest",
            len(skipped),
            len(symbols),
            ", ".join(skipped),
        )
    if not quotes:
        context.log.warning(
            "No usable quotes were fetched in this run (market closed, all "
            "symbols invalid, or API returned no data)."
        )

    context.add_output_metadata(
        {
            "symbols_requested": len(symbols),
            "quotes_fetched": len(quotes),
            "symbols_skipped": len(skipped),
            "skipped_symbols": ", ".join(skipped) or "none",
        }
    )
    return quotes


@op(
    description=(
        "Upsert the fetched quotes into the PostgreSQL stock_quotes table "
        "(one row per symbol per day). If no quotes were fetched, the op "
        "skips gracefully instead of writing junk or failing the run."
    )
)
def load_quotes(context, quotes: list[Quote]) -> tuple[int, int]:
    """Upsert the fetched quotes, or skip cleanly if nothing was fetched."""
    if not quotes:
        raise SkipReason(
            "No quotes to load: nothing fetched in this run; database "
            "left unchanged."
        )

    config = load_config()

    with db.connect(config) as conn:
        db.ensure_table(conn)  # safety net if init.sql didn't run
        inserted, updated = db.upsert_quotes(conn, quotes)

    context.add_output_metadata(
        {
            "rows_inserted": inserted,
            "rows_refreshed": updated,
        }
    )
    context.log.info(
        "Database updated: %d new row(s), %d refreshed row(s).", inserted, updated
    )
    return inserted, updated


@job(
    name="stock_pipeline_job",
    description=(
        "Fetch current stock quotes from Finnhub and upsert them into "
        "PostgreSQL. Scheduled hourly; can also be triggered manually from "
        "the Launchpad."
    ),
)
def stock_pipeline_job() -> None:
    load_quotes(fetch_quotes())


hourly_stock_schedule = ScheduleDefinition(
    job=stock_pipeline_job,
    cron_schedule=SCHEDULE_CRON,
    # RUNNING by default so a fresh deployment has a live schedule without
    # toggling it on in the UI
    default_status=DefaultScheduleStatus.RUNNING,
    description=(
        "Run the stock pipeline at the top of every hour, keeping the "
        "stock_quotes table current throughout the trading day."
    ),
)


defs = Definitions(
    jobs=[stock_pipeline_job],
    schedules=[hourly_stock_schedule],
    resources={
        "finnhub": FinnhubApiResource(
            api_key=EnvVar("FINNHUB_API_KEY"),
            timeout=10.0,
            max_retries=3,
            retry_backoff=1.0,
        )
    },
)
