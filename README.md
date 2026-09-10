# StockFlow: Dockerized Stock Market Data Pipeline

A production-style data pipeline that **fetches live stock quotes from
[Finnhub](https://finnhub.io) on an hourly schedule, validates them, and
upserts them into PostgreSQL**, orchestrated by
[Dagster](https://dagster.io) and fully Dockerized, so the whole stack
starts with a single command.

```
                ┌─────────────────┐        hourly schedule
                │  Finnhub API    │─────── "0 * * * *" ┌──────────────┐
                │  /quote (JSON)  │                  │ dagster-daemon│
                └────────┬────────┘                  └──────┬───────┘
                         │  requests (timeout, retry,        │ launches runs
                         │  backoff, validation)             ▼
                         │                          ┌──────────────────┐
                         │                          │   stock_pipeline │
                         ▼                          │      _job        │
                ┌────────────────┐                   │ fetch→validate→ │
                │  stock_quotes  │◄──── upsert ──────│ load (upsert)   │
                │ table (1 row/  │    ON CONFLICT    └────────┬────────┘
                │ symbol/day)    │                            │ served over gRPC
                └────────────────┘                   ┌───────┴────────┐
                                                     │ pipeline-code  │
                ┌────────────────┐                   └───────┬────────┘
                │   PostgreSQL   │◄───────────────────────────┘
                │ dagster +      │        http://localhost:3000
                │ stockdata DBs  │           ┌──────────────────┐
                └────────────────┘           │dagster-webserver │─► your browser
                                             └──────────────────┘
```

| Requirement | How it's met |
|---|---|
| Scheduled JSON fetch | Dagster schedule `0 * * * *` → `fetch_quotes` op → `requests` |
| Parse & store | `validation.py` parses/validates → `db.py` upserts into `stock_quotes` |
| Robustness | Retries w/ backoff, all-zero detection, partial-quote NULLs, op-level skips |
| Docker Compose | `docker compose up -d --build` starts all 4 services |
| Secrets | `.env` → env vars → Dagster `EnvVar`; never in code, logs, or git |
| Scalability | Stateless services, Postgres-backed Dagster state, configurable symbol list |

---

## Prerequisites

- **Docker Desktop** for Windows (WSL2 backend): <https://docs.docker.com/desktop/>
- A **free Finnhub API key**: <https://finnhub.io/register> (free tier: 60 requests/min)

## Quick start

```bash
# 1. Configure secrets (one-time)
copy .env.example .env        # Windows   (Linux/macOS: cp .env.example .env)
#    edit .env: set FINNHUB_API_KEY and a POSTGRES_PASSWORD

# 2. Build and start the entire pipeline
docker compose up -d --build

# 3. Open the Dagster UI
start http://localhost:3000
```

In the UI: **Jobs → `stock_pipeline_job` → Launchpad → Launch run** (no config
needed; the symbol list comes from `.env`). You'll see the two ops:

1. `fetch_quotes`: quotes for every symbol, warnings for any symbol with no data
2. `load_quotes`: row counts for the upsert

The schedule runs the job automatically at the top of every hour
(enable/disable it under **Deployment → Schedules**; it's on by default when
the daemon is running).

### Verify the data

```bash
docker compose exec postgres psql -U stockflow -d stockdata -c \
  "SELECT symbol, current, change, percent_change, fetched_at FROM stock_quotes ORDER BY symbol;"
```

(Replace `stockflow` with your `POSTGRES_USER` if you changed it.)

---

## What the pipeline does, in detail

1. **Fetch**: `finnhub_client.py` calls `GET /api/v1/quote?symbol=…` with a
   10 s timeout. Transient failures (network errors, HTTP 429, 5xx) retry with
   exponential backoff, honoring `Retry-After` when Finnhub sends it. Permanent
   errors (400/401/403) fail fast instead of burning the rate limit.
2. **Validate**: `validation.py` turns each JSON payload into a typed `Quote`.
   Finnhub answers an *invalid symbol* with HTTP 200 and all-zero fields. That
   shape is detected and the symbol is **skipped with a warning**, never
   crashing the run. A quote with some missing fields is kept with NULLs
   rather than discarded.
3. **Load**: `db.py` upserts the batch into `stock_quotes` in a **single
   transaction** with `INSERT … ON CONFLICT (symbol, trade_date) DO UPDATE`:
   the first run of a day inserts, later hourly runs refresh. Failures leave
   the table untouched; re-runs never duplicate rows. `trade_date` is the
   **UTC** day of the fetch (the convention for US-market data), so a run
   just after local midnight can legitimately file under the previous day.
4. **Orchestrate**: `definitions.py` wires it into a Dagster job with op-level
   retry policies and an hourly schedule, and exposes it in the UI.

## Error handling matrix

| Failure mode | Behavior |
|---|---|
| Invalid/unknown symbol (all-zero response) | ⚠ symbol skipped with warning, run continues |
| Partial quote (some fields missing) | ⚠ stored with NULLs for missing fields |
| Network error / timeout | retried up to 3× with exponential backoff, then op retry |
| HTTP 429 rate limit | retried with `Retry-After` backoff; inter-request delay keeps batches under 60/min |
| HTTP 400/401/403 (bad key/request) | no client-side retries; Dagster retries the op 3×, then the run fails with a clear message |
| Malformed JSON | retried, then op fails with a clear error in the UI |
| All symbols unusable | `load_quotes` **skips**: run marked skipped, not failed, DB untouched |
| Database unavailable | op retried (Dagster RetryPolicy); no partial writes |
| Missing env var | startup fails immediately with a message naming the variable |

## Configuration

All settings live in `.env` (see [.env.example](.env.example)):

| Variable | Default | Meaning |
|---|---|---|
| `FINNHUB_API_KEY` | *(required)* | Your Finnhub API key |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | *(required)* | Database credentials |
| `STOCK_SYMBOLS` | `AAPL,MSFT,GOOGL,AMZN,TSLA` | Comma-separated tickers to fetch |
| `FINNHUB_TIMEOUT` | `10` | Seconds before an HTTP request gives up |
| `FINNHUB_MAX_RETRIES` | `3` | Retries per symbol for transient errors |
| `FINNHUB_RETRY_BACKOFF` | `1` | Base seconds for backoff (1, 2, 4 …) |
| `FINNHUB_INTER_REQUEST_DELAY` | `1.1` | Pause between symbols (rate-limit safety) |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `postgres` / `5432` | Container-internal; rarely changed |
| `POSTGRES_DB` | `dagster` | Database for Dagster's own state |
| `STOCK_DB_NAME` | `stockdata` | Database for stock data |

Change symbols for one specific run without editing `.env`: in the Launchpad,
set `symbols_override` (e.g. `NVDA,META`):

```yaml
ops:
  fetch_quotes:
    config:
      symbols_override: "NVDA,META"
```

This is also the quickest way to demo the graceful-missing-data handling:
mix in a bogus ticker (`AAPL,NOTAREALTICKER`) and watch the run still
succeed. The fake symbol is skipped with a warning while the real ones
land in the table.

## Scaling up

- **More symbols**: edit `STOCK_SYMBOLS` and restart. The fetch loop spaces
  requests 1.1 s apart, so even 60 symbols stay under the 60 req/min ceiling.
  For bigger lists, raise `FINNHUB_INTER_REQUEST_DELAY`.
- **More frequent runs**: change the cron in `definitions.py`
  (`SCHEDULE_CRON`) and rebuild.
- **Horizontal scale**: webserver/daemon/code-server are stateless; all
  state is in Postgres, so services can run as multiple replicas.

## Repository layout

```
├── docker-compose.yml          # 4-service stack: postgres, code server, webserver, daemon
├── Dockerfile                  # one image shared by the three Dagster services
├── init.sql                    # creates stockdata DB + stock_quotes table on first boot
├── dagster.yaml                # Dagster instance state → PostgreSQL
├── workspace.yaml              # points Dagster at the gRPC code server
├── .env.example                # template for secrets/settings
└── src/
    ├── pyproject.toml
    └── stock_pipeline/
        ├── definitions.py      # ← Dagster job, ops, schedule, resources
        ├── finnhub_client.py   # ← fetches quotes (requests + retries + backoff)
        ├── validation.py       # parses/validates each JSON response
        ├── db.py               # PostgreSQL connection + batched upsert
        └── config.py           # env-var config, validated up front
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `docker: command not found` | Install Docker Desktop, ensure WSL2 backend, restart terminal |
| Port 3000 already in use | Stop the other app, or change the host port in `docker-compose.yml` (`"3000:3000"` → `"3001:3000"`) |
| Run fails with HTTP 401/403 | Bad `FINNHUB_API_KEY`; fix `.env`, then `docker compose up -d` |
| HTTP 429 in logs | Rate-limited: increase `FINNHUB_INTER_REQUEST_DELAY`, lower symbol count |
| Code location "loading" forever / gRPC errors | `docker compose logs pipeline-code`; a missing env var or syntax error shows there |
| Fresh start / reset everything | `docker compose down -v` (⚠ deletes the stock data volume too), then `up -d --build` |

## Security notes

- All credentials live in `.env` (gitignored) and reach containers only via
  environment variables; `.env.example` documents every variable.
- The Finnhub key is passed to the client through Dagster's `EnvVar`; it is
  never printed in logs (the client logs only the key's last 4 characters).
- PostgreSQL is not exposed to the host network; the Dagster UI (port 3000) is
  the only open door. Add a reverse proxy + auth before putting the UI on a
  shared network.
