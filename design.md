# Market Data Collector – Design Notes

> Last updated: 2026-01-07 — Polymarket crypto discovery split into deterministic `/markets` enumeration + legacy search mode

## Architecture Overview

The system is a **multi-venue, venue-agnostic order book collection pipeline** designed to continuously collect and persist order book snapshots from multiple venues (currently **Limitless** + **Polymarket**).

The system is intentionally split into two responsibilities:

- **Discovery (slow path)**
  - Finds and refreshes the “active set” of instruments per venue
  - Writes an **atomic snapshot** file per venue that the poller consumes
  - Writes *market/instrument metadata* logs (optional but enabled)

- **Polling (hot path)**
  - Runs a tight loop polling order books for the current active set
  - Never blocks on discovery
  - Applies per-instrument backoff and per-venue cooldown
  - Writes normalized JSONL order book snapshots with time-based rotation

## Core Components

### MarketLogger (Poller)

A **multi-venue polling service**.

Responsibilities:
- Load per-venue snapshots from discovery (`active_instruments.snapshot.json`)
- Maintain an in-memory dictionary of active instruments (no persisted active state)
- Maintain in-memory backoff per instrument and cooldown per venue
- Poll order books using the venue client and an opaque `poll_key`
- Write JSONL logs under rotating directories

Non-responsibilities:
- Discovery, filtering, or market metadata hydration
- Trading logic (strategy, pricing, execution)

Key properties:
- Snapshot reload is non-fatal (poller never dies due to snapshot issues)
- Snapshot state is treated as read-only and authoritative
- Backoff uses monotonic time to avoid system clock jumps
- Per-venue cooldown prevents one venue outage from blocking the other
- Order book logs may span multiple files per day; ordering is by record timestamp, not file name

### DiscoveryService (Discovery)

A **multi-venue discovery runner**.

Responsibilities:
- Run `discover_fn` for each venue on a slower cadence
- Discover the full active set of instruments for the venue on each run
- Derive a stable `instrument_key` per instrument
- Write:
  - `state/active_instruments.snapshot.json` (atomic overwrite)
  - `markets/` JSONL logs (metadata / instrument records) on discovery cadence

Notes:
- Discovery is the sole owner of active-set determination
- No active state is persisted beyond the snapshot file

### VenueRuntime

Lightweight descriptor for a venue. It is the seam that keeps the system venue-agnostic.

Fields:
- `name` – venue identifier (`limitless`, `polymarket`)
- `client` – venue API client
- `normalizer` – venue-specific snapshot normalizer (optional)
- `out_dir` – venue-scoped output root
- `discover_fn` – venue-specific discovery function (used by DiscoveryService)

**Current convention:** `discover_fn` is typically a thin wrapper around `client.discover_instruments(...)`, so discovery logic is **owned by the venue client**, not app glue code.

## Configuration Model

This repo distinguishes:

- **Runtime settings** (how the engine runs): `config/settings.py` → `AppSettings`
  - JSONL rotation / fsync cadence
  - discovery cadence
  - poll cadence
  - output directory root
- **Discovery rules** (what markets to watch): venue-scoped rule modules
  - `config/polymarket_rules.py`
  - `config/limitless_rules.py`

Environment variables are intentionally minimal. The only supported env override is:

- `OUTPUT_DIR` – absolute path where all venue output is written

## Data Model

### Instrument Dictionary (Shared Shape)

All venues emit **instrument dictionaries** with a common minimum schema:

```json
{
  "venue": "polymarket",
  "market_id": "1059711",
  "instrument_id": "4036821470...",
  "poll_key": "4036821470...",
  "slug": "btc-updown-15m-1767123000",
  "expiration": 1767123900000
}
```

Contract notes:
- `expiration` is **epoch milliseconds** (UTC)
- `poll_key` is the opaque identifier required by the venue client to fetch an order book

Common optional fields:
- `question` / `title`
- `outcome`
- `rule` (discovery provenance)
- `raw_market` (venue payload for audit/debug)

### Instrument Identity

A stable `instrument_key` is defined as:

```
<venue>:<poll_key>
```

Rationale:
- `poll_key` is already present in discovery output
- It is stable and unique per order book
- Avoids venue-specific key derivation logic

This key is used as the primary key in snapshots and all in-memory poller state.

## Snapshot Contract (Discovery → Poller)

Each venue produces:

- `state/active_instruments.snapshot.json`

Shape:

```json
{
  "asof_ts_utc": "2026-01-07T19:40:07.857844",
  "venue": "polymarket",
  "count": 32,
  "instruments": {
    "<instrument_key>": { "<instrument dict>" }
  }
}
```

Notes:
- Written via atomic replace (write temp + rename)
- Poller treats this as read-only source of truth
- Cached in memory until a newer snapshot arrives

## Venue-Specific Discovery Semantics

### Limitless

- Discovery input: `LIMITLESS_RULES`
- Discovery implementation: `LimitlessVenueClient.discover_instruments(rules)`
- Discovery output: **one instrument per CLOB market**
- Polling:
  - `poll_key = market.slug`
  - `instrument_id = "BOOK"`
- Filtering:
  - Exclude AMM markets
  - Keep funded, tradable, non-expired markets

### Polymarket

Polymarket discovery now supports **two explicit modes**, owned by the venue client:

#### 1. Crypto Markets (`mode = "crypto_markets"`)

- Discovery path: **Gamma `GET /markets` pagination**
- Purpose: deterministic, complete enumeration of crypto intraday markets
- Hard filters (source-of-truth):
  - `enableOrderBook == true`
  - `archived == false`
  - `closed == false`
  - `acceptingOrders == true`
- Classification:
  - Uses `events[0].series[0].slug` and `recurrence`
  - Avoids search, titles, or relevance ranking
- Time semantics:
  - `eventStartTime` defines window open
  - `endDate` defines window close
- Output:
  - **Two instruments per market** (YES / NO CLOBs)
  - Schema identical to legacy discovery output

This path is used for **crypto intraday, hourly, and daily markets**, where series metadata is authoritative.

#### 2. Thematic / Search-Based (`mode = "search"`)

- Discovery path: `GET /public-search` → slug hydration via `GET /markets`
- Purpose: thematic, non-crypto, or text-discoverable markets
- Retained for future expansion
- Less deterministic; not used for crypto

Both modes emit **identical instrument dictionaries** and are interchangeable downstream.

## Persistence Layout

For each venue under `settings.OUTPUT_DIR`:

- `orderbooks/date=YYYY-MM-DD/`  
  Rotating JSONL logs of order book snapshots

- `markets/date=YYYY-MM-DD/`  
  Discovery cadence JSONL logs of market / instrument metadata

- `state/active_instruments.snapshot.json`  
  Atomic snapshot consumed by the poller

## Market Metadata Index

Market and instrument metadata is indexed by **MarketCatalog**.

See: `docs/market_catalog.md`
