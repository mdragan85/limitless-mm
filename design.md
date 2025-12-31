# Market Data Collector – Design Notes

> Last updated: 2025-12-31 — Discovery rules split from runtime settings; discovery now client-owned per venue

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
- Derive a stable `instrument_key` per instrument (see below)
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

**Current convention:** `discover_fn` is typically a thin wrapper around `client.discover_instruments(rules)` so discovery logic is **owned by the venue client**, not app glue code.

## Configuration Model

This repo distinguishes:

- **Runtime settings** (how the engine runs): `config/settings.py` → `AppSettings`
  - JSONL rotation / fsync cadence
  - discovery cadence
  - poll cadence
  - output directory root
- **Discovery rules** (what markets to watch): venue-scoped rule modules
  - `config/polymarket_rules.py` → `POLYMARKET_RULES`
  - `config/limitless_rules.py` → `LIMITLESS_RULES`

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
- `underlying`
- `title` / `question`
- `outcome` (for venues where a market has multiple books)
- `rule` (discovery rule name / provenance)
- `raw` (venue response payload for audit/debug)

### Instrument Identity

A stable `instrument_key` is defined as:

```
<venue>:<poll_key>
```

Rationale:
- `poll_key` is already present in discovery output
- It is stable and unique per order book
- It avoids venue-specific key derivation logic

This key is used as the primary key in snapshots and all in-memory poller state.

## Snapshot Contract (Discovery → Poller)

Each venue produces:

- `state/active_instruments.snapshot.json`

Shape:

```json
{
  "asof_ts_utc": "2025-12-30T19:40:07.857844",
  "venue": "polymarket",
  "count": 32,
  "instruments": {
    "<instrument_key>": { "<instrument dict>" },
    "...": "..."
  }
}
```

Notes:
- Written via atomic replace (write temp + rename) so the poller never sees partial content
- Poller treats this as read-only “source of truth”
- Poller may keep a cached copy in memory until a newer snapshot arrives

## Venue-Specific Discovery Semantics

### Limitless
- Discovery input: `LIMITLESS_RULES` (currently a list of underlyings; may evolve into structured rules)
- Discovery implementation: `LimitlessVenueClient.discover_instruments(rules)`
- Discovery output: **one instrument per CLOB market**
- Polling:
  - `poll_key = market.slug`
  - `instrument_id = "BOOK"`
- Filtering (current):
  - Exclude AMM markets (e.g., `tradeType != "clob"` or `tokens is None`)
  - Keep funded/active, not-expired markets (as implemented in the client)

### Polymarket
- Discovery input: `POLYMARKET_RULES`
- Discovery implementation: `PolymarketClient.discover_instruments(rules)` (Gamma search + hydration)
- Each market has **two CLOBs** (YES/NO), modeled as **two instruments**
- Polling:
  - `poll_key = asset_id` (token / CLOB identifier)
  - `market_id = Gamma market id`
  - `instrument_id = asset_id`

## Persistence Layout

For each venue under `settings.OUTPUT_DIR` (or env `OUTPUT_DIR`):

- `orderbooks/date=YYYY-MM-DD/`  
  Rotating JSONL logs of order book snapshots

  Notes:
  - Files are named `orderbooks.part-XXXX.jsonl`
  - Part numbers are monotonic across process restarts
  - Downstream consumers must sort by record timestamp for strict ordering

- `markets/date=YYYY-MM-DD/`  
  Discovery cadence JSONL logs of discovered instrument/market metadata

- `state/active_instruments.snapshot.json`  
  Atomic snapshot consumed by the poller

## Current Status

- Limitless: ~8 active CLOB markets (varies with discovery window)
- Polymarket: ~32 instruments (two books per market)
- Total: ~40 order books polled across venues
- Discovery and polling are fully decoupled; poller cadence can be increased independently

## Next Planned Cleanup

- Optional: upgrade `LIMITLESS_RULES` from a simple list to structured rules (match Polymarket pattern)
- Optional: introduce explicit discovery semantics for
  - strict replacement vs
  - grace-window (sticky) active sets
- Optional: downstream compaction or bucketing of order book logs for analytics
