# Market Data Collector – Design Notes

> Last updated: 2025-12-30 — Discovery/poller split (snapshots + venue runtimes)

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
- Maintain in-memory backoff per instrument and cooldown per venue
- Poll order books using the venue client and an opaque `poll_key`
- Write JSONL logs under rotating directories

Non-responsibilities:
- Discovery, filtering, or market metadata hydration
- Trading logic (strategy, pricing, execution)

Key properties:
- Snapshot reload is non-fatal (poller never dies due to snapshot issues)
- Backoff uses monotonic time to avoid system clock jumps
- Per-venue cooldown prevents one venue outage from blocking the other

### DiscoveryService (Discovery)

A **multi-venue discovery runner**.

Responsibilities:
- Run `discover_fn` for each venue on a slower cadence
- Produce a dict of instruments keyed by `instrument_key`
- Write:
  - `state/active_instruments.snapshot.json` (atomic overwrite)
  - `markets/` JSONL logs (metadata / instrument records) on discovery cadence
  - Optionally: `state/active_instruments.json` (legacy cache; can be removed)

### VenueRuntime

Lightweight descriptor for a venue. It is the seam that keeps the system venue-agnostic.

Fields:
- `name` – venue identifier (`limitless`, `polymarket`)
- `client` – venue API client
- `normalizer` – venue-specific snapshot normalizer (optional)
- `out_dir` – venue-scoped output root
- `discover_fn` – venue-specific discovery function (used by DiscoveryService)

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

Common optional fields:
- `underlying`
- `title` / `question`
- `outcome` (for venues where a market has multiple books)
- `rule` (discovery rule name / provenance)
- `raw` (venue response payload for audit/debug)

### Instrument Identity

A stable `instrument_key` is defined as:

```
<venue>:<market_id>:<instrument_id>
```

This is used as the primary key in snapshots and in-memory state.

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
- Written via atomic replace (write temp + rename) so poller never sees partial content
- Poller treats this as read-only “source of truth”
- Poller may keep a cached copy in memory until a newer snapshot arrives

## Venue-Specific Discovery Semantics

### Limitless
- Discovery input: configured `settings.UNDERLYINGS`
- Discovery output: **one instrument per CLOB market**
- Polling:
  - `instrument_id = "BOOK"`
  - `poll_key = market.slug`
- Filtering (current):
  - Exclude AMM markets (e.g., `tradeType != "clob"` or `tokens is None`)
  - Keep funded, not-expired markets (as configured)

### Polymarket
- Discovery via Gamma search + hydration
- Each market has **two CLOBs** (YES/NO), modeled as **two instruments**
- Polling:
  - `poll_key = asset_id` (token/asset identifier)
  - `market_id = Gamma market id`
  - `instrument_id = asset_id`

## Persistence Layout

For each venue under `settings.OUTPUT_DIR`:

- `orderbooks/date=YYYY-MM-DD/`  
  Rotating JSONL logs of order book snapshots

- `markets/date=YYYY-MM-DD/`  
  Discovery cadence JSONL logs of discovered instrument/market metadata

- `state/active_instruments.snapshot.json`  
  Atomic snapshot consumed by poller

- (optional / legacy) `state/active_instruments.json`  
  Previously used for restart continuity; planned removal in favor of snapshot-only

## Current Status

- Limitless: ~8 active CLOB markets (varies with discovery window)
- Polymarket: ~32 instruments (two books per market)
- Total: ~40 order books polled across venues
- Discovery and polling are decoupled; poller cadence can be increased independently

## Next Planned Cleanup

- Remove `ActiveInstruments` from the poller completely (snapshot-only → in-memory dict)
- Ensure discovery semantics explicitly define whether the active set is:
  - “replace each run” (strict) vs
  - “grace window” (sticky)
