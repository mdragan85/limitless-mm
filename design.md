# Market Data Collector – Design Notes

> Last updated: 2026-01-07 — Deterministic Polymarket crypto discovery, sticky polling semantics, and change-driven snapshots

## Architecture Overview

The system is a **multi-venue, venue-agnostic order book collection pipeline** designed to continuously collect and persist order book snapshots from multiple venues (currently **Limitless** + **Polymarket**).

The system is intentionally split into two responsibilities:

- **Discovery (slow path)**
  - Finds and refreshes the “active set” of instruments per venue
  - Writes an **atomic snapshot** file per venue that the poller consumes
  - Writes *market / instrument metadata* logs **only when membership changes**

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
- Maintain an in-memory dictionary of active instruments (poller-owned, non-persistent)
- Maintain in-memory backoff per instrument and cooldown per venue
- Poll order books using the venue client and an opaque `poll_key`
- Write JSONL logs under rotating directories

Non-responsibilities:
- Discovery, filtering, or market metadata hydration
- Trading logic (strategy, pricing, execution)

Key properties:
- Snapshot reload is non-fatal (poller never dies due to snapshot issues)
- Snapshot state is treated as read-only and authoritative
- Backoff and cooldown use **monotonic time**
- Per-venue cooldown prevents one venue outage from blocking the other
- Order book logs may span multiple files per day; ordering is by record timestamp
- Instruments are **sticky until expiration**

### DiscoveryService (Discovery)

A **multi-venue discovery runner**.

Responsibilities:
- Run `discover_fn` for each venue on a slower cadence
- Discover the full active set of instruments for the venue on each run
- Derive and enforce a stable `instrument_key` per instrument
- Write:
  - `state/active_instruments.snapshot.json` (atomic overwrite)
  - `markets/` JSONL logs **only when the active set changes**

Notes:
- Discovery is the sole owner of membership determination
- No active state is persisted beyond the snapshot file

## Venue-Specific Discovery Semantics

### Polymarket

- Deterministic crypto discovery via `GET /markets` pagination
- Hard filters: `enableOrderBook`, `acceptingOrders`, not `closed`, not `archived`
- Two instruments per market (YES / NO CLOBs)
- Instruments remain active until expiration passes

### Limitless

- One instrument per CLOB market
- AMM markets excluded
- Funded and tradable markets only

## Persistence Layout

For each venue under `OUTPUT_DIR`:

- `orderbooks/date=YYYY-MM-DD/`
- `markets/date=YYYY-MM-DD/`
- `state/active_instruments.snapshot.json`

## Market Metadata Index

Market and instrument metadata is indexed by **MarketCatalog**.

See: `docs/market_catalog.md`
