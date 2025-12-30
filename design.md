# Market Data Collector – Design Notes

> Last updated: 2025-12-30 — Polymarket discovery stabilized (start-time gating + per-underlying rules)

## Architecture Overview

The system is a **multi-venue, venue-agnostic market data logger** designed to continuously collect orderbook snapshots from prediction markets.

Core components:

- **MarketLogger**
  - Single orchestrator loop
  - Manages discovery, polling, persistence, and backoff
  - Contains *no venue-specific logic*

- **VenueRuntime**
  - Lightweight descriptor for a single venue
  - Fields:
    - `name` – venue identifier (e.g. `limitless`, `polymarket`)
    - `client` – venue API client
    - `normalizer` – venue-specific snapshot normalizer
    - `out_dir` – venue-scoped output directory
    - `discover_fn` – venue-specific discovery function

- **ActiveInstruments**
  - Persistent state machine of currently active instruments
  - Handles deduplication, expiry pruning, and restart continuity
  - Keyed by a stable `instrument_key`

## Core Principles

- **One logger, many venues**
- **VenueRuntime owns all venue-specific semantics**
- **MarketLogger is fully venue-agnostic**
- **One instrument = one CLOB**
- **Discovery produces instruments, not markets**
- **Polling is driven by opaque `poll_key`**
- **Raw data is always preserved**
- **Normalization is optional and incremental**
- **No strategy, pricing, or execution logic in collectors**

## Instrument Model

All venues emit **instrument dictionaries** with a shared shape.

Minimum required fields:

```json
{
  "venue": "polymarket",
  "market_id": "1007246",
  "instrument_id": "3542261056...",
  "poll_key": "3542261056...",
  "slug": "btc-updown-15m-1766611800",
  "expiration": 1766612700000
}
```

Optional but commonly included:

- `question`
- `underlying`
- `outcome`
- `minutes_to_expiry`
- `rule`
- `raw_market`

### Instrument Identity

`instrument_key` is defined as:

```
<venue>:<market_id>:<instrument_id>
```

## Discovery Model

### Limitless

- Discovery input: underlying symbols
- Discovery output: **one instrument per market**
- Instrument mapping:
  - `instrument_id = "BOOK"`
  - `poll_key = market.slug`

### Polymarket

- Discovery via Gamma `public-search`
- Markets hydrated by slug
- Each market has two CLOBs (YES / NO)
- Slug→rule association preserved
- Start-time gating via `eventStartTime`

## Rule-Based Discovery

Rules are defined per underlying (Option A).

Example:

```python
{
  "name": "crypto_intraday_btc",
  "queries": ["Bitcoin up or down"],
  "min_minutes_to_expiry": 5,
  "max_minutes_to_expiry": 1440,
  "lead_ms": 120_000,
  "start_time_fields": ["eventStartTime"]
}
```

## Current Behavior

- ~4 active markets per asset
- ~32 instruments total
- Logging stable in production
