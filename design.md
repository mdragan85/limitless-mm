# Market Data Collector – Design Notes

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

## Instrument Model (Unifying Abstraction)

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

This guarantees:
- Stable deduplication
- Safe persistence across restarts
- Venue-independent handling

## Discovery Model

### Limitless

- Discovery input: underlying symbols
- Discovery output: **one instrument per market**
- Instrument mapping:
  - `instrument_id = "BOOK"`
  - `poll_key = market.slug`
- Limitless derives NO implicitly from YES; only one CLOB exists per market

### Polymarket

- `public-search` returns **EVENTS**, not tradable objects
- Tradable objects are **MARKETS embedded inside events**
- Each Polymarket market has **two CLOBs** (YES / NO)
- Discovery pipeline:
  1. Search queries → events
  2. Extract market slugs from events
  3. Hydrate markets by slug
  4. Apply rule-based filters
  5. Emit **one instrument per CLOB token**

Instrument mapping:
- `instrument_id = clobTokenId`
- `poll_key = clobTokenId`
- Orderbooks are fetched from the CLOB API using `token_id`

## Rule-Based Discovery

Discovery is driven by a configurable list of **rules**.

Example rule shape:

```python
{
  "name": "crypto_intraday",
  "queries": ["Bitcoin Up or Down"],
  "min_minutes_to_expiry": 1,
  "max_minutes_to_expiry": 1440,
  "min_minutes_to_start": -5,
  "max_minutes_to_start": 0,
  "must_contain": [],
  "must_not_contain": [],
}
```

Rules may filter on:
- Minutes to expiry
- Minutes to start (to exclude not-yet-opened markets)
- Title + slug substring matching
- Venue-specific metadata (via `raw_market`)

Rules are applied **after full market hydration**, never on search results alone.

## Polling Model

- MarketLogger loops over all VenueRuntimes
- Each venue:
  - Periodically re-discovers instruments
  - Maintains its own ActiveInstruments state
  - Polls orderbooks using `client.get_orderbook(poll_key)`
- `poll_key` is opaque to MarketLogger
- Backoff and cooldown are applied **per venue**, not globally

## Persistence & Storage

Per venue directory structure:

```
<output>/<venue>/
  markets/date=YYYY-MM-DD/
  orderbooks/date=YYYY-MM-DD/
  state/active_instruments.json
```

### Markets Logs
- Instrument metadata
- Written on discovery
- Intended for:
  - Audit
  - Debugging
  - Reconstructing instrument lifetimes

### Orderbooks Logs
- High-frequency snapshots
- Raw or normalized
- One JSONL record per poll

## Normalization

- Normalizers are **venue-specific**
- Normalization is optional
- During development, Polymarket uses a passthrough normalizer
- Raw orderbooks are always stored

## Polymarket-Specific Observations

- Markets may be `active=true` before their start time
- Markets may remain `active=true` after event time while awaiting resolution
- Intraday crypto markets exist at multiple granularities:
  - 15m
  - 1h
  - 4h
  - 1d
- Slug patterns encode series identity and are reliable filters
- CLOB book endpoint returns `{bids, asks}` with `{price, size}` as strings

## Rejected / Avoided Approaches

- Recursive scraping of IDs from search results
- Treating events as tradable objects
- Polling by market slug on Polymarket
- Mixing venue logic inside MarketLogger
- Strategy or trading logic in collectors
- Implicit market → instrument conversions outside discovery

## Open / Future Work

- Series-aware filtering (e.g. 15m vs 1h vs daily)
- Market start-time gating as first-class rule fields
- Deduplicated market metadata logging (log only on new instruments)
- Polymarket orderbook normalization
- Liquidity / volume-based discovery filters
- Additional venues using the same instrument abstraction
