# MarketCatalog (Metadata Index & Analysis Layer)

The **MarketCatalog** is a **read-only, venue-agnostic metadata index** built from discovery logs written under venue-partitioned `markets/` directories.

It exists to support:

- Offline analysis
- Interactive inspection (e.g. notebooks)
- Downstream selection and readers

**without ever touching order book data.**

> **Key principle**  
> MarketCatalog indexes **markets and instruments**, **not order books**.

---

## What MarketCatalog Is

- A **derived, in-memory view** of discovery metadata
- Cheap to rebuild from disk at any time
- Independent of poller runtime state
- Safe to use in notebooks, batch jobs, and future readers

## What MarketCatalog Is Not

- Not a persistent database
- Not a strategy engine
- Not an order book index
- Not required by the poller hot path

---

## Time Standard

All time in this project is **UTC**.

- Canonical internal representation is **epoch milliseconds** (`*_ms`)
- Any human-facing datetime views are **UTC** (e.g. `*_utc`)
- No local timezone conversions occur inside core objects (only optionally in notebooks)

This eliminates cross-machine ambiguity and makes multi-venue comparisons consistent.

---

## Conceptual Model

### Instrument vs Market

The system distinguishes between:

- **Instrument**  
  One pollable order book stream  
  (e.g. a Limitless CLOB, or a Polymarket YES / NO book)

- **Market**  
  A logical grouping of one or more instruments that resolve together  
  (e.g. Polymarket YES+NO, or a single-book Limitless market)

This distinction is essential because:
- Some venues expose **1 instrument per market**
- Others expose **multiple instruments per market**
- Analysis and selection often operate at the *market* level

---

## MarketCatalog Data Flow

```
DiscoveryService
   ↓
<venue>/markets/date=YYYY-MM-DD/*.jsonl
   ↓
VenueParser (per venue)
   ↓
InstrumentDraft
   ↓
InstrumentAccum / MarketAccum
   ↓
InstrumentMeta / MarketMeta
   ↓
MarketCatalog
```

---

## Inputs

MarketCatalog reads **only metadata**:

- `<venue>/markets/date=YYYY-MM-DD/*.jsonl`
- (optional) `<venue>/state/active_instruments.snapshot.json`

It **never** reads order book logs.

---

## Outputs (In-Memory)

- `InstrumentMeta` — one per pollable order book
- `MarketMeta` — one per logical market

Both are immutable and safe to share.

---

## Core Types

### InstrumentMeta

Represents **one canonical order book stream**.

Key fields:
- `instrument_id = "<venue>:<poll_key>"`
- `venue`
- `poll_key`
- `market_id`
- `slug` (optional)
- `expiration_ms` (**epoch ms, UTC**, unified across venues)
- `title` (optional)
- `underlying` (optional; see notes below)
- `outcome` (optional; e.g. YES/NO or Up/Down)
- `rule` (optional; discovery provenance)
- `cadence` (optional; derived window length; e.g. `15m`, `1h`, `4h`, `1d`, `1w`)
- `is_active` (optional annotation; see *Active Semantics*)
- `first_seen_ms`, `last_seen_ms`
- `extra` (small, venue-specific subset)

**Invariant**  
One `InstrumentMeta` ↔ one pollable order book.

#### Underlying Notes

Some venues do not provide an explicit underlying for all instruments.
For example, Polymarket instruments may require inference from `slug`/`title`
(e.g. `btc-updown-15m-...`) when `underlying` is missing.

Rules:
- Prefer explicit venue fields when present
- Otherwise infer from stable, low-ambiguity tokens in `slug`/`title`
- Keep inference logic **venue-scoped** inside that venue's parser
- Only infer from a **small, explicit allowlist** of known underlyings

---

### MarketMeta

Represents **one logical market**, grouping instruments.

Key fields:
- Identity: `(venue, market_id)`
- `instruments`: tuple of `instrument_id`
- `expiration_ms`
- `cadence`, `underlying`, `rule`
- `is_active` (true if *any* instrument is active)
- Observation window: `first_seen_ms`, `last_seen_ms`

**Invariant**  
A market may have **1 or many instruments**.

---

## Cadence Semantics

`cadence` represents the **contract window length**, not recurrence frequency.

Examples:
- `15m`, `1h`, `4h`, `1d`, `1w`

Derivation rules:
- Venue-specific and isolated in parsers
- Prefer **explicit venue signals** (e.g. tags, encoded window)
- Fall back to slug/title tokens if necessary
- Never infer cadence from time-to-expiry

---

## Active Semantics

`is_active` means:

> *The instrument or market has not yet expired.*

Defaults:
- If snapshot data is present: annotated from snapshot
- Otherwise: inferred via `now_ms < expiration_ms`

Notes:
- `is_active` does **not** imply liquidity or live polling
- It is a **theoretical activity window**, not operational state

---

## Venue Parsers

All venue-specific logic is **fully isolated** in parsers.

Each venue implements:

```python
parse_line(record: dict) -> list[InstrumentDraft]
```

Rules:
- Return `[]` for non-instrument records
- Return one or more `InstrumentDraft` for pollable instruments
- Never mutate shared state
- Never assume storage layout or other venues

---

## Why Drafts Exist

Discovery logs:
- Span multiple days
- Repeat markets and instruments
- May evolve schema over time

`InstrumentDraft` is intentionally **merge-friendly**.  
The catalog later consolidates drafts into canonical metadata.

---

## Snapshot Annotation (Optional)

If present, `active_instruments.snapshot.json` is used to annotate:

- `InstrumentMeta.is_active`
- `MarketMeta.is_active`

Notes:
- Snapshot is treated as read-only
- Catalog remains valid without snapshots
- Snapshot schema is not required for ingestion

---

## Query & Selection Layer (Notebook-First)

Selection is intentionally **separate** from MarketCatalog.

The project provides a **lightweight query helper** (`InstrumentQuery`) that:

- Operates purely in-memory on `InstrumentMeta`
- Performs **filtering, ordering, and truncation**
- Does **not** touch the filesystem
- Does **not** cache or persist state

`InstrumentQuery` is explicitly **not**:
- A database
- A SQL-like DSL
- A required abstraction

It exists solely to:
- Make common notebook selections concise
- Centralize ordering semantics (`top_n`, sort direction)
- Return either:
  - instrument IDs
  - `InstrumentMeta` objects
  - or a human-readable DataFrame view

MarketCatalog remains the authoritative universe; queries merely select subsets.

---

## Relationship to Readers

Downstream readers operate on **InstrumentMeta**, not raw IDs.

Typical flow in notebooks:

```
MarketCatalog
   ↓
InstrumentQuery (selection only)
   ↓
InstrumentMeta
   ↓
OrderbookHistory / OrderbookStream
```

This avoids object juggling and keeps metadata attached to the data it describes.

---

## Orderbook Reading Layers

Orderbook reading is intentionally separate from MarketCatalog.

Typical layering:

- **OrderbookReader** (I/O only)  
  Streams JSONL orderbook records from disk and filters by `instrument_id`.

  Typical layout:
  - `<input_dir>/<venue>/orderbooks/date=YYYY-MM-DD/orderbooks.part-*.jsonl`

- **OrderbookStream** (identity + convenience)  
  Binds an `InstrumentMeta` to an `OrderbookReader` and provides `iter_snapshots(...)`.

- **OrderbookHistory** (in-memory evolution)  
  Materializes a bounded list of snapshots and provides notebook-friendly views
  (e.g. L1/LN DataFrames derived from nested bids/asks).

  `OrderbookHistory` carries its `InstrumentMeta` to avoid object juggling in notebooks.

---

### Timestamps in OrderbookHistory

Orderbook logs may contain:
- `ts_ms`: collector fetch/write time (always present)
- `ob_ts_ms`: venue “as-of” timestamp (present for Polymarket; not universal)

`OrderbookHistory` supports configurable time semantics via:
- `time_field`
- `fallback_time_field`

All ordering, deduplication, and windowing uses a shared
`effective_ts_ms(...)` helper to ensure consistency.

---

## Summary & Health Checks

MarketCatalog provides venue-agnostic summaries to validate ingestion:

- Instruments per venue
- Markets per venue
- Instruments-per-market ratio
- Cadence distribution
- Missing-underlying diagnostics

This catches:
- Missing sides of multi-instrument markets
- Partial ingestion
- Venue schema drift

---

## Design Guarantees

- Adding a new venue requires:
  1. A new `VenueParser`
  2. Registering it with `MarketCatalog`
- No core logic changes required
- Deterministic given the same on-disk logs
- Safe to rebuild at any time

---

## Next Planned Work

- Debugging ingestion completeness (cadence coverage, missing markets)
- Additional orderbook-derived metrics layered on top of L1/LN views
- Optional future distinction between:
  - *theoretical activity* vs
  - *operational availability*
