# MarketCatalog (Metadata Index & Analysis Layer)

The **MarketCatalog** is a **read-only, venue-agnostic metadata index** built from discovery logs written under `markets/`.

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
markets/date=YYYY-MM-DD/*.jsonl
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

- `markets/date=YYYY-MM-DD/*.jsonl`
- (optional) `state/active_instruments.snapshot.json`

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
- `expiration_ms` (epoch ms, unified across venues)
- `outcome` (if applicable)
- `rule` (discovery provenance)
- `cadence` (derived window length; e.g. `1h`, `1d`, `1w`; may be None)
- `is_active` (annotated; see below)
- `first_seen_ms`, `last_seen_ms`
- `extra` (small, venue-specific subset)

**Invariant**  
One `InstrumentMeta` ↔ one pollable order book.

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
- `1h`, `4h`, `1d`, `1w`

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
- Otherwise: inferred via `now < expiration_ms`

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

## Summary & Health Checks

MarketCatalog provides venue-agnostic summaries to validate ingestion:

- Instruments per venue
- Markets per venue
- Instruments-per-market ratio
- Cadence distribution

This catches:
- Missing sides of multi-instrument markets
- Partial ingestion
- Venue schema drift

---

## Relationship to Query & Readers

MarketCatalog is the **entry point** for:

- Query / selection layers
- Orderbook readers
- Research notebooks
- Offline analytics

Downstream components should depend on:
- `MarketMeta`
- `InstrumentMeta`

**Never directly on discovery logs.**

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

- Query layer for selecting markets / instruments
- OrderbookStore / Reader consuming `instrument_id`
- Optional future distinction between:
  - *theoretical activity* vs
  - *operational availability*
