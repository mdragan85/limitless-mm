# Market Data Collector – Design Notes

> Last updated: 2026-01-10 — Thread-safe, bounded-parallel polling with deterministic backoff and 1 Hz-class throughput

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
  - Fetches order books **in bounded parallel**
  - Writes normalized JSONL order book snapshots with time-based rotation

## Core Components

### MarketLogger (Poller)

A **multi-venue polling service**.

Responsibilities:
- Load per-venue snapshots from discovery (`state/active_instruments.snapshot.json`)
- Maintain an in-memory dictionary of active instruments (poller-owned, non-persistent)
- Maintain in-memory **backoff per instrument** and **cooldown per venue**
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
- All orderbook, stats, and error streams rotate by UTC date
- Writers for all streams roll over atomically at midnight UTC
- Thread pools are shut down cleanly on exit
- Rate-limit events (HTTP 429) are explicitly tracked and trigger venue cooldowns


#### Per-Venue Concurrency & Cooldown

Each venue is polled using its own **thread pool and inflight cap**:

- `max_workers`: size of the venue’s thread pool
- `max_inflight`: hard cap on simultaneous requests for that venue

These limits are **configured per venue** (e.g. Polymarket vs Limitless) so aggressive venues
do not starve or get banned alongside more permissive ones.

The poller also applies **two layers of protection**:

1. **Per-instrument exponential backoff**
   - On failure, an instrument is skipped for an increasing period
   - Prevents broken markets from being hammered

2. **Per-venue cooldown**
   - If a large fraction of instruments fail in one loop
   - Or if HTTP 429 (rate limit) is observed
   - The entire venue is paused for a short time

Cooldowns use **monotonic time** and never block other venues.


## High-Throughput Polling Model (2026-01 Upgrade)

Only the **blocking HTTP fetch** is parallelized using per-venue ThreadPoolExecutors. All state mutation, normalization, and file writes remain single-threaded and deterministic.

## Thread-Safe Venue Clients

- **Polymarket** uses a thread-local `httpx.Client`
- **Limitless** uses a thread-local `requests.Session`

This prevents connection pool corruption while preserving discovery behavior.

## Persistence & Durability

JSONL is used as the append-only source-of-truth. Writers use periodic fsync for durability; per-record flush was removed to enable high throughput.

## Persistence Layout
- `orderbooks/date=YYYY-MM-DD/`
- `markets/date=YYYY-MM-DD/`
- `state/active_instruments.snapshot.json`

### Poller Telemetry & Error Streams

In addition to order book logs, the poller emits **lightweight operational telemetry** to make
rate-limits, outages, and performance diagnosable after the fact.

For each venue under `OUTPUT_DIR`:

- `poll_stats/date=YYYY-MM-DD/`
  - Periodic per-venue polling metrics
  - Includes:
    - number of active instruments
    - submitted / successful / failed polls
    - HTTP 4xx / 5xx / 429 counts
    - timeout counts
    - p50 / p95 latency (rolling window)
    - remaining venue cooldown
    - concurrency limits (workers, inflight)

- `poll_errors/date=YYYY-MM-DD/`
  - Sampled error records for failed orderbook fetches
  - Includes:
    - venue, market_id, slug, instrument_key
    - HTTP status (if available)
    - latency
    - error type + truncated message

These streams are **diagnostic only** and are not part of the market data model.
They allow post-hoc analysis of:
- rate limiting (429 storms)
- venue instability
- network issues
- mis-tuned concurrency

## Adaptive Polling & Rate‑Limit Control (AIMD)

### Motivation

Empirical testing showed that both **Polymarket** and **Limitless** apply opaque,
IP‑based rate limits that vary by venue, time, and burstiness. Static concurrency
settings required frequent manual tuning and led to repeated 429 throttling.

The poller therefore implements **adaptive, self‑calibrating throttling** whose goal is:

> **Maximum sustainable throughput with near‑zero 429s**

Not peak throughput.

---

### Design Overview

Adaptive throttling is implemented using **AIMD (Additive Increase, Multiplicative Decrease)**,
the same control strategy used by TCP congestion control.

Key properties:

- **Per‑venue isolation**  
  Each venue independently adapts its own inflight limit. Polymarket may ramp up while
  Limitless remains conservative.

- **Single control variable**  
  Only `inflight` concurrency is adapted at runtime. Thread pool size remains fixed.

- **Fast backoff, slow probing**  
  - Any observed HTTP 429 → immediate cooldown + inflight halved  
  - Sustained stability → inflight increases slowly (one unit at a time)

- **Stateless across restarts**  
  No persistent calibration state is stored. Each process starts conservatively
  and re‑learns safely.

---

### Control Signals

The AIMD controller uses the following runtime telemetry:

- **HTTP 429 count** (primary congestion signal)
- **Failure rate** (secondary congestion signal)
- **p95 request latency** (early congestion indicator)

All signals are collected per venue.

---

### Adjustment Rules (Simplified)

**Multiplicative Decrease**
- If *any* HTTP 429 occurs:
  - `inflight = max(1, inflight // 2)`
  - Apply per‑venue cooldown
  - Reset stability window

**Additive Increase**
- If *all* conditions hold:
  - No 429s for `AIMD_STABLE_SECONDS_*`
  - Failure rate < ½ failure threshold
  - p95 latency < low‑latency threshold
  - Minimum adjustment interval elapsed
- Then:
  - `inflight += 1` (up to configured ceiling)

**Gentle Decrease**
- If failure rate or p95 latency exceeds high threshold (without 429):
  - `inflight -= 1`

---

### Safety Constraints

- Hard per‑venue ceilings prevent runaway concurrency.
- Cooldowns prevent hammering during partial outages.
- All state mutation remains single‑threaded.
- Network fetches are the *only* parallelized section.

---

### Observability

The poller emits structured telemetry for post‑hoc analysis:

- `poll_stats/` JSONL  
  - successes, failures, HTTP error counts
  - p50 / p95 latency
  - cooldown remaining
  - current inflight limit

- `poll_errors/` JSONL (sampled)
  - status code
  - latency
  - error type and message

These streams allow offline inspection of rate‑limit behavior
and verification that the system converges to a stable operating point.

---

### Rationale for JSONL Retention

Despite high event rates, JSONL remains sufficient because:

- Orderbook records are append‑only
- No strict global ordering is required
- Compression and columnar formats (e.g., Parquet) can be applied downstream
- Debuggability and recovery are superior during active development

Parquet remains a future optimization once schemas and access patterns stabilize.

---

### Summary

This adaptive design replaces manual tuning with a deterministic,
well‑understood control loop that:

- Maximizes sustainable throughput
- Avoids bans and prolonged throttling
- Preserves debuggability and safety
- Scales naturally as venues or market counts change