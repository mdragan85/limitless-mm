# Timestamp Contract (Orderbook Logs)

This project logs orderbook snapshots across multiple venues. Each record may contain multiple timestamps that represent different clocks. Do not conflate them.

## Required (all venues)

### ts_ms (int)
- Meaning: collector capture time (when our system fetched/wrote the snapshot)
- Units: milliseconds since Unix epoch (UTC)
- Use for: ordering, time-series indexing, joins, and all analysis math

## Human-readable (debug only)

Each venue may also include an ISO-8601 string timestamp for readability:
- Limitless: `ts_utc` (string, treated as UTC by convention)
- Polymarket: `timestamp` (string, treated as UTC by convention)

These are redundant once `ts_ms` exists. Do not rely on these strings for ordering or math.

## Optional (venue-reported)

### ob_ts_ms (int, optional)
- Meaning: venue-reported “as-of” timestamp for the orderbook snapshot (if provided by the venue)
- Units: milliseconds since Unix epoch (UTC)
- Source:
  - Polymarket: parsed from `orderbook.timestamp`
  - Limitless: not available in raw payload → omitted

Use for: measuring venue/transport staleness (e.g., `ts_ms - ob_ts_ms`), diagnostics, and latency analysis.

## Summary

- `ts_ms` = when we saw it (canonical)
- `ob_ts_ms` = when the venue says the book is from (if available)
- ISO strings = for humans only
