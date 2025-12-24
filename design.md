# Market Data Collector – Design Notes

## Architecture Overview
- Single MarketLogger orchestrates all venues
- Each venue is described by a VenueRuntime:
  - name
  - client (API access)
  - normalizer (venue-specific)
  - out_dir (venue-scoped storage)

## Core Principles
- One logger, many venues
- No strategy logic in collectors
- Raw + normalized data are both stored
- Orderbook polling is venue-agnostic

## Discovery Model
### Limitless
- Discover by underlying symbols
- Poll via market slug

### Polymarket
- public-search returns EVENTS, not markets
- Tradable objects are MARKET slugs inside events
- Discovery pipeline:
  search → extract market slugs → hydrate by slug → filter

## Polymarket Assumptions
- Each Yes/No question is its own market
- Each market has two CLOB tokens
- Intraday crypto markets may expire in 15m / 1h / 4h / 1d
- Min expiry enforced per rule (≥ 5 min)

## Rejected Approaches
- Recursive ID scraping from search results
- Mixing venue logic inside MarketLogger
