# Orderbook Record Schema
`record_type` and `schema_version` are top-level fields on every record.
If `schema_version` is missing, readers should treat it as legacy (0) and parse best-effort.

## schema_version = 1

### Required fields (all records)
- record_type: "orderbook"
- schema_version: 1
- venue: string
- poll_key: string
  - Identifier used to poll the venue orderbook
  - Limitless: slug
  - Polymarket: asset_id
- instrument_id: string (canonical: "<venue>:<poll_key>")
- ts_ms: int (epoch ms, collector capture time)


### Optional fields
- ob_ts_ms: int (epoch ms, venue-reported as-of time)
- snapshot_asof: string (ISO-8601, discovery time)
- bids: list
- asks: list
  - Each element is a dict containing at least: price, size
  - Types may vary by venue (readers must coerce)
- best_bid / best_ask / mid / spread
- raw / orderbook (venue-specific payloads)

### Timestamp semantics
- ts_ms is authoritative for ordering and joins
- ob_ts_ms is informational and may be absent
- ISO timestamps are for debugging only

### Evolution rules
- New optional fields may be added without bumping schema_version
- Renaming/removing required fields requires a new schema_version
- Readers must ignore unknown fields
