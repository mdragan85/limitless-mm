# Markets Record Schema
The schema_version indicates the record contract defined in this document.
It is incremented only when a breaking change is made to required fields
or their semantics.

This document defines the canonical schema contract for `markets.jsonl` records
written by the DiscoveryService.

Markets records describe *static or slowly changing market metadata* and are
distinct from high-frequency orderbook snapshots.

---

## schema_version = 1

### Required identity fields
Each markets record MUST include the following identity fields:

- `venue` : string  
  Venue identifier (e.g. `"limitless"`, `"polymarket"`)

- `poll_key` : string  
  Venue-native identifier used by the orderbook poller

- `instrument_id` : string  
  Canonical identifier, formatted as `"<venue>:<poll_key>"`


```json
{
  "record_type": "market",
  "schema_version": 1
}

Readers MUST:
- tolerate unknown fields
- ignore fields not documented here
- rely only on required fields for identity and joins
