from pathlib import Path
from readers.market_catalog.catalog import MarketCatalog
from readers.market_catalog.parsers import LimitlessParser, PolymarketParser

cat = MarketCatalog(
    output_dir=Path(".outputs/logs"),
    venues=["limitless", "polymarket"],
    parsers={
        "limitless": LimitlessParser(),
        "polymarket": PolymarketParser(),
    }
)

cat.refresh(scan_days=7, all_time=False, use_snapshot=True)


#%%
dfm = cat.markets_df()
dfi = cat.instruments_df()




#%%
from readers.market_catalog.instrument_query import InstrumentQuery

# Build InstrumentQuery directly (no filters assumed)
q = InstrumentQuery(tuple(cat.instruments.values()))

# --- 1) Basic: select with debug, confirm is_active exists ---
ids, dbg = q.select(top_n=20, debug=True)

print("IDs:", len(ids))
print("Debug rows:", 0 if dbg is None else len(dbg))
print("Sample row keys:", sorted(dbg[0].keys()) if dbg else None)
print("Sample row:", dbg[0] if dbg else None)

assert dbg is not None
assert "is_active" in dbg[0], "is_active missing from debug output"

# --- 2) Determinism test using now_ms ---
first = dbg[0]
exp = first["expiration_ms"]

_, dbg_before = q.select(top_n=1, debug=True, now_ms=exp - 1)
_, dbg_at = q.select(top_n=1, debug=True, now_ms=exp)
_, dbg_after = q.select(top_n=1, debug=True, now_ms=exp + 1)

print("\nBoundary check:")
print("expiration_ms:", exp)
print("now=exp-1  is_active:", dbg_before[0]["is_active"])
print("now=exp    is_active:", dbg_at[0]["is_active"])
print("now=exp+1  is_active:", dbg_after[0]["is_active"])

# Strict definition check
assert dbg_before[0]["is_active"] is True
assert dbg_at[0]["is_active"] is False
assert dbg_after[0]["is_active"] is False

# %%
