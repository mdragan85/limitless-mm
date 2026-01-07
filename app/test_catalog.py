import time

import pandas as pd 

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

from pathlib import Path
from readers.market_catalog.catalog import MarketCatalog
from readers.market_catalog.parsers import LimitlessParser, PolymarketParser

from readers.orderbooks.reader import OrderbookReader
from readers.orderbooks.stream import OrderbookStream
from readers.orderbooks.history import OrderbookHistory


#%%
cat = MarketCatalog.default()


cat.refresh(scan_days=7, all_time=False, use_snapshot=True)


#%%
dfm = cat.markets_df()
dfi = cat.instruments_df()


#%%

# --- Latest BTC 15m from Polymarket ---
pm_btc_15m = max(
    (i for i in cat.instruments.values()
     if i.venue == "polymarket" and i.cadence == "15m" and i.underlying=='BTC'),
    key=lambda i: i.last_seen_ms
)

# --- Latest BTC (any cadence) from Limitless ---
lm_btc_latest = max(
    (i for i in cat.instruments.values()
     if i.venue == "limitless" and i.underlying=='BTC'),
    key=lambda i: i.last_seen_ms
)

print(pm_btc_15m) 
print(lm_btc_latest)



#%%
if False: 
    from readers.market_catalog.instrument_query import InstrumentQuery

    # Build InstrumentQuery directly (no filters assumed)
    q = InstrumentQuery(tuple(cat.instruments.values()))

    # Next 20 expiries
    ids, dbg = q.select(top_n=5, debug=True)
    dbg

    # active only

    ids, dbg = q.is_active(False).select(debug=True)

    for d in dbg:
        print(d)


#%%

from readers.orderbooks.history import OrderbookHistory


hist = OrderbookHistory.from_instrument(lm_btc_latest)  # scans instrument first/last seen dates
hist.instrument          # full InstrumentMeta right here
df = hist.to_dataframe()
df.tail()


# %%
# pick the first snapshot
snap = hist.snapshots[0]

bids, asks = hist._normalize_book(snap)

print("Top of book:")
print("  best bid:", bids[0] if bids else None)
print("  best ask:", asks[0] if asks else None)

print("\nCounts:")
print("  n_bid_levels:", len(bids))
print("  n_ask_levels:", len(asks))

# quick ordering sanity
if len(bids) > 1:
    assert bids[0][0] >= bids[1][0]
if len(asks) > 1:
    assert asks[0][0] <= asks[1][0]

print("\nOrdering sanity passed.")

# %%
from collections import Counter

bid_prices = [px for px, _ in bids]
ask_prices = [px for px, _ in asks]

print("Duplicate bid prices:", [px for px, c in Counter(bid_prices).items() if c > 1])
print("Duplicate ask prices:", [px for px, c in Counter(ask_prices).items() if c > 1])


#%%
df = hist.levels_df()          # L1
df2 = hist.levels_df(5)        # L5 if available

df.head()


# %%

snap = hist.snapshots[0]
bids, asks = hist._normalize_book(snap)

print("n_bid_levels:", len(bids), "min_bid_px:", bids[-1][0] if bids else None, "max_bid_px:", bids[0][0] if bids else None)
print("n_ask_levels:", len(asks), "min_ask_px:", asks[0][0] if asks else None, "max_ask_px:", asks[-1][0] if asks else None)

print("first 5 bids:", bids[:5])
print("first 5 asks:", asks[:5])

#%%
df[["t_ms","bid1_px","ask1_px","mid","spread","micro"]].head()
