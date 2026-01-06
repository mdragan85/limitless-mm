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
from readers.market_catalog.instrument_query import InstrumentQuery

# Build InstrumentQuery directly (no filters assumed)
q = InstrumentQuery(tuple(cat.instruments.values()))

# %% Next 20 expiries
ids, dbg = q.select(top_n=5, debug=True)
dbg

# %% active only

ids, dbg = q.is_active(True).select(debug=True)

for d in dbg:
    print(d)


#%%

from readers.orderbooks.history import OrderbookHistory


hist = OrderbookHistory.from_instrument(pm_btc_15m)  # scans instrument first/last seen dates
hist.instrument          # full InstrumentMeta right here
df = hist.to_dataframe()
df.tail()

