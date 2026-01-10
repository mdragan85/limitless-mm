import time

import pandas as pd 

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

from datetime import datetime, timezone
from pathlib import Path
from readers.market_catalog.catalog import MarketCatalog
from readers.market_catalog.parsers import LimitlessParser, PolymarketParser

from readers.orderbooks.reader import OrderbookReader
from readers.orderbooks.stream import OrderbookStream
from readers.orderbooks.history import OrderbookHistory
from readers.market_catalog.instrument_query import InstrumentQuery


def _ms_to_utc_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

#%%
cat = MarketCatalog.default()
cat.refresh(scan_days=7, all_time=False, use_snapshot=True)

#%%
dfm = cat.markets_df()
dfi = cat.instruments_df()

#%%
q = InstrumentQuery.from_catalog(cat).venues("polymarket").cadence_in("15m").underlying_in("BTC")
df, ims = q.df_and_items(top_n=150, sort_by='expiration_ms', descending=True)

df
#%%
im = ims[3]
print('first seen:', _ms_to_utc_str(im.first_seen_ms))
print('last seen:', _ms_to_utc_str(im.last_seen_ms))

# %%

#%%
ob = OrderbookHistory.from_instrument(im)
df = ob.levels_df(n_levels=1)
df.mid.plot()