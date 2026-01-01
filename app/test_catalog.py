import time

from pathlib import Path
from readers.market_catalog.catalog import MarketCatalog
from readers.market_catalog.parsers import LimitlessParser, PolymarketParser

from readers.orderbooks.reader import OrderbookReader
from readers.orderbooks.stream import OrderbookStream
from readers.orderbooks.history import OrderbookHistory

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

# %% Next 20 expiries
ids, dbg = q.select(top_n=5, debug=True)
dbg

# %% active only

ids, dbg = q.is_active(True).select(debug=True)

for d in dbg:
    print(d)


#%%


reader = OrderbookReader(output_dir=Path(".outputs/logs"))

# pick an instrument
instrument_id = next(iter(cat.instruments.keys()))
meta = cat.instruments[instrument_id]

stream = OrderbookStream(instrument=meta, reader=reader)

now = time.time_ns() // 1_000_000
dates = [time.strftime("%Y-%m-%d")]

snaps = list(stream.iter_snapshots(dates=dates))

hist = OrderbookHistory(
    instrument_id=meta.instrument_id,
    snapshots=snaps,
    time_field="ts_ms",
)

hist.sort_in_place()
df = hist.to_dataframe()
df.head()

# %%
