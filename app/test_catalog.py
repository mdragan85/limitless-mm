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

# %% Next 20 expiries
ids, dbg = q.select(top_n=5, debug=True)
dbg

# %% active only

ids, dbg = q.is_active(True).select(debug=True)
dbg
