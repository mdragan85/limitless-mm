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

dfm = cat.markets_df()
dfi = cat.instruments_df()



if False: 
    from readers.market_catalog.instrument_query import InstrumentQuery

    q = InstrumentQuery.from_catalog(cat)

    ids, dbg = (
        q.venues("polymarket")
        .active_only(False)
        .cadence_in("15m", "1h")
        .underlying_in("BTC")
        .select(top_n=25, per_market="one", debug=True)
    )

    len(ids), dbg[:3]