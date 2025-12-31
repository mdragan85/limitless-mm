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
print(len(cat.instruments), len(cat.markets))

cat.summary()