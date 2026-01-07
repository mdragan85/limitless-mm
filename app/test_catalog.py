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
q = InstrumentQuery.from_catalog(cat).venues("polymarket").cadence_in("4h").underlying_in("BTC")
df, ims = q.df_and_items(top_n=10, sort_by='expiration_ms', descending=True)

df
#%%
print(ims[0])
# %%
print('first seen:', _ms_to_utc_str(ims[0].first_seen_ms))
print('last seen:', _ms_to_utc_str(ims[0].last_seen_ms))
# %%



#%% 1st debug step
import json
from pathlib import Path
from config.settings import settings
INPUT_DIR = settings.INPUT_DIR
base = INPUT_DIR / "polymarket" / "markets"

hits = []

for fp in sorted(base.glob("date=*/**/*.jsonl")):
    with fp.open() as f:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)

            raw = rec.get("raw_market") or {}
            series0 = ((raw.get("events") or [{}])[0].get("series") or [{}])[0]

            blob = " ".join([
                str(rec.get("slug", "")),
                str(rec.get("question", "")),
                str(raw.get("question", "")),
                str(series0.get("slug", "")),
                str(series0.get("ticker", "")),
                str(series0.get("title", "")),
            ]).lower()

            if ("btc" in blob or "bitcoin" in blob) and ("4h" in blob or "4 hour" in blob):
                hits.append({
                    "market_id": rec.get("market_id"),
                    "poll_key": rec.get("poll_key"),
                    "slug": rec.get("slug"),
                    "question": rec.get("question"),
                    "expiration": rec.get("expiration"),
                    "date_dir": fp.parent.name,
                })

print("SOL+4h records found:", len(hits))
print("unique market_ids:", len({h["market_id"] for h in hits}))
print("unique poll_keys:", len({h["poll_key"] for h in hits}))
print("sample:", hits[:5])

# %%
slugs = sorted({h["slug"] for h in hits})
print("unique slugs:", len(slugs))
print("\n".join(slugs[:50]))