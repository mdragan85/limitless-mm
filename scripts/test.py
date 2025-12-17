from market_data.active_markets import ActiveMarkets
from pathlib import Path

a = ActiveMarkets(Path("data/state.json"), 120)
a.refresh([])
a.save()
print("OK")