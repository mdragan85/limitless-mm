import json
from pathlib import Path

from readers.market_catalog.parsers import LimitlessParser


def read_first_line(path: Path) -> str:
    with path.open("r", encoding="utf-8") as handle:
        return handle.readline().strip()



print('--------------------- Limitless Test---------------------------------------')
jsonl_path = Path(
    ".outputs/logs/limitless/markets/date=2025-12-31/markets.part-0089.jsonl"
)
YOUR_JSON_LINE_STRING = read_first_line(jsonl_path)

p = LimitlessParser()
rec = json.loads(YOUR_JSON_LINE_STRING)
drafts = p.parse_line(rec)
print(drafts[0].instrument_id, drafts[0].cadence, drafts[0].expiration_ms)


print('--------------------- Polymarket Test---------------------------------------')
import json
from readers.market_catalog.parsers import PolymarketParser

jsonl_path = Path(
    ".outputs/logs/polymarket/markets/date=2025-12-31/markets.part-0080.jsonl"
)
POLY_STRING = read_first_line(jsonl_path)
print(POLY_STRING)
print(' ')

p = PolymarketParser()
rec = json.loads(POLY_STRING)
d = p.parse_line(rec)[0]
print(d.instrument_id, d.outcome, d.rule, d.cadence, d.expiration_ms)