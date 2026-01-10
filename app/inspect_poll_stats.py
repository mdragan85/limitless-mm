import json
import glob
from pathlib import Path
from datetime import datetime

ROOT = Path(".outputs/logs")

def load_stats():
    rows = []
    for venue_dir in ROOT.iterdir():
        if not venue_dir.is_dir():
            continue
        for f in venue_dir.glob("poll_stats/date=*/poll_stats.part-*.jsonl"):
            with open(f) as fh:
                for line in fh:
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        pass
    return rows

rows = load_stats()

# sort by time
rows.sort(key=lambda r: r.get("ts_ms", 0))

print(f"Loaded {len(rows)} stats records\n")

# pretty print recent history
for r in rows[-50:]:
    t = datetime.utcfromtimestamp(r["ts_ms"] / 1000).strftime("%H:%M:%S")
    print(
        f"{t}  {r['venue']:<11}  "
        f"ok={r['successes']:>3}  "
        f"fail={r['failures']:>3}  "
        f"429={r['http_429']:>3}  "
        f"to={r['timeouts']:>3}  "
        f"p50={r['lat_p50_ms'] or '-':>4}ms  "
        f"p95={r['lat_p95_ms'] or '-':>4}ms  "
        f"cooldown={r['cooldown_remaining_s']:>4.0f}s  "
        f"inflight={r['max_inflight']:>2}"
    )
