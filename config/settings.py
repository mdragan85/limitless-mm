"""
Application configuration for the Limitless data logger.
Reads settings from environment variables and provides defaults.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load .env file if present (explicit path avoids python-dotenv auto-discovery issues on newer Python)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"  # repo_root/.env if config/ is one level down
load_dotenv(dotenv_path=ENV_PATH, override=False)


@dataclass(frozen=True)
class AppSettings:
    """
    Settings for connecting to the Limitless API and logging behavior.
    """

    # JSonl Writing Settings ------------------------------------------------------
    ROTATE_MINUTES: int = 10            # How often to rotate into a new file
    FSYNC_SECONDS: int = 5              # Force sync file every N seconds (?check?)


    # Discovery Settings ----------------------------------------------------------
    DISCOVER_EVERY_SECONDS: int = 60    # How often to run Discovery

    # Orderbook Logging Settings---------------------------------------------------
    FULL_ORDERBOOK: bool = True         # Full book vs. top of book
    POLL_INTERVAL: float = 0.01         # How long to sleep after polling

    # Output Logs------------------------------------------------------------------
    OUTPUT_DIR: Path = Path(os.getenv("OUTPUT_DIR", ".outputs/logs"))
    INPUT_DIR: Path = Path(os.getenv("INPUT_DIR", ".outputs/logs_server"))

    # Schema versions (Maybe this out to live somewhere else? )
    SCHEMA_VERSION_ORDERBOOK = 1
    SCHEMA_VERSION_MARKETS = 1


    # Poller concurrency (per venue) -------------------------------------------
    POLL_MAX_WORKERS_POLY: int = 16     # threads per venue for orderbook fetch
    POLL_MAX_INFLIGHT_POLY: int = 8     # max concurrent orderbook requests per venue

    POLL_MAX_WORKERS_LIMITLESS: int = 2
    POLL_MAX_INFLIGHT_LIMITLESS: int = 1

    # Orderbook HTTP timeouts (per venue) --------------------------------------
    ORDERBOOK_TIMEOUT_POLY: float = 2.0
    ORDERBOOK_TIMEOUT_LIMITLESS: float = 2.5

    POLL_STATS_EVERY_SECONDS: int = 10          # write one stats record every N seconds
    RATE_LIMIT_COOLDOWN_SECONDS: int = 30       # cooldown on first HTTP 429
    POLL_ERROR_SAMPLE_EVERY: int = 5            # write 1 sampled error every Nth consecutive failure per instrument (0 disables)


    # --- Adaptive throttling (AIMD) ---
    AIMD_ENABLED: bool = True

    # Cooldown when we see 429s (per venue)
    RATE_LIMIT_COOLDOWN_SECONDS: int = 30

    # Periodic stats/errors logging
    POLL_STATS_EVERY_SECONDS: int = 10
    POLL_ERROR_SAMPLE_EVERY: int = 3  # 0 disables; 3 logs every 3rd failure per instrument

    AIMD_ENABLED: bool = True                    # If True, dynamically adjusts per-venue inflight to find max sustainable throughput
    AIMD_START_INFLIGHT: int = 4                 # Starting inflight per venue after launch (AIMD will ramp from here)

    # Hard ceilings so AIMD never exceeds what you consider safe
    AIMD_INFLIGHT_CEILING_POLY: int = 32         # Absolute max inflight Polymarket is allowed to reach
    AIMD_INFLIGHT_CEILING_LIMITLESS: int = 4     # Absolute max inflight Limitless is allowed to reach (recommend low)

    # --- Polymarket AIMD tuning ---
    AIMD_STABLE_SECONDS_POLY: int = 300          # Needs this many seconds of “stable” behavior before increasing inflight
    AIMD_ADJUST_MIN_SECONDS_POLY: int = 60       # Minimum time between AIMD adjustments (prevents oscillation)
    AIMD_LAT_P95_HIGH_MS_POLY: int = 1500        # If p95 latency exceeds this, treat as congestion and decrease inflight
    AIMD_LAT_P95_LOW_MS_POLY: int = 800          # If p95 latency is below this *and* failures low, consider increasing inflight
    AIMD_FAIL_RATE_HIGH_POLY: float = 0.25       # If failures/requests exceeds this, treat as congestion and decrease inflight

    # --- Limitless AIMD tuning (more conservative) ---
    AIMD_STABLE_SECONDS_LIMITLESS: int = 600     # Longer stability window before probing upward (Limitless rate limits feel harsher)
    AIMD_ADJUST_MIN_SECONDS_LIMITLESS: int = 120 # Slow adjustments to avoid triggering bans
    AIMD_LAT_P95_HIGH_MS_LIMITLESS: int = 2000   # High latency threshold for decreasing inflight on Limitless
    AIMD_LAT_P95_LOW_MS_LIMITLESS: int = 1000    # Low latency threshold for increasing inflight on Limitless
    AIMD_FAIL_RATE_HIGH_LIMITLESS: float = 0.20  # Fail-rate threshold for decreasing inflight on Limitless



# Create a single config instance for import
settings = AppSettings()
