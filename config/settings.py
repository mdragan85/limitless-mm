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

    # AIMD behavior (start, then slowly probe upward)
    AIMD_START_INFLIGHT: int = 4

    # Per-venue ceilings (hard caps)
    AIMD_INFLIGHT_CEILING_POLY: int = 32
    AIMD_INFLIGHT_CEILING_LIMITLESS: int = 4

    # Polymarket AIMD thresholds
    AIMD_STABLE_SECONDS_POLY: int = 300
    AIMD_ADJUST_MIN_SECONDS_POLY: int = 60
    AIMD_LAT_P95_HIGH_MS_POLY: int = 1500
    AIMD_LAT_P95_LOW_MS_POLY: int = 800
    AIMD_FAIL_RATE_HIGH_POLY: float = 0.25

    # Limitless AIMD thresholds (more conservative)
    AIMD_STABLE_SECONDS_LIMITLESS: int = 600
    AIMD_ADJUST_MIN_SECONDS_LIMITLESS: int = 120
    AIMD_LAT_P95_HIGH_MS_LIMITLESS: int = 2000
    AIMD_LAT_P95_LOW_MS_LIMITLESS: int = 1000
    AIMD_FAIL_RATE_HIGH_LIMITLESS: float = 0.20



# Create a single config instance for import
settings = AppSettings()
