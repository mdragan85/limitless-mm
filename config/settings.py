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
    POLL_MAX_WORKERS_POLY: int = 32     # threads per venue for orderbook fetch
    POLL_MAX_INFLIGHT_POLY: int = 32    # max concurrent orderbook requests per venue

    POLL_MAX_WORKERS_LIMITLESS: int = 8
    POLL_MAX_INFLIGHT_LIMITLESS: int = 2

    # Orderbook HTTP timeouts (per venue) --------------------------------------
    ORDERBOOK_TIMEOUT_POLY: float = 2.0
    ORDERBOOK_TIMEOUT_LIMITLESS: float = 2.5

    POLL_STATS_EVERY_SECONDS: int = 10          # write one stats record every N seconds
    RATE_LIMIT_COOLDOWN_SECONDS: int = 30       # cooldown on first HTTP 429
    POLL_ERROR_SAMPLE_EVERY: int = 5            # write 1 sampled error every Nth consecutive failure per instrument (0 disables)


# Create a single config instance for import
settings = AppSettings()
