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
    POLL_INTERVAL: float = 0.01          # How often (in seconds) to poll orderbooks

    # Output Logs------------------------------------------------------------------
    OUTPUT_DIR: Path = Path(os.getenv("OUTPUT_DIR", ".outputs/logs"))
    INPUT_DIR: Path = Path(os.getenv("INPUT_DIR", ".outputs/logs_server"))

    # Schema versions (Maybe this out to live somewhere else? )
    SCHEMA_VERSION_ORDERBOOK = 1
    SCHEMA_VERSION_MARKETS = 1



    # NEW - Multithreading Poller
    POLL_MAX_WORKERS = 16
    POLL_MAX_INFLIGHT = 16
    ORDERBOOK_TIMEOUT_SECONDS = 2.0

# Create a single config instance for import
settings = AppSettings()
