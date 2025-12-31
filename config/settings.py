"""
Application configuration for the Limitless data logger.
Reads settings from environment variables and provides defaults.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from dotenv import load_dotenv

from config.polymarket_rules import POLYMARKET_RULES
from config.limitless_rules import LIMITLESS_RULES

# Load .env file if present (explicit path avoids python-dotenv auto-discovery issues on newer Python)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"  # repo_root/.env if config/ is one level down
load_dotenv(dotenv_path=ENV_PATH, override=False)


@dataclass
class AppSettings:
    """
    Settings for connecting to the Limitless API and logging behavior.
    """

    # JSonl Writing Settings
    ROTATE_MINUTES = 10             # How often to rotate into a new file
    FSYNC_SECONDS = 5               # Force sync file every N seconds (?check?)

    # Discovery Settings
    DISCOVER_EVERY_SECONDS = 60     # How often to run Discovery

    # Orderbook Logging Settings
    FULL_ORDERBOOK = True           # Full book vs. top of book
    POLL_INTERVAL = 1.0              # How often (in seconds) to poll orderbooks

    # Where to write logs
    OUTPUT_DIR: Path = Path(os.getenv("OUTPUT_DIR", ".outputs/logs"))


# Create a single config instance for import
settings = AppSettings()
