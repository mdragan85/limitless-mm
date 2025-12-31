# -------------------------
# Polymarket discovery rules
# -------------------------
# Note: for start_time_fields there is a field called startTime which is the field for most
# markets, and eventStartTime does not exist. These crypto specific markets,  have a startTime corresponding
# to when the market is created but not yet traddeable. it bcomes tradeable at eventStartTime

POLYMARKET_RULES = [
    {
        "name": "crypto_intraday_btc",
        "queries": ["Bitcoin up or down"],
        "min_minutes_to_expiry": 5,
        "max_minutes_to_expiry": 1440,  # < 24h
        "lead_ms": 120_000,
        "start_time_fields": ["eventStartTime", "startTime"],
        "must_contain": [],
        "must_not_contain": [],
    },
    {
        "name": "crypto_intraday_eth",
        "queries": ["Ethereum up or down"],
        "min_minutes_to_expiry": 5,
        "max_minutes_to_expiry": 1440,
        "lead_ms": 120_000,
        "start_time_fields": ["eventStartTime", "startTime"],
        "must_contain": [],
        "must_not_contain": [],
    },
    {
        "name": "crypto_intraday_sol",
        "queries": ["Solana up or down"],
        "min_minutes_to_expiry": 5,
        "max_minutes_to_expiry": 1440,
        "lead_ms": 120_000,
        "start_time_fields": ["eventStartTime", "startTime"],
        "must_contain": [],
        "must_not_contain": [],
    },
    {
        "name": "crypto_intraday_xrp",
        "queries": ["XRP up or down"],
        "min_minutes_to_expiry": 5,
        "max_minutes_to_expiry": 1440,
        "lead_ms": 120_000,
        "start_time_fields": ["eventStartTime", "startTime"],
        "must_contain": [],
        "must_not_contain": [],
    },
]