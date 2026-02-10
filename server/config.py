"""
HoopsHype Live — Configuration
Edit this file to customize your broadcast.
"""

# ═══════════════════════════════════════
# BLUESKY FEED
# ═══════════════════════════════════════

# Bluesky accounts to pull posts from (handle format, no @)
# Add or remove accounts as needed
BLUESKY_ACCOUNTS = [
    # Add your curated list here, e.g.:
    # "wojespn.bsky.social",
    # "shamscharania.bsky.social",
    # "marcstein.bsky.social",
]

BLUESKY_REFRESH_SECONDS = 120        # How often to fetch new posts (2 min)
BLUESKY_MAX_POSTS = 10               # Max posts to display
BLUESKY_SHOW_REPOSTS = False         # False = original posts only
BLUESKY_CACHE_TTL_SECONDS = 90       # Cache lifetime before refetch


# ═══════════════════════════════════════
# HEADLINES TICKER
# ═══════════════════════════════════════

HEADLINES_URL = "https://hoopshype.com/rumors/"
HEADLINES_REFRESH_SECONDS = 180      # How often to scrape (3 min)
HEADLINES_MAX_ITEMS = 20             # Max headlines in ticker
HEADLINES_CACHE_TTL_SECONDS = 150    # Cache lifetime
HEADLINES_NEW_THRESHOLD_MINUTES = 60 # Headlines newer than this get "NEW" badge


# ═══════════════════════════════════════
# LIVE SCORES (Phase 2 — not yet active)
# ═══════════════════════════════════════

SCORES_REFRESH_SECONDS = 30
SCORES_PRIORITY_TEAMS = []           # e.g. ["LAL", "BOS"] — featured more often


# ═══════════════════════════════════════
# RANKINGS / GOOGLE SHEETS (Phase 3 — not yet active)
# ═══════════════════════════════════════

RANKINGS_SHEETS = [
    # {
    #     "name": "Trade Value Rankings",
    #     "url": "https://docs.google.com/spreadsheets/d/SHEET_ID/gviz/tq?tqx=out:json&sheet=TAB_NAME",
    #     "columns": ["Rank", "Player", "Team", "Trade Value"],
    #     "max_rows": 10,
    # },
]
RANKINGS_ROTATE_SECONDS = 15
RANKINGS_REFRESH_SECONDS = 300


# ═══════════════════════════════════════
# SERVER
# ═══════════════════════════════════════

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 5000
DEBUG = True
