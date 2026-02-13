# HoopsHype Live — Broadcast Overlay
## Project Overview
NBA broadcast overlay (1920×1080) for YouTube live streaming. Single-page app with Flask backend serving real-time NBA data, Bluesky social feed, and HoopsHype headlines.
## Tech Stack
- Frontend: Single index.html (all HTML/CSS/JS inline), 1920×1080 fixed layout
- Backend: Flask (server/app.py), Python 3.12
- Data: NBA CDN (scores/boxscores), Bluesky AT Protocol (social feed), Google Sheets (headlines)
- No external JS/CSS libraries
## Current Architecture
Browser (index.html) ←→ Flask (/api/*) ←→ Data Sources
- /api/scores → NBA CDN (todaysScoreboard + boxscores per game)
- /api/bluesky → public.api.bsky.app (365 accounts, parallel fetch, 20 workers)
- /api/headlines → Google Sheets CSV export (column B)
- /api/status → health check
- /api/debug/boxscore → raw CDN data structure (dev only)
## Layout Structure
- Top bar: HoopsHype logo (hoopshype-logo.png served via Flask route) + LIVE badge + date/time
- Main area (left ~75%): Game screens or Rankings screens
- Right sidebar (~25%): NBA Bluesky Buzz feed (real posts with profile photos)
- Bottom: Mini-scores strip + Headlines ticker
## LIVE Mode (games active)
Three screens per game, rotating automatically:
- Screen 0 (9s): Scoreboard + Team Stats (12 stats + lead changes/times tied) + Game Leaders (8 categories: PTS, REB, AST, BLK, STL, 3PT, TO, +/-) + Score Flow SVG chart
- Screen 1 (15s): Away team boxscore (full player names, jersey # after name, no position)
- Screen 2 (15s): Home team boxscore (same format)
Total per-game cycle: 39 seconds, then rotateGame()
Smart game rotation: Live close games (≤10pt) shown 2x more than blowouts, 3:1 live-to-final ratio
## RANKINGS Mode (no live games)
Currently shows mock salary rankings. TODO: Replace with real nba_api data.
## Boxscore Highlights
- Counting stats (PTS, REB, AST, STL, BLK, +/-): highest value wins, tiebreak = fewest minutes
- TO: lowest value wins (fewer turnovers better), tiebreak = fewest minutes
- Percentages (FG%, 3PT%, FT%): best %, min attempts (FG:5, 3PT:3, FT:3), tiebreak = most makes, then fewest minutes
## Key Design Rules
- Dark theme: #0f1923 background, #f07f2e orange highlights, #e8eaed text
- No scrollbars anywhere (YouTube broadcast)
- Team logos from NBA CDN: https://cdn.nba.com/logos/nba/{teamId}/primary/L/logo.svg
- Bluesky avatars: real profile photos with initials fallback
- Quarter scores: winning team highlighted orange per quarter
- Full player names (firstName + familyName), jersey number after name, no position labels
- Player name column 200px with white-space: nowrap
## Configuration (server/config.py)
- 365 Bluesky accounts in BLUESKY_ACCOUNTS list
- HEADLINES_SHEET_ID, HEADLINES_SHEET_GID for Google Sheets source
- SCORES_CACHE_TTL_LIVE = 30s, SCORES_CACHE_TTL_FINAL = 300s
- Bluesky uses public.api.bsky.app (no auth required)
## Backend Patterns
- ThreadPoolExecutor for parallel fetching (20 workers Bluesky, 10 workers boxscores)
- TTLCache with stale-while-revalidate (serve last good data on failure)
- Pre-warm thread populates all caches on startup
- Flask: static_folder=None, threaded=True, app.run(host='0.0.0.0')
- Logo served via dedicated Flask route /hoopshype-logo.png
## Files
- index.html — complete frontend (HTML + CSS + JS)
- server/app.py — Flask API server
- server/config.py — all configuration constants
- server/__init__.py — package marker
- hoopshype-logo.png — header logo
- requirements.txt — flask, flask-cors, requests, cachetools, nba_api
## Common Issues
- NBA CDN boxscore: player status can be null/lowercase/"" — only reject INACTIVE/NOT_WITH_TEAM
- Bluesky: must use public.api.bsky.app not bsky.social (auth required)
- Flask static_folder=".." breaks routing — use static_folder=None
- Windows: venv\Scripts\activate, not source venv/bin/activate
