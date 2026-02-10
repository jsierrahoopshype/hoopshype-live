# CLAUDE.md

## Project Overview
HoopsHype Live is a browser-based NBA broadcast overlay for 24/7 YouTube streaming.
A Flask backend fetches data; a single-file HTML/CSS/JS frontend displays it at 1920×1080.

## Tech Stack
- **Frontend**: Vanilla HTML/CSS/JS (no build step, no npm). Single file: `index.html`
- **Backend**: Python 3 + Flask. Entry point: `server/app.py`, config: `server/config.py`
- **Fonts**: Google Fonts (Outfit, DM Sans, DM Mono) loaded via CDN
- **No database**: All caching is in-memory via cachetools

## How to Run
```bash
pip install -r requirements.txt
python server/app.py
# Open http://localhost:5000 in Chrome
```

## Project Structure
```
index.html              - The broadcast overlay (all HTML/CSS/JS in one file)
server/app.py           - Flask server, API routes, Bluesky + headlines fetchers
server/config.py        - All configuration (accounts, URLs, intervals, refresh rates)
docs/ARCHITECTURE.md    - Technical architecture
docs/LEGAL.md           - Legal checklist for displayed content
```

## Current State
- The frontend has mock data hardcoded for all sections (scores, feed, ticker, rankings)
- The backend (`server/app.py`) has working endpoint stubs for `/api/bluesky` and `/api/headlines`
- **Next priority**: Wire the frontend to poll `/api/bluesky` and `/api/headlines` instead of using mock data, and validate the Bluesky fetcher + headlines scraper work end-to-end
- After that: Add nba_api integration for live scores (Phase 2), then Google Sheets for rankings (Phase 3)

## Key Context
- The owner (Jorge) works at HoopsHype and has permission to display their headlines
- The overlay targets 1920×1080 @ 60fps for YouTube streaming via OBS
- Clock displays Eastern Time (ET), not local time
- Bluesky feed shows original posts only (no reposts, no replies)
- Ticker label says "LATEST" (not "RUMORS")
- Only recent headlines get a "NEW" badge; older ones have no badge
- The sidebar title is "NBA BLUESKY BUZZ" with the Bluesky butterfly SVG logo

## Code Style
- The frontend is intentionally a single HTML file for simplicity (non-coder user)
- CSS uses custom properties (variables) defined in :root for theming
- JS is vanilla — no frameworks, no modules, no build tools
- Python follows standard Flask patterns; config centralized in `server/config.py`
- Caching uses `cachetools.TTLCache` with stale-while-revalidate fallback

## Important Design Rules
- Dark broadcast aesthetic with HoopsHype orange (#FF8C00) accents
- No empty space on screen — go bold with scores and data
- 3-screen rotation per game: scoreboard → away boxscore → home boxscore
- Scoreboard includes quarter-by-quarter scores (ESPN style)
- Boxscore screens have full scoreboard on top + player stats table with STARTERS/BENCH sections
- Automated LIVE/RANKINGS mode switching (LIVE when games active, RANKINGS 6h after last game)
- Ticker and sidebar text should be large and high-contrast (white/near-white)
- Only one "LIVE" badge in the topbar (the one with the pulsing red dot)
