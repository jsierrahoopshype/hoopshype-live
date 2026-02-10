# HoopsHype Live

A browser-based NBA broadcast overlay for 24/7 YouTube streaming. Opens in Chrome, captures with OBS, streams to YouTube.

## What It Does

Three-region broadcast layout at 1920×1080 @ 60fps:

- **Main Area** — Live box scores during games (3-screen rotation: scoreboard → away boxscore → home boxscore) or rotating rankings from Google Sheets when no games are on
- **Bottom Ticker** — CNN-style scrolling headlines from HoopsHype Rumors
- **Right Sidebar** — Live NBA Bluesky Buzz feed from curated reporter accounts

## Architecture

```
Browser (index.html)  ←→  Python Server (Flask)  ←→  Data Sources
                              ├── /api/bluesky      → Bluesky AT Protocol
                              ├── /api/headlines     → HoopsHype Rumors
                              ├── /api/scores        → nba_api (Phase 2)
                              ├── /api/rankings      → Google Sheets (Phase 3)
                              └── /api/status        → Health check
```

## Quick Start

### 1. Install dependencies

```bash
python3 -m venv venv
source venv/bin/activate    # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure

Edit `server/config.py` to set your Bluesky accounts list and any other preferences.

### 3. Run the server

```bash
python server/app.py
```

Server starts at `http://localhost:5000`. The broadcast page auto-opens or visit it in Chrome.

### 4. Stream with OBS

1. Add a **Browser Source** in OBS pointing to `http://localhost:5000`
2. Set dimensions to 1920×1080
3. Connect your YouTube stream key and go live

## Project Structure

```
hoopshype-live/
├── index.html              # Broadcast overlay (frontend)
├── requirements.txt        # Python dependencies
├── server/
│   ├── app.py              # Flask server + API endpoints
│   └── config.py           # All configuration (accounts, refresh rates, etc.)
├── docs/
│   ├── ARCHITECTURE.md     # Technical architecture plan
│   └── LEGAL.md            # Legal checklist for displayed content
└── .gitignore
```

## Implementation Phases

- [x] Phase 0 — Layout prototype with mock data
- [ ] Phase 1 — Bluesky feed (AT Protocol public API)
- [ ] Phase 2 — HoopsHype headlines ticker
- [ ] Phase 3 — nba_api live scores + schedule
- [ ] Phase 4 — Google Sheets rankings rotation
- [ ] Phase 5 — Polish, error handling, status dashboard

## Legal

See [docs/LEGAL.md](docs/LEGAL.md) for a full breakdown. Summary: we display public facts (scores), headlines we have permission to use (HoopsHype), public social posts with attribution (Bluesky), and our own data (Google Sheets).
