# HoopsHype Live — Architecture

## Tech Stack

- **Frontend**: Single HTML file (vanilla JS, CSS, no build step)
- **Backend**: Python Flask server with in-memory caching
- **Capture**: OBS Studio Browser Source → YouTube RTMP
- **Data**: Bluesky AT Protocol, HoopsHype scraper, nba_api (future), Google Sheets (future)

## Data Flow

```
[Bluesky API]    → Flask /api/bluesky    → Frontend polls every 2min
[HoopsHype.com]  → Flask /api/headlines  → Frontend polls every 3min
[nba_api]        → Flask /api/scores     → Frontend polls every 30s  (Phase 2)
[Google Sheets]  → Flask /api/rankings   → Frontend polls every 5min (Phase 3)
```

## Caching Strategy

1. Flask fetches data on a TTL schedule (configurable per source)
2. Results are cached in memory via `cachetools.TTLCache`
3. If a fetch fails, the last successful result is served ("stale-while-revalidate")
4. The frontend has its own in-memory buffer, so even if the server hiccups the UI stays filled

## Refresh Rates

| Source     | Default  | Configurable in         |
|------------|----------|-------------------------|
| Bluesky    | 120s     | `config.BLUESKY_REFRESH_SECONDS`    |
| Headlines  | 180s     | `config.HEADLINES_REFRESH_SECONDS`  |
| Scores     | 30s      | `config.SCORES_REFRESH_SECONDS`     |
| Rankings   | 300s     | `config.RANKINGS_REFRESH_SECONDS`   |

## Mode Switching (Automated)

- **LIVE mode**: Activates when any NBA game is in progress
- **RANKINGS mode**: Activates 6 hours after the last game of the day ends
- Check runs every 60 seconds on the frontend
