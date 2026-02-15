"""
HoopsHype Live — Flask Server
Serves the broadcast page and API endpoints for live data.

Phase 1: Bluesky feed + HoopsHype headlines (via Google Sheets)
Phase 2: Live NBA scores via nba.com CDN
Phase 3: Google Sheets rankings (TODO)
"""

import csv
import io
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import requests
from cachetools import TTLCache

import config

# ─── Setup ───
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # hoopshype-live/

# static_folder=None prevents Flask from registering a catch-all static route
# (static_folder=".." was generating a broken /../<path:filename> route that shadowed /api/ endpoints)
app = Flask(__name__, static_folder=None)
CORS(app)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hoopshype-live")

# ─── Caches ───
bluesky_cache = TTLCache(maxsize=1, ttl=config.BLUESKY_CACHE_TTL_SECONDS)
headlines_cache = TTLCache(maxsize=1, ttl=config.HEADLINES_CACHE_TTL_SECONDS)
# Scores cache uses dynamic TTL — start with live game TTL, rebuilt as needed
scores_cache = TTLCache(maxsize=1, ttl=config.SCORES_CACHE_TTL_LIVE)
salaries_cache = TTLCache(maxsize=1, ttl=1800)  # 30 min TTL

# Fallback data (served when fetch fails)
last_good_bluesky = []
last_good_headlines = []
last_good_scores = []
last_good_salaries = {"rankings": [], "teams": {}, "count": 0}

# Cross-reference lookups (populated by fetch_salaries / fetch_depth)
_player_salary_map = {}   # "LeBron James" → "$46.7M"
_full_name_map = {}       # "L. James" → "LeBron James", etc.
_player_team_map = {}     # "LeBron James" → "Los Angeles Lakers" (from depth charts)

# HTTP request defaults (no shared Session — requests.Session is NOT thread-safe
# and we call from 20+ concurrent ThreadPoolExecutor workers)
_HTTP_HEADERS = {"User-Agent": "HoopsHypeLive/1.0"}


# ═══════════════════════════════════════
# BLUESKY FEED
# ═══════════════════════════════════════

BLUESKY_MAX_WORKERS = 20  # concurrent threads for fetching feeds


def _fetch_one_feed(handle):
    """Fetch recent posts for a single Bluesky handle. Returns list of post dicts."""
    posts = []
    try:
        # Public API — no auth required (bsky.social/xrpc requires auth)
        feed_url = (
            f"https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed"
            f"?actor={handle}&limit=5&filter=posts_no_replies"
        )
        resp = requests.get(feed_url, headers=_HTTP_HEADERS, timeout=8)
        resp.raise_for_status()
        feed = resp.json().get("feed", [])

        for item in feed:
            post = item.get("post", {})
            record = post.get("record", {})
            author = post.get("author", {})

            # Skip reposts
            if not config.BLUESKY_SHOW_REPOSTS and item.get("reason"):
                continue

            # Skip replies (belt-and-suspenders: filter should exclude, but check anyway)
            if record.get("reply"):
                continue

            text = record.get("text", "").strip()
            if not text:
                continue

            created = record.get("createdAt", "")

            posts.append({
                "author": author.get("displayName", handle),
                "handle": f"@{author.get('handle', handle)}",
                "avatar": _initials(author.get("displayName", handle)),
                "avatarUrl": author.get("avatar", ""),
                "text": text,
                "time": _time_ago(created),
                "timestamp": created,
            })

    except Exception as e:
        log.debug(f"Bluesky fetch failed for {handle}: {e}")

    return posts


def fetch_bluesky_posts():
    """Fetch recent posts from configured Bluesky accounts via public API (parallelized)."""
    global last_good_bluesky

    # Return cached if available
    if "posts" in bluesky_cache:
        return bluesky_cache["posts"]

    log.info(f"Fetching Bluesky feeds for {len(config.BLUESKY_ACCOUNTS)} accounts...")

    all_posts = []
    success_count = 0
    fail_count = 0
    with ThreadPoolExecutor(max_workers=BLUESKY_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_one_feed, handle): handle
            for handle in config.BLUESKY_ACCOUNTS
        }
        for future in as_completed(futures):
            handle = futures[future]
            try:
                posts = future.result()
                if posts:
                    all_posts.extend(posts)
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                log.debug(f"Bluesky worker error for {handle}: {e}")

    log.info(f"Bluesky fetch done: {success_count} accounts returned posts, {fail_count} empty/failed")

    # Sort by timestamp (newest first) and limit
    all_posts.sort(key=lambda p: p.get("timestamp", ""), reverse=True)
    all_posts = all_posts[:config.BLUESKY_MAX_POSTS]

    if all_posts:
        last_good_bluesky = all_posts
        bluesky_cache["posts"] = all_posts
        with_avatar = sum(1 for p in all_posts if p.get("avatarUrl"))
        log.info(
            f"Cached {len(all_posts)} Bluesky posts "
            f"({with_avatar}/{len(all_posts)} have profile photos, "
            f"newest: {all_posts[0].get('author', '?')})"
        )
    else:
        log.warning("No Bluesky posts fetched — check network or API endpoint")

    return last_good_bluesky


def _time_ago(iso_str):
    """Convert ISO timestamp to '5m', '2h', etc."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        diff = (now - dt).total_seconds()
        if diff < 60:
            return "now"
        elif diff < 3600:
            return f"{int(diff // 60)}m"
        elif diff < 86400:
            return f"{int(diff // 3600)}h"
        else:
            return f"{int(diff // 86400)}d"
    except Exception:
        return ""


def _initials(name):
    """Get 2-letter initials from a display name."""
    parts = name.strip().split()
    if len(parts) >= 2:
        return (parts[0][0] + parts[-1][0]).upper()
    elif parts:
        return parts[0][:2].upper()
    return "??"


# ═══════════════════════════════════════
# HOOPSHYPE HEADLINES (Google Sheets)
# ═══════════════════════════════════════

def fetch_headlines():
    """Fetch headlines from a public Google Sheet (CSV export).

    The sheet is the single source of truth for ticker headlines.
    Column B contains headline text; first N rows get a NEW badge.
    """
    global last_good_headlines

    if "headlines" in headlines_cache:
        return headlines_cache["headlines"]

    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{config.HEADLINES_SHEET_ID}"
        f"/export?format=csv&gid={config.HEADLINES_SHEET_GID}"
    )
    log.info(f"Fetching headlines from Google Sheet: {csv_url}")

    try:
        resp = requests.get(csv_url, timeout=15)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Google Sheets headlines fetch failed: {e}")
        return last_good_headlines

    # Parse CSV — column B is index 1 (0-indexed)
    col_index = ord(config.HEADLINES_COLUMN.upper()) - ord("A")
    reader = csv.reader(io.StringIO(resp.text))
    items = []

    for row_num, row in enumerate(reader):
        if len(row) <= col_index:
            continue
        text = row[col_index].strip()
        # Clean encoding artifacts (Â, non-breaking spaces, etc.)
        text = text.replace('\u00a0', ' ').replace('\u00c2', '').replace('\xc2', '')
        text = ' '.join(text.split())  # collapse multiple spaces
        if not text:
            continue
        # Skip header row (first row if it looks like a label)
        if row_num == 0 and text.lower() in ("headline", "headlines", "text", "title", "rumor", "rumors"):
            continue

        items.append({
            "text": text,
            "time": "",
            "isNew": len(items) < config.HEADLINES_NEW_COUNT,
        })

        if len(items) >= config.HEADLINES_MAX_ITEMS:
            break

    if items:
        last_good_headlines = items
        headlines_cache["headlines"] = items
        new_count = sum(1 for h in items if h["isNew"])
        log.info(f"Cached {len(items)} headlines from Google Sheet ({new_count} marked NEW)")
    else:
        log.warning("Google Sheet returned no usable headlines")

    return last_good_headlines


# ═══════════════════════════════════════
# LIVE NBA SCORES (Phase 2)
# ═══════════════════════════════════════

# NBA CDN headers — mimic browser to avoid 403
_NBA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json",
}


def _parse_game_clock(iso_duration):
    """Convert ISO 8601 duration (PT04M32.00S) → '4:32'. Returns '' if empty/invalid."""
    if not iso_duration or iso_duration in ("PT00M00.00S", ""):
        return ""
    m = re.match(r"PT(\d+)M([\d.]+)S", iso_duration)
    if m:
        minutes = int(m.group(1))
        seconds = int(float(m.group(2)))
        return f"{minutes}:{seconds:02d}"
    return ""


def _period_label(period, game_status):
    """Convert period number + status → display label like '3RD QTR', 'HALFTIME', 'FINAL'."""
    if game_status == 3:
        return "FINAL"
    if game_status == 1:
        return "SCHEDULED"
    # game_status == 2 → live
    if period == 0:
        return "PREGAME"
    ordinals = {1: "1ST", 2: "2ND", 3: "3RD", 4: "4TH"}
    if period in ordinals:
        return f"{ordinals[period]} QTR"
    return f"OT{period - 4}" if period > 4 else f"Q{period}"


def _game_status_str(game_status):
    """Map nba_api gameStatus int → frontend status string."""
    return {1: "scheduled", 2: "live", 3: "final"}.get(game_status, "scheduled")


def _format_record(wins, losses):
    """Format team record string."""
    return f"{wins}-{losses}"


def _build_quarters(periods_list, total_periods=4):
    """Convert nba periods array → quarter score arrays for away/home.

    nba_api periods: [{"period": 1, "periodType": "REGULAR", "score": 28}, ...]
    Frontend expects: [28, 31, null, null] for a 2nd-quarter game.
    """
    scores = [None] * max(total_periods, len(periods_list))
    for p in periods_list:
        idx = p.get("period", 1) - 1
        if 0 <= idx < len(scores):
            scores[idx] = p.get("score", 0)
    return scores[:max(total_periods, len(periods_list))]


def _leader_name(full_name):
    """Convert 'Jayson Tatum' → 'J. Tatum'."""
    if not full_name:
        return "—"
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return f"{parts[0][0]}. {parts[-1]}"
    return full_name


def _fetch_boxscore(game_id):
    """Fetch detailed boxscore for a single game from nba.com CDN."""
    url = config.SCORES_BOXSCORE_URL.format(game_id=game_id)
    try:
        resp = requests.get(url, headers=_NBA_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # Log raw response structure for debugging
        top_keys = list(data.keys()) if isinstance(data, dict) else type(data).__name__
        log.debug(f"Boxscore raw response keys for {game_id}: {top_keys}")

        game_data = data.get("game", {})
        if game_data:
            game_keys = list(game_data.keys())
            log.debug(f"Boxscore game keys for {game_id}: {game_keys}")

            # Check if team data exists at expected path
            ht = game_data.get("homeTeam")
            at = game_data.get("awayTeam")
            if ht:
                ht_players = len(ht.get("players", []))
                ht_stats = bool(ht.get("statistics"))
                log.debug(f"  homeTeam: {ht_players} players, has stats: {ht_stats}")
            else:
                log.warning(f"  homeTeam missing from boxscore {game_id}! Available keys: {game_keys}")
            if at:
                at_players = len(at.get("players", []))
                log.debug(f"  awayTeam: {at_players} players")
            else:
                log.warning(f"  awayTeam missing from boxscore {game_id}!")
        else:
            log.warning(f"Boxscore 'game' key empty/missing for {game_id}, raw keys: {top_keys}")

        return game_data
    except Exception as e:
        log.debug(f"Boxscore fetch failed for {game_id}: {e}")
        return None


def _transform_player(player):
    """Transform a single player from nba_api boxscore format → frontend format."""
    stats = player.get("statistics", {})
    fg_made = stats.get("fieldGoalsMade", 0)
    fg_att = stats.get("fieldGoalsAttempted", 0)
    three_made = stats.get("threePointersMade", 0)
    three_att = stats.get("threePointersAttempted", 0)
    ft_made = stats.get("freeThrowsMade", 0)
    ft_att = stats.get("freeThrowsAttempted", 0)

    # Minutes: "PT24M30.00S" → "24" (just the integer minutes)
    minutes_iso = stats.get("minutesCalculated", "") or stats.get("minutes", "")
    min_match = re.match(r"PT(\d+)M", minutes_iso) if minutes_iso else None
    minutes = min_match.group(1) if min_match else "0"

    plus_minus = stats.get("plusMinusPoints", 0)
    pm_val = int(plus_minus) if isinstance(plus_minus, float) else plus_minus
    pm_str = f"+{pm_val}" if pm_val > 0 else str(pm_val)

    # Full name: prefer firstName + familyName, fall back to nameI
    first = player.get("firstName", "")
    family = player.get("familyName", "")
    full_name = f"{first} {family}" if first and family else (player.get("nameI", "") or "—")

    return {
        "num": player.get("jerseyNum", ""),
        "name": full_name,
        "pos": player.get("position", ""),
        "min": minutes,
        "pts": stats.get("points", 0),
        "reb": stats.get("reboundsTotal", 0),
        "ast": stats.get("assists", 0),
        "stl": stats.get("steals", 0),
        "blk": stats.get("blocks", 0),
        "fg": f"{fg_made}-{fg_att}",
        "three": f"{three_made}-{three_att}",
        "ft": f"{ft_made}-{ft_att}",
        "pm": pm_str,
        "to": stats.get("turnovers", 0),
    }


def _transform_team_boxscore(team_data):
    """Transform nba_api team boxscore → frontend boxscore format (starters + bench)."""
    players = team_data.get("players", [])
    if not players:
        log.warning("_transform_team_boxscore: no players in team_data")
        return {"starters": [], "bench": []}

    # Log first player structure for debugging
    p0 = players[0]
    log.debug(
        f"Boxscore first player: status={p0.get('status')!r}, "
        f"starter={p0.get('starter')!r}, played={p0.get('played')!r}, "
        f"name={p0.get('nameI', p0.get('name', '?'))}"
    )

    starters = []
    bench = []
    skipped = 0
    for p in players:
        # Skip players who didn't play — accept ACTIVE, or any player with played="1"
        status = (p.get("status") or "").upper()
        played = p.get("played", "")
        if status in ("INACTIVE", "NOT_WITH_TEAM"):
            skipped += 1
            continue
        # Also skip if status is explicitly set to something that's not ACTIVE/empty
        # but the player has played="0" (did not actually play)
        if played == "0" and status != "ACTIVE":
            skipped += 1
            continue

        transformed = _transform_player(p)

        # Handle starter field (can be "1", 1, True)
        starter_val = p.get("starter", "")
        if starter_val in ("1", 1, True):
            starters.append(transformed)
        else:
            bench.append(transformed)

    if skipped:
        log.debug(f"Boxscore: skipped {skipped} inactive players, kept {len(starters)} starters + {len(bench)} bench")

    return {"starters": starters, "bench": bench}


def _team_stats_from_boxscore(team_data):
    """Extract team-level stats from boxscore → frontend stats format."""
    stats = team_data.get("statistics", {})

    # Bench points: prefer CDN value, fall back to computing from non-starter players
    bench_pts = stats.get("benchPoints", None)
    if bench_pts is None:
        players = team_data.get("players", [])
        bench_pts = sum(
            p.get("statistics", {}).get("points", 0)
            for p in players
            if p.get("starter", "") not in ("1", 1, True)
            and (p.get("status", "") or "").upper() not in ("INACTIVE", "NOT_WITH_TEAM")
        )

    return {
        "fgPct": f"{stats.get('fieldGoalsPercentage', 0) * 100:.1f}",
        "threePct": f"{stats.get('threePointersPercentage', 0) * 100:.1f}",
        "ftPct": f"{stats.get('freeThrowsPercentage', 0) * 100:.1f}",
        "reb": stats.get("reboundsTotal", 0),
        "ast": stats.get("assists", 0),
        "stl": stats.get("steals", 0),
        "blk": stats.get("blocks", 0),
        "to": stats.get("turnovers", 0),
        "fastBreak": stats.get("fastBreakPointsMade", stats.get("pointsFastBreak", 0)),
        "paint": stats.get("pointsInThePaint", stats.get("pointsInThePaintMade", 0)),
        "benchPts": bench_pts,
        "biggestLead": stats.get("biggestLead", 0),
    }


def _leaders_from_scoreboard(leaders_data):
    """Transform gameLeaders from scoreboard → frontend leaders format.

    Scoreboard only has a single leader per team, so extended stats get placeholders.
    """
    name = leaders_data.get("name", "") or "—"
    _empty = {"name": "—", "val": 0}
    return {
        "pts": {"name": name, "val": leaders_data.get("points", 0)},
        "reb": {"name": name, "val": leaders_data.get("rebounds", 0)},
        "ast": {"name": name, "val": leaders_data.get("assists", 0)},
        "blk": _empty.copy(),
        "stl": _empty.copy(),
        "threepm": _empty.copy(),
        "to": _empty.copy(),
        "pm": _empty.copy(),
    }


def _leaders_from_boxscore_players(players):
    """Compute game leaders from boxscore player stats (more accurate than scoreboard leaders).

    Returns leaders for 8 categories: PTS, REB, AST, BLK, STL, 3PM, TO, +/-.
    """
    def _find_leader(stat_key):
        return max(players, key=lambda p: p.get("statistics", {}).get(stat_key, 0), default={})

    def _leader(player, stat_key):
        first = player.get("firstName", "")
        family = player.get("familyName", "")
        name = f"{first} {family}".strip() if first and family else (player.get("nameI", "") or "—")
        val = player.get("statistics", {}).get(stat_key, 0)
        if isinstance(val, float):
            val = int(val)  # plusMinusPoints comes as float from CDN
        return {"name": name, "val": val}

    return {
        "pts": _leader(_find_leader("points"), "points"),
        "reb": _leader(_find_leader("reboundsTotal"), "reboundsTotal"),
        "ast": _leader(_find_leader("assists"), "assists"),
        "blk": _leader(_find_leader("blocks"), "blocks"),
        "stl": _leader(_find_leader("steals"), "steals"),
        "threepm": _leader(_find_leader("threePointersMade"), "threePointersMade"),
        "to": _leader(_find_leader("turnovers"), "turnovers"),
        "pm": _leader(_find_leader("plusMinusPoints"), "plusMinusPoints"),
    }


def _transform_game(sb_game, boxscore_data=None):
    """Transform a single game from nba_api scoreboard + boxscore → frontend MOCK_GAMES format."""
    game_status = sb_game.get("gameStatus", 1)
    period = sb_game.get("period", 0)
    game_clock = _parse_game_clock(sb_game.get("gameClock", ""))

    away_team = sb_game.get("awayTeam", {})
    home_team = sb_game.get("homeTeam", {})

    # Quarter scores from scoreboard
    away_quarters = _build_quarters(away_team.get("periods", []))
    home_quarters = _build_quarters(home_team.get("periods", []))

    # Build team sides
    def build_side(team, box_team, leaders_sb):
        side = {
            "teamId": team.get("teamId", 0),
            "abbr": team.get("teamTricode", ""),
            "city": team.get("teamCity", "").upper(),
            "name": team.get("teamName", ""),
            "record": _format_record(team.get("wins", 0), team.get("losses", 0)),
            "score": team.get("score", 0),
        }

        # Leaders: prefer boxscore (per-stat leader) over scoreboard (single leader for all)
        if box_team and box_team.get("players"):
            side["leaders"] = _leaders_from_boxscore_players(box_team["players"])
        elif leaders_sb:
            side["leaders"] = _leaders_from_scoreboard(leaders_sb)
        else:
            side["leaders"] = {
                "pts": {"name": "—", "val": 0},
                "reb": {"name": "—", "val": 0},
                "ast": {"name": "—", "val": 0},
                "blk": {"name": "—", "val": 0},
                "stl": {"name": "—", "val": 0},
                "threepm": {"name": "—", "val": 0},
                "to": {"name": "—", "val": 0},
                "pm": {"name": "—", "val": 0},
            }

        # Stats and boxscore: from boxscore data if available
        _empty_stats = {"fgPct": "0.0", "threePct": "0.0", "ftPct": "0.0", "reb": 0, "ast": 0, "stl": 0, "blk": 0, "to": 0, "fastBreak": 0, "paint": 0, "benchPts": 0, "biggestLead": 0}
        _empty_boxscore = {"starters": [], "bench": []}
        if box_team and box_team.get("statistics"):
            side["stats"] = _team_stats_from_boxscore(box_team)
        else:
            side["stats"] = _empty_stats
        if box_team and box_team.get("players"):
            side["boxscore"] = _transform_team_boxscore(box_team)
        else:
            side["boxscore"] = _empty_boxscore

        return side

    # Boxscore team data
    box_away = None
    box_home = None
    if boxscore_data:
        box_away = boxscore_data.get("awayTeam")
        box_home = boxscore_data.get("homeTeam")
        game_id = sb_game.get("gameId", "?")
        away_p = len(box_away.get("players", [])) if box_away else 0
        home_p = len(box_home.get("players", [])) if box_home else 0
        log.info(
            f"Game {game_id}: boxscore has "
            f"awayTeam={'yes' if box_away else 'NO'}({away_p}p), "
            f"homeTeam={'yes' if box_home else 'NO'}({home_p}p)"
        )

    # Scoreboard leaders
    game_leaders = sb_game.get("gameLeaders", {})
    away_leaders = game_leaders.get("awayLeaders", {})
    home_leaders = game_leaders.get("homeLeaders", {})

    # Arena/venue
    arena = ""
    if boxscore_data and boxscore_data.get("arena"):
        a = boxscore_data["arena"]
        arena = f"{a.get('arenaName', '')}, {a.get('arenaCity', '')}"
    elif sb_game.get("arenaName"):
        arena = f"{sb_game.get('arenaName', '')}, {sb_game.get('arenaCity', '')}"

    # Status text for scheduled games (e.g. "7:00 pm ET")
    status_text = sb_game.get("gameStatusText", "").strip()

    # Game-wide stats (leadChanges and timesTied are the same for both teams)
    game_stats = {"leadChanges": 0, "timesTied": 0}
    if boxscore_data:
        for side_key in ("homeTeam", "awayTeam"):
            side_stats = boxscore_data.get(side_key, {}).get("statistics", {})
            if "leadChanges" in side_stats:
                game_stats["leadChanges"] = side_stats.get("leadChanges", 0)
                game_stats["timesTied"] = side_stats.get("timesTied", 0)
                break

    return {
        "id": sb_game.get("gameId", ""),
        "status": _game_status_str(game_status),
        "period": _period_label(period, game_status),
        "periodNum": period,
        "clock": game_clock if game_status == 2 else ("" if game_status == 3 else status_text),
        "venue": arena,
        "quarters": {
            "away": away_quarters,
            "home": home_quarters,
        },
        "gameStats": game_stats,
        "away": build_side(away_team, box_away, away_leaders),
        "home": build_side(home_team, box_home, home_leaders),
    }


def fetch_scores():
    """Fetch today's NBA scores from nba.com CDN with boxscore details."""
    global last_good_scores, scores_cache

    if "scores" in scores_cache:
        return scores_cache["scores"]

    log.info("Fetching NBA scores from cdn.nba.com...")

    try:
        resp = requests.get(config.SCORES_SCOREBOARD_URL, headers=_NBA_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Scoreboard fetch failed: {e}")
        return last_good_scores

    sb = data.get("scoreboard", {})
    games_raw = sb.get("games", [])

    if not games_raw:
        log.info("No NBA games today")
        scores_cache["scores"] = []
        last_good_scores = []
        return []

    log.info(f"Found {len(games_raw)} games today, fetching boxscores...")

    # Fetch boxscores in parallel for live/final games
    boxscores = {}
    games_needing_box = [
        g for g in games_raw
        if g.get("gameStatus", 1) >= 2  # live or final
    ]

    if games_needing_box:
        with ThreadPoolExecutor(max_workers=min(len(games_needing_box), 10)) as executor:
            future_to_id = {
                executor.submit(_fetch_boxscore, g["gameId"]): g["gameId"]
                for g in games_needing_box
            }
            for future in as_completed(future_to_id):
                gid = future_to_id[future]
                try:
                    result = future.result()
                    if result:
                        boxscores[gid] = result
                except Exception as e:
                    log.debug(f"Boxscore worker error for {gid}: {e}")

    log.info(f"Fetched {len(boxscores)}/{len(games_needing_box)} boxscores")

    # Transform all games
    games = []
    for g in games_raw:
        gid = g.get("gameId", "")
        box = boxscores.get(gid)
        games.append(_transform_game(g, box))

    # Determine cache TTL: shorter if any games are live
    has_live = any(g["status"] == "live" for g in games)
    ttl = config.SCORES_CACHE_TTL_LIVE if has_live else config.SCORES_CACHE_TTL_FINAL
    scores_cache = TTLCache(maxsize=1, ttl=ttl)
    scores_cache["scores"] = games
    last_good_scores = games

    live_count = sum(1 for g in games if g["status"] == "live")
    final_count = sum(1 for g in games if g["status"] == "final")
    sched_count = sum(1 for g in games if g["status"] == "scheduled")
    log.info(
        f"Cached {len(games)} NBA games "
        f"(live: {live_count}, final: {final_count}, scheduled: {sched_count}, "
        f"cache TTL: {ttl}s)"
    )

    return games


# ═══════════════════════════════════════
# API ROUTES
# ═══════════════════════════════════════

# ═══════════════════════════════════════
# TEAM SALARIES (Google Sheets)
# ═══════════════════════════════════════

_SALARIES_SHEET_ID = "11llk0icQqoi0JwJXat5KO8y2RQeBMN5rS9FhAY56Idc"
_SALARIES_GID = "0"

# Official NBA 2025-26 international players → ISO country code
# Source: pr.nba.com/international-players-2025-26-nba-rosters/
_PLAYER_COUNTRY = {
    "Dyson Daniels": "au", "Josh Green": "au", "Josh Giddey": "au",
    "Lachlan Olbrich": "au", "Luke Travers": "au", "Tyrese Proctor": "au",
    "Dante Exum": "au", "Kyrie Irving": "au", "Alex Toohey": "au",
    "Johnny Furphy": "au", "Jock Landale": "au", "Joe Ingles": "au",
    "Rocco Zikarsky": "au", "Jakob Poeltl": "at",
    "Buddy Hield": "bs", "Deandre Ayton": "bs", "VJ Edgecombe": "bs",
    "Ajay Mitchell": "be", "Toumani Camara": "be",
    "Jusuf Nurkic": "ba", "Gui Santos": "br",
    "Pascal Siakam": "cm", "Christian Koloko": "cm",
    "Yves Missi": "cm", "Joel Embiid": "cm",
    "Caleb Houstan": "ca", "Nickeil Alexander-Walker": "ca",
    "Emanuel Miller": "ca", "Dwight Powell": "ca", "Ryan Nembhard": "ca",
    "Jamal Murray": "ca", "Jackson Rowe": "ca", "Andrew Nembhard": "ca",
    "Bennedict Mathurin": "ca", "Brandon Clarke": "ca",
    "Olivier-Maxence Prosper": "ca", "Zach Edey": "ca",
    "Andrew Wiggins": "ca", "Leonard Miller": "ca",
    "Luguentz Dort": "ca", "Dillon Brooks": "ca",
    "Shaedon Sharpe": "ca", "Kelly Olynyk": "ca",
    "RJ Barrett": "ca", "AJ Lawson": "ca", "Will Riley": "ca",
    "Jahmyl Telfort": "ca", "Shai Gilgeous-Alexander": "ca",
    "Yang Hansen": "cn",
    "Karlo Matkovic": "hr", "Ivica Zubac": "hr", "Dario Saric": "hr",
    "Vit Krejci": "cz",
    "Jonathan Kuminga": "cd", "Bismack Biyombo": "cd", "Oscar Tshiebwe": "cd",
    "Al Horford": "do", "David Jones Garcia": "do",
    "Lauri Markkanen": "fi",
    "Zaccharie Risacher": "fr", "Nolan Traore": "fr",
    "Tidjane Salaun": "fr", "Moussa Diabate": "fr",
    "Noa Essengue": "fr", "Nicolas Batum": "fr",
    "Joan Beringer": "fr", "Rudy Gobert": "fr",
    "Guerschon Yabusele": "fr", "Mohamed Diawara": "fr",
    "Pacome Dadiet": "fr", "Ousmane Dieng": "fr",
    "Rayan Rupert": "fr", "Sidy Cissoko": "fr", "Maxime Raynaud": "fr",
    "Victor Wembanyama": "fr", "Alex Sarr": "fr", "Alexandre Sarr": "fr",
    "Bilal Coulibaly": "fr", "Noah Penda": "fr",
    "Goga Bitadze": "ge", "Sandro Mamukelashvili": "ge",
    "Maxi Kleber": "de", "Ariel Hukporti": "de",
    "Isaiah Hartenstein": "de", "Tristan da Silva": "de",
    "Dennis Schroder": "de", "Franz Wagner": "de", "Moritz Wagner": "de",
    "Giannis Antetokounmpo": "gr", "Thanasis Antetokounmpo": "gr",
    "Alex Antetokounmpo": "gr",
    "Moussa Cisse": "gn",
    "Ben Saraf": "il", "Deni Avdija": "il",
    "Simone Fontecchio": "it", "Nick Richards": "jm", "Rui Hachimura": "jp",
    "Kristaps Porzingis": "lv",
    "Jonas Valanciunas": "lt", "Kasparas Jakucionis": "lt",
    "Domantas Sabonis": "lt",
    "N'Faly Dante": "ml", "Nikola Vucevic": "me", "Quinten Post": "nl",
    "Steven Adams": "nz", "Adem Bona": "ng", "Josh Okogie": "ng",
    "Neemias Queta": "pt", "Egor Demin": "ru", "Vladislav Goldin": "ru",
    "Eli Ndiaye": "sn", "Mouhamed Gueye": "sn",
    "Nikola Jokic": "rs", "Bogdan Bogdanovic": "rs", "Nikola Jovic": "rs",
    "Nikola Topic": "rs", "Tristan Vukcevic": "rs", "Nikola Djurisic": "rs",
    "Luka Doncic": "si",
    "Khaman Maluach": "ss", "Duop Reath": "ss",
    "Hugo Gonzalez": "es", "Santi Aldama": "es",
    "Chris Boucher": "lc",
    "Bobi Klintman": "se", "Pelle Larsson": "se",
    "Clint Capela": "ch", "Yanic Konan Niederhauser": "ch", "Kyshawn George": "ch",
    "Alperen Sengun": "tr",
    "Svi Mykhailiuk": "ua", "Max Shulga": "ua",
    "Amari Williams": "gb", "OG Anunoby": "gb",
    "Tosan Evbuomwan": "gb", "Jeremy Sochan": "gb",
}


def fetch_salaries():
    """Fetch player salary data from HoopsHype Google Sheet, grouped by team."""
    global last_good_salaries

    if "salaries" in salaries_cache:
        return salaries_cache["salaries"]

    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{_SALARIES_SHEET_ID}"
        f"/export?format=csv&gid={_SALARIES_GID}"
    )
    log.info("Fetching team salaries from Google Sheet...")

    try:
        resp = requests.get(csv_url, timeout=15)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Salaries fetch failed: {e}")
        return last_good_salaries

    def parse_money(val):
        try:
            clean = val.replace("$", "").replace(",", "").replace('"', '').strip()
            if not clean or clean == '-':
                return 0
            return int(clean)
        except (ValueError, AttributeError):
            return 0

    reader = csv.reader(io.StringIO(resp.text))
    teams_dict = {}

    for row_num, row in enumerate(reader):
        if row_num == 0:
            continue
        if len(row) < 6:
            continue
        player = row[0].strip()
        team = row[2].strip()
        if not player or not team:
            continue

        salary = parse_money(row[3])
        status = row[5].strip() if len(row) > 5 else ""
        salary_next = parse_money(row[6]) if len(row) > 6 else 0
        salary_next_display = row[6].strip() if len(row) > 6 else ""
        cap_space = row[10].strip() if len(row) > 10 else ""
        team_status = row[9].strip() if len(row) > 9 else ""
        country = _PLAYER_COUNTRY.get(player, "")

        if team not in teams_dict:
            teams_dict[team] = {
                "team": team, "players": [], "total": 0, "totalNext": 0,
                "capSpace": cap_space, "teamStatus": team_status,
            }

        teams_dict[team]["players"].append({
            "name": player, "salary": salary,
            "salaryDisplay": row[3].strip(),
            "salaryNextDisplay": salary_next_display,
            "status": status, "country": country,
        })
        teams_dict[team]["total"] += salary
        teams_dict[team]["totalNext"] += salary_next

        # Build cross-reference lookups
        sal_display = row[3].strip()
        _player_salary_map[player] = sal_display
        # Build abbreviated → full name map ("N. Alexander-Walker" → "Nickeil Alexander-Walker")
        parts = player.split(" ", 1)
        if len(parts) == 2 and len(parts[0]) > 0:
            abbr = parts[0][0] + ". " + parts[1]
            _full_name_map[abbr] = player

    teams_list = list(teams_dict.values())
    teams_list.sort(key=lambda t: t["total"], reverse=True)
    for i, t in enumerate(teams_list):
        t["rank"] = i + 1
        t["totalDisplay"] = f"${t['total']:,}"
        t["totalNextDisplay"] = f"${t['totalNext']:,}"
        t["playerCount"] = len(t["players"])
        t["players"].sort(key=lambda p: p["salary"], reverse=True)

    result = {
        "rankings": [{
            "rank": t["rank"], "team": t["team"],
            "totalDisplay": t["totalDisplay"],
            "totalNextDisplay": t["totalNextDisplay"],
            "capSpace": t["capSpace"], "teamStatus": t["teamStatus"],
            "playerCount": t["playerCount"],
        } for t in teams_list],
        "teams": {t["team"]: {
            "team": t["team"], "rank": t["rank"],
            "totalDisplay": t["totalDisplay"],
            "totalNextDisplay": t["totalNextDisplay"],
            "capSpace": t["capSpace"], "teamStatus": t["teamStatus"],
            "players": t["players"],
        } for t in teams_list},
        "count": len(teams_list),
    }

    if teams_list:
        salaries_cache["salaries"] = result
        last_good_salaries = result
        log.info(f"Cached {len(teams_list)} teams' salaries (highest: {teams_list[0]['team']} {teams_list[0]['totalDisplay']})")

    return result


# ═══════════════════════════════════════
# GLOBAL RATINGS (Google Sheets)
# ═══════════════════════════════════════

_RATINGS_SHEET_ID = "15sz5Quun4k86N-XEXvbXU9D5BrLg_26z7PtuH-T5bP8"
_RATINGS_GID = "1342397740"

ratings_cache = TTLCache(maxsize=1, ttl=1800)  # 30 min
last_good_ratings = []

# Column blocks: (start_col, title, num_players)
_RATINGS_BLOCKS = [
    (3,  "Global Rating — Last 365 Days", 20),
    (14, "Global Rating — Season", 20),
    (25, "Global Rating — Rookies", 20),
    (36, "Global Rating — International", 20),
    (47, "Global Rating — Sixth Man of the Year", 20),
    (58, "Global Rating — Last 7 Days", 20),
    (80, "Global Rating — Last 30 Days", 20),
    (91, "Global Rating — Most In Form", 20),
]


def fetch_ratings():
    """Fetch Global Rating data from Google Sheet, returning ranking screens."""
    global last_good_ratings

    if "ratings" in ratings_cache:
        return ratings_cache["ratings"]

    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{_RATINGS_SHEET_ID}"
        f"/export?format=csv&gid={_RATINGS_GID}"
    )
    log.info("Fetching Global Ratings from Google Sheet...")

    try:
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Ratings fetch failed: {e}")
        return last_good_ratings

    reader = list(csv.reader(io.StringIO(resp.text)))
    if len(reader) < 2:
        return last_good_ratings

    headers = reader[0]
    screens = []

    for start_col, title, num_players in _RATINGS_BLOCKS:
        # Determine column layout
        # Standard: PLAYER(+0), RAT(+1), G(+2), PTS(+3), REB(+4), AST(+5), extra(+6)
        is_form = start_col == 91  # "Most In Form" has different layout: PLAYER, RAT, 2024-25, DIFF

        players = []
        for row_idx in range(1, len(reader)):
            if len(players) >= num_players:
                break
            row = reader[row_idx]
            if start_col >= len(row):
                continue

            name = row[start_col].strip() if start_col < len(row) else ""
            if not name:
                continue

            rat = row[start_col + 1].strip() if start_col + 1 < len(row) else ""
            country = _PLAYER_COUNTRY.get(name, "")

            if is_form:
                # Cols: PLAYER, RAT (current), 2024-25 (old), DIFF
                old_rat = row[start_col + 2].strip() if start_col + 2 < len(row) else ""
                diff = row[start_col + 3].strip() if start_col + 3 < len(row) else ""
                players.append({
                    "rank": len(players) + 1,
                    "name": name,
                    "rating": rat,
                    "oldRating": old_rat,
                    "diff": diff,
                    "country": country,
                })
            else:
                games = row[start_col + 2].strip() if start_col + 2 < len(row) else ""
                pts = row[start_col + 3].strip() if start_col + 3 < len(row) else ""
                reb = row[start_col + 4].strip() if start_col + 4 < len(row) else ""
                ast = row[start_col + 5].strip() if start_col + 5 < len(row) else ""
                players.append({
                    "rank": len(players) + 1,
                    "name": name,
                    "rating": rat,
                    "games": games,
                    "pts": pts,
                    "reb": reb,
                    "ast": ast,
                    "country": country,
                })

        if players:
            screens.append({
                "title": title,
                "isForm": is_form,
                "players": players,
            })

    if screens:
        ratings_cache["ratings"] = screens
        last_good_ratings = screens
        log.info(f"Cached {len(screens)} rating screens")

    return screens


@app.route("/api/ratings")
def api_ratings():
    """Return Global Rating ranking screens."""
    data = fetch_ratings()
    return jsonify({"screens": data, "count": len(data)})


# ═══════════════════════════════════════
# TEAM RATINGS (cross-ref Ratings + Depth Charts)
# ═══════════════════════════════════════

team_ratings_cache = TTLCache(maxsize=1, ttl=1800)  # 30 min
last_good_team_ratings = []


def fetch_team_ratings():
    """Fetch Season ratings, one screen per team with all rostered players."""
    global last_good_team_ratings

    if "tr" in team_ratings_cache:
        return team_ratings_cache["tr"]

    # Need depth chart data for player→team mapping
    if not _player_team_map:
        log.warning("Team ratings: player_team_map not populated yet, skipping")
        return last_good_team_ratings

    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{_RATINGS_SHEET_ID}"
        f"/export?format=csv&gid={_RATINGS_GID}"
    )
    log.info("Fetching team ratings from Google Sheet...")

    try:
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Team ratings fetch failed: {e}")
        return last_good_team_ratings

    reader = list(csv.reader(io.StringIO(resp.text)))
    if len(reader) < 5:
        return last_good_team_ratings

    # Parse Season block (col 14): PLAYER, RAT, G, PTS, REB, AST
    season_col = 14
    rated_players = {}  # name → player dict
    for ri in range(1, len(reader)):
        row = reader[ri]
        if season_col >= len(row):
            continue
        name = row[season_col].strip()
        if not name:
            break
        try:
            rat = float(row[season_col + 1].strip())
        except (ValueError, IndexError):
            continue

        games = row[season_col + 2].strip() if season_col + 2 < len(row) else ""
        pts = row[season_col + 3].strip() if season_col + 3 < len(row) else ""
        reb = row[season_col + 4].strip() if season_col + 4 < len(row) else ""
        ast = row[season_col + 5].strip() if season_col + 5 < len(row) else ""

        rated_players[name] = {
            "rating": rat,
            "games": games,
            "pts": pts,
            "reb": reb,
            "ast": ast,
        }

    # Build team screens using depth chart rosters
    teams = {}
    for player_name, team_name in _player_team_map.items():
        if team_name not in teams:
            teams[team_name] = []
        rp = rated_players.get(player_name)
        teams[team_name].append({
            "rank": 0,  # will be set after sort
            "name": player_name,
            "rating": f"{rp['rating']:.2f}" if rp else "—",
            "ratingNum": rp["rating"] if rp else 0,
            "games": rp["games"] if rp else "",
            "pts": rp["pts"] if rp else "",
            "reb": rp["reb"] if rp else "",
            "ast": rp["ast"] if rp else "",
            "country": _PLAYER_COUNTRY.get(player_name, ""),
        })

    # Sort players within each team by rating desc, assign ranks
    team_list = []
    for team_name, players in teams.items():
        players.sort(key=lambda x: x["ratingNum"], reverse=True)
        for i, p in enumerate(players):
            p["rank"] = i + 1
        rated = [p for p in players if p["ratingNum"] > 0]
        avg_rat = sum(p["ratingNum"] for p in rated) / len(rated) if rated else 0
        team_list.append({
            "name": team_name,
            "avgRating": round(avg_rat, 2),
            "avgDisplay": f"{avg_rat:.2f}",
            "ratedCount": len(rated),
            "players": players,
        })

    # Sort teams by avg rating desc, assign rank
    team_list.sort(key=lambda t: t["avgRating"], reverse=True)
    for i, t in enumerate(team_list):
        t["rank"] = i + 1

    # One screen per team
    screens = []
    for t in team_list:
        screens.append({
            "title": f"#{t['rank']} {t['name']}",
            "subtitle": f"Team Avg: {t['avgDisplay']} ({t['ratedCount']} rated players)",
            "isForm": False,
            "players": t["players"],
        })

    if screens:
        team_ratings_cache["tr"] = screens
        last_good_team_ratings = screens
        log.info(f"Cached {len(screens)} team rating screens ({len(rated_players)} rated players matched)")

    return screens


@app.route("/api/team_ratings")
def api_team_ratings():
    """Return team rating screens."""
    data = fetch_team_ratings()
    return jsonify({"screens": data, "count": len(data)})


# ═══════════════════════════════════════
# INJURY REPORTS (Google Sheets)
# ═══════════════════════════════════════

_INJURIES_SHEET_ID = "14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY"
_INJURIES_GID = "306285159"

injuries_cache = TTLCache(maxsize=1, ttl=900)  # 15 min
last_good_injuries = []


def fetch_injuries():
    """Fetch injury report data from Google Sheet, grouped by team."""
    global last_good_injuries

    if "injuries" in injuries_cache:
        return injuries_cache["injuries"]

    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{_INJURIES_SHEET_ID}"
        f"/export?format=csv&gid={_INJURIES_GID}"
    )
    log.info("Fetching injury reports from Google Sheet...")

    try:
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Injuries fetch failed: {e}")
        return last_good_injuries

    reader = list(csv.reader(io.StringIO(resp.text)))
    if len(reader) < 2:
        return last_good_injuries

    # Cols 17-23: Team, Player, _, Status, Injury, Date, GameStatus
    teams = {}  # team -> [players]
    for row_idx in range(1, len(reader)):
        row = reader[row_idx]
        if len(row) < 24:
            continue

        team = row[17].strip()
        player = row[18].strip()
        status = row[20].strip()       # Out, Available
        injury = row[21].strip()       # Injury description
        date = row[22].strip()         # Date
        game_status = row[23].strip()  # Out, Questionable, Probable

        if not team or not player:
            continue
        # Skip fully healthy players
        if not injury and status == "Available" and game_status == "Available":
            continue
        # Skip "Available" game status with no injury note
        if game_status == "Available" and not injury:
            continue

        if team not in teams:
            teams[team] = []

        teams[team].append({
            "name": player,
            "status": game_status or status,
            "injury": injury,
            "date": date,
            "salary": _player_salary_map.get(player, ""),
            "country": _PLAYER_COUNTRY.get(player, ""),
        })

    # Build two-column screens: left and right columns, ~15 rows each
    MAX_PER_COL = 14
    # First build a flat list of team blocks (header + players)
    team_blocks = []
    for team_name in sorted(teams.keys()):
        team_players = teams[team_name]
        block = [{"isHeader": True, "team": team_name}]
        for p in team_players:
            block.append(p)
        team_blocks.append(block)

    # Pack into columns, then pair columns into screens
    columns = []
    current_col = []
    current_rows = 0
    for block in team_blocks:
        needed = len(block)
        if current_col and current_rows + needed > MAX_PER_COL:
            columns.append(current_col)
            current_col = []
            current_rows = 0
        current_col.extend(block)
        current_rows += needed
    if current_col:
        columns.append(current_col)

    # Pair columns into screens (left + right)
    screens = []
    for i in range(0, len(columns), 2):
        left = columns[i]
        right = columns[i + 1] if i + 1 < len(columns) else []
        screens.append({
            "title": "Injury Report",
            "left": left,
            "right": right,
        })

    if screens:
        injuries_cache["injuries"] = screens
        last_good_injuries = screens
        total_injured = sum(1 for b in team_blocks for p in b if not p.get("isHeader"))
        log.info(f"Cached {len(screens)} injury screens ({total_injured} players across {len(teams)} teams)")

    return screens


@app.route("/api/injuries")
def api_injuries():
    """Return injury report screens."""
    from datetime import datetime
    from zoneinfo import ZoneInfo
    data = fetch_injuries()
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return jsonify({
        "screens": data,
        "count": len(data),
        "lastUpdated": now_et.strftime("%b %d, %I:%M %p ET"),
    })


# ═══════════════════════════════════════
# DEPTH CHARTS (Google Sheets)
# ═══════════════════════════════════════

_DEPTH_SHEET_ID = "14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY"
_DEPTH_GID = "24771201"

depth_cache = TTLCache(maxsize=1, ttl=1800)  # 30 min
last_good_depth = []

_POSITIONS = ["PG", "SG", "SF", "PF", "C"]
_MAX_LEVELS = 10  # effectively unlimited
_TEAMS_PER_SCREEN = 2

# Teams appear alphabetically in the depth chart sheet
_NBA_TEAMS_ALPHA = [
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets",
    "Chicago Bulls", "Cleveland Cavaliers", "Dallas Mavericks", "Denver Nuggets",
    "Detroit Pistons", "Golden State Warriors", "Houston Rockets", "Indiana Pacers",
    "LA Clippers", "Los Angeles Lakers", "Memphis Grizzlies", "Miami Heat",
    "Milwaukee Bucks", "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks",
    "Oklahoma City Thunder", "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns",
    "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors",
    "Utah Jazz", "Washington Wizards",
]


def fetch_depth():
    """Fetch depth chart data from Google Sheet, packed 3 teams per screen."""
    global last_good_depth

    if "depth" in depth_cache:
        return depth_cache["depth"]

    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{_DEPTH_SHEET_ID}"
        f"/export?format=csv&gid={_DEPTH_GID}"
    )
    log.info("Fetching depth charts from Google Sheet...")

    try:
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Depth charts fetch failed: {e}")
        return last_good_depth

    import re
    reader = list(csv.reader(io.StringIO(resp.text)))
    if len(reader) < 5:
        return last_good_depth

    # Find header rows (PG/SG/SF/PF/C) to delimit team blocks
    header_rows = []
    for ri, row in enumerate(reader):
        if (len(row) >= 5 and
            row[0].strip() == "PG" and row[1].strip() == "SG" and
            row[2].strip() == "SF" and row[3].strip() == "PF" and
            row[4].strip() == "C"):
            header_rows.append(ri)

    log.info(f"Depth charts: found {len(header_rows)} header rows, {len(_NBA_TEAMS_ALPHA)} teams expected")

    all_teams = []
    for hi, hrow in enumerate(header_rows):
        if hi >= len(_NBA_TEAMS_ALPHA):
            break
        team_name = _NBA_TEAMS_ALPHA[hi]
        end = header_rows[hi + 1] if hi + 1 < len(header_rows) else len(reader)

        # Parse depth levels: pairs of (name_row, salary_row)
        levels = []
        ri = hrow + 1
        while ri < end and len(levels) < _MAX_LEVELS:
            row = reader[ri] if ri < len(reader) else []
            if len(row) < 5:
                ri += 1
                continue
            cols = [row[i].strip() if i < len(row) else "" for i in range(5)]
            if not any(cols):
                ri += 1
                continue
            # Detect position header rows (PG/SG/SF/PF/C) - fuzzy match
            pos_set = {"PG", "SG", "SF", "PF", "C"}
            if sum(1 for c in cols if c in pos_set) >= 3:
                break

            is_salary = any(c.startswith("$") for c in cols if c)
            if not is_salary and any(cols):
                names = cols
                salaries = ["", "", "", "", ""]
                if ri + 1 < end:
                    next_row = reader[ri + 1] if ri + 1 < len(reader) else []
                    next_cols = [next_row[j].strip() if j < len(next_row) else "" for j in range(5)]
                    if any(c.startswith("$") for c in next_cols if c):
                        salaries = next_cols
                        ri += 1

                level_idx = len(levels)
                label = "Starters" if level_idx == 0 else "Bench" if level_idx == 1 else "Out"
                level = {"label": label, "players": []}
                for pi in range(5):
                    if names[pi]:
                        full_name = _full_name_map.get(names[pi], names[pi])
                        level["players"].append({
                            "pos": _POSITIONS[pi],
                            "name": full_name,
                            "salary": salaries[pi],
                            "country": _PLAYER_COUNTRY.get(full_name, _PLAYER_COUNTRY.get(names[pi], "")),
                        })
                if level["players"]:
                    levels.append(level)

            ri += 1

        if levels:
            all_teams.append({"name": team_name, "levels": levels})

    # Build player→team cross-reference for team ratings
    _player_team_map.clear()
    for t in all_teams:
        for level in t["levels"]:
            for p in level["players"]:
                _player_team_map[p["name"]] = t["name"]
    log.info(f"Depth charts: built player→team map with {len(_player_team_map)} players")

    # Pack teams into screens
    screens = []
    for i in range(0, len(all_teams), _TEAMS_PER_SCREEN):
        batch = all_teams[i:i + _TEAMS_PER_SCREEN]
        screens.append({
            "title": "Depth Charts",
            "teamNames": [t["name"] for t in batch],
            "teams": batch,
        })

    if screens:
        depth_cache["depth"] = screens
        last_good_depth = screens
        log.info(f"Cached {len(all_teams)} teams into {len(screens)} depth chart screens")

    return screens


@app.route("/api/depth")
def api_depth():
    """Return depth chart screens (multiple teams per screen)."""
    data = fetch_depth()
    return jsonify({"screens": data, "count": len(data)})


@app.route("/")
def serve_index():
    """Serve the broadcast overlay page."""
    return send_from_directory(PROJECT_ROOT, "index.html")


@app.route("/hoopshype-logo.png")
def serve_logo():
    """Serve the HoopsHype logo image."""
    return send_from_directory(PROJECT_ROOT, "hoopshype-logo.png")


@app.route("/api/bluesky")
def api_bluesky():
    """Return latest Bluesky posts."""
    posts = fetch_bluesky_posts()
    return jsonify({"posts": posts, "count": len(posts)})


@app.route("/api/headlines")
def api_headlines():
    """Return latest HoopsHype headlines."""
    headlines = fetch_headlines()
    return jsonify({"headlines": headlines, "count": len(headlines)})


@app.route("/api/scores")
def api_scores():
    """Return today's NBA game scores with full boxscore data."""
    games = fetch_scores()
    has_live = any(g["status"] == "live" for g in games)
    return jsonify({
        "games": games,
        "count": len(games),
        "hasLive": has_live,
    })


@app.route("/api/salaries")
def api_salaries():
    """Return team salary rankings and per-team breakdowns."""
    data = fetch_salaries()
    if isinstance(data, dict):
        return jsonify(data)
    return jsonify({"rankings": [], "teams": {}, "count": 0})


@app.route("/api/debug/boxscore")
def api_debug_boxscore():
    """Debug endpoint: return raw boxscore structure for the first live/final game."""
    try:
        resp = requests.get(config.SCORES_SCOREBOARD_URL, headers=_NBA_HEADERS, timeout=10)
        resp.raise_for_status()
        sb = resp.json().get("scoreboard", {})
        games_raw = sb.get("games", [])

        # Find first live or final game
        target = None
        for g in games_raw:
            if g.get("gameStatus", 1) >= 2:
                target = g
                break
        if not target:
            return jsonify({"error": "No live/final games", "games": len(games_raw)})

        gid = target["gameId"]
        box_resp = requests.get(
            config.SCORES_BOXSCORE_URL.format(game_id=gid),
            headers=_NBA_HEADERS, timeout=10,
        )
        box_resp.raise_for_status()
        raw = box_resp.json()

        # Return structure summary (not full data — too large)
        game_data = raw.get("game", {})
        summary = {
            "gameId": gid,
            "raw_top_keys": list(raw.keys()),
            "game_keys": list(game_data.keys()) if isinstance(game_data, dict) else str(type(game_data)),
        }

        for side in ("homeTeam", "awayTeam"):
            team = game_data.get(side)
            if team and isinstance(team, dict):
                players = team.get("players", [])
                summary[side] = {
                    "keys": list(team.keys()),
                    "teamTricode": team.get("teamTricode"),
                    "playersCount": len(players),
                    "hasStatistics": bool(team.get("statistics")),
                }
                if players:
                    p0 = players[0]
                    summary[side]["firstPlayer"] = {
                        "keys": list(p0.keys()),
                        "status": p0.get("status"),
                        "starter": p0.get("starter"),
                        "played": p0.get("played"),
                        "nameI": p0.get("nameI"),
                        "hasStatistics": bool(p0.get("statistics")),
                        "statsKeys": list(p0.get("statistics", {}).keys())[:15],
                    }
            else:
                summary[side] = None

        return jsonify(summary)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/status")
def api_status():
    """Health check — returns status of all data sources."""
    return jsonify({
        "ok": True,
        "sources": {
            "bluesky": {
                "cached": "posts" in bluesky_cache,
                "last_count": len(last_good_bluesky),
                "accounts_configured": len(config.BLUESKY_ACCOUNTS),
            },
            "headlines": {
                "cached": "headlines" in headlines_cache,
                "last_count": len(last_good_headlines),
            },
            "scores": {
                "cached": "scores" in scores_cache,
                "last_count": len(last_good_scores),
            },
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


# ═══════════════════════════════════════
# CACHE PRE-WARM
# ═══════════════════════════════════════

def _prewarm_caches():
    """Pre-populate caches in background so first API calls return instantly."""
    import time
    time.sleep(1)  # let server finish binding

    log.info("Pre-warming caches (background)...")

    try:
        fetch_headlines()
    except Exception as e:
        log.warning(f"Pre-warm headlines failed: {e}")

    try:
        fetch_bluesky_posts()
    except Exception as e:
        log.warning(f"Pre-warm Bluesky failed: {e}")

    try:
        fetch_scores()
    except Exception as e:
        log.warning(f"Pre-warm scores failed: {e}")

    try:
        fetch_salaries()
    except Exception as e:
        log.warning(f"Pre-warm salaries failed: {e}")

    try:
        fetch_ratings()
    except Exception as e:
        log.warning(f"Pre-warm ratings failed: {e}")

    try:
        fetch_depth()
    except Exception as e:
        log.warning(f"Pre-warm depth charts failed: {e}")

    try:
        fetch_team_ratings()
    except Exception as e:
        log.warning(f"Pre-warm team ratings failed: {e}")

    try:
        fetch_injuries()
    except Exception as e:
        log.warning(f"Pre-warm injuries failed: {e}")

    log.info("Cache pre-warm complete")


# ═══════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("HoopsHype Live — Starting server")
    log.info(f"Bluesky accounts: {len(config.BLUESKY_ACCOUNTS)}")
    log.info(f"Headlines source: Google Sheet {config.HEADLINES_SHEET_ID} (col {config.HEADLINES_COLUMN})")
    log.info(f"Scores source: {config.SCORES_SCOREBOARD_URL}")
    log.info(f"Server: http://localhost:{config.SERVER_PORT}")
    log.info("=" * 50)

    # Pre-warm caches in background thread (only in actual server process, not reloader)
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not config.DEBUG:
        threading.Thread(target=_prewarm_caches, daemon=True).start()

    app.run(
        host=config.SERVER_HOST,
        port=config.SERVER_PORT,
        debug=config.DEBUG,
        threaded=True,  # handle concurrent requests (Bluesky fetch blocks for 30s+)
    )
