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

# Fallback data (served when fetch fails)
last_good_bluesky = []
last_good_headlines = []
last_good_scores = []

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
    return {
        "fgPct": f"{stats.get('fieldGoalsPercentage', 0) * 100:.1f}",
        "threePct": f"{stats.get('threePointersPercentage', 0) * 100:.1f}",
        "ftPct": f"{stats.get('freeThrowsPercentage', 0) * 100:.1f}",
        "reb": stats.get("reboundsTotal", 0),
        "ast": stats.get("assists", 0),
        "stl": stats.get("steals", 0),
        "blk": stats.get("blocks", 0),
        "to": stats.get("turnovers", 0),
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
        _empty_stats = {"fgPct": "0.0", "threePct": "0.0", "ftPct": "0.0", "reb": 0, "ast": 0, "stl": 0, "blk": 0, "to": 0}
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

@app.route("/")
def serve_index():
    """Serve the broadcast overlay page."""
    return send_from_directory(PROJECT_ROOT, "index.html")


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
