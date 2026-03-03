"""
HoopsHype Live — Flask Server
Serves the broadcast page and API endpoints for live data.

Phase 1: Bluesky feed + HoopsHype headlines (via Google Sheets)
Phase 2: Live NBA scores via nba.com CDN
Phase 3: Google Sheets rankings (TODO)
"""

import csv
import io
import json
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
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
_player_salary_raw = {}   # "LeBron James" → 46700000 (integer)
_full_name_map = {}       # "L. James" → "LeBron James", etc.
_player_team_map = {}     # "LeBron James" → "Los Angeles Lakers" (from depth charts)
_player_position_map = {} # "LeBron James" → "PG" (starters only, from depth charts)
_depth_starters = {}      # "Boston" → [{"name": "...", "pos": "PG"}, ...]
_player_full_stats = {}   # "LeBron James" → {"PTS": 25.1, "REB": 7.2, ...} (from NBA leagueleaders)
_player_adv_stats = {}    # "LeBron James" → {"NET_RATING": 5.2, ...} (from GitHub JSON)
_salary_team_lookup = {}  # "LeBron James" → "Los Angeles Lakers" (from salary data, for depth ID)
_last_name_map = {}       # "Mathurin" → [("Bennedict Mathurin", "Bennedict"), ...]
_player_rating_season = {}  # "LeBron James" → 23.5 (Season Global Rating)
_player_rating_7d = {}      # "LeBron James" → 28.1 (Last 7 Days Global Rating)
_team_standings = {}        # "OKC" → {"wins": 38, "losses": 9, "confRank": 1, ...}
_schedule_cache = {}        # Cached full NBA schedule JSON for season series / rest days
_team_adv_stats = {}        # "OKC" → {"ortg": 118.5, "drtg": 105.2, "pace": 99.1, ...}


def resolve_player_name(raw_name):
    """Resolve a depth chart name to a full salary-sheet name.

    Handles: exact match, 'F. LastName', 'Benn. LastName', last-name-only.
    """
    # 1. Exact match
    if raw_name in _salary_team_lookup:
        return raw_name
    # 2. Standard abbreviation map ("N. Alexander-Walker" → "Nickeil ...")
    if raw_name in _full_name_map:
        return _full_name_map[raw_name]
    # 3. Truncated first name with dot ("Benn. Mathurin", "Jul. Champagnie")
    if ". " in raw_name:
        prefix, last = raw_name.split(". ", 1)
        last_clean = last.strip()
        candidates = _last_name_map.get(last_clean, [])
        if len(candidates) == 1:
            return candidates[0][0]  # unique last name, safe match
        # Multiple candidates: match by prefix
        for full_name, first_name in candidates:
            if first_name.lower().startswith(prefix.rstrip(".").lower()):
                return full_name
    # 4. Last name only fallback (unique last names)
    parts = raw_name.rsplit(" ", 1)
    if len(parts) == 2:
        last = parts[1]
        candidates = _last_name_map.get(last, [])
        if len(candidates) == 1:
            return candidates[0][0]
        # Try matching first letter
        first_initial = parts[0][0].upper() if parts[0] else ""
        for full_name, first_name in candidates:
            if first_name and first_name[0].upper() == first_initial:
                return full_name
    return raw_name

# HTTP request defaults (no shared Session — requests.Session is NOT thread-safe
# and we call from 20+ concurrent ThreadPoolExecutor workers)
_HTTP_HEADERS = {"User-Agent": "HoopsHypeLive/1.0"}

def _now_et():
    """Get current datetime in US Eastern Time (handles DST)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        # Fallback: UTC - 5 hours (EST approximation)
        return datetime.now(timezone.utc) - timedelta(hours=5)

# Team abbreviation → city name mapping
_TEAM_TO_CITY = {
    "ATL": "Atlanta", "BOS": "Boston", "BKN": "Brooklyn", "CHA": "Charlotte",
    "CHI": "Chicago", "CLE": "Cleveland", "DAL": "Dallas", "DEN": "Denver",
    "DET": "Detroit", "GSW": "Golden State", "HOU": "Houston", "IND": "Indiana",
    "LAC": "LA Clippers", "LAL": "LA Lakers", "MEM": "Memphis", "MIA": "Miami",
    "MIL": "Milwaukee", "MIN": "Minnesota", "NOP": "New Orleans", "NYK": "New York",
    "OKC": "Oklahoma City", "ORL": "Orlando", "PHI": "Philadelphia", "PHX": "Phoenix",
    "POR": "Portland", "SAC": "Sacramento", "SAS": "San Antonio", "TOR": "Toronto",
    "UTA": "Utah", "WAS": "Washington",
    # Full names → city names
    "Atlanta Hawks": "Atlanta", "Boston Celtics": "Boston", "Brooklyn Nets": "Brooklyn",
    "Charlotte Hornets": "Charlotte", "Chicago Bulls": "Chicago", "Cleveland Cavaliers": "Cleveland",
    "Dallas Mavericks": "Dallas", "Denver Nuggets": "Denver", "Detroit Pistons": "Detroit",
    "Golden State Warriors": "Golden State", "Houston Rockets": "Houston", "Indiana Pacers": "Indiana",
    "LA Clippers": "LA Clippers", "Los Angeles Clippers": "LA Clippers",
    "LA Lakers": "LA Lakers", "Los Angeles Lakers": "LA Lakers",
    "Memphis Grizzlies": "Memphis", "Miami Heat": "Miami", "Milwaukee Bucks": "Milwaukee",
    "Minnesota Timberwolves": "Minnesota", "New Orleans Pelicans": "New Orleans",
    "New York Knicks": "New York", "Oklahoma City Thunder": "Oklahoma City",
    "Orlando Magic": "Orlando", "Philadelphia 76ers": "Philadelphia",
    "Phoenix Suns": "Phoenix", "Portland Trail Blazers": "Portland",
    "Sacramento Kings": "Sacramento", "San Antonio Spurs": "San Antonio",
    "Toronto Raptors": "Toronto", "Utah Jazz": "Utah", "Washington Wizards": "Washington",
}

def team_city(raw):
    """Convert team abbreviation or full name to city name."""
    if not raw:
        return ""
    return _TEAM_TO_CITY.get(raw) or _TEAM_TO_CITY.get(raw.upper()) or raw


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

            post_data = {
                "author": author.get("displayName", handle),
                "handle": f"@{author.get('handle', handle)}",
                "avatar": _initials(author.get("displayName", handle)),
                "avatarUrl": author.get("avatar", ""),
                "text": text,
                "time": _time_ago(created),
                "timestamp": created,
            }

            # Extract embedded images
            embed = post.get("embed", {})
            embed_type = embed.get("$type", "")
            images = []
            if "images" in embed_type or "recordWithMedia" in embed_type:
                img_list = embed.get("images", [])
                if not img_list and "media" in embed:
                    img_list = embed["media"].get("images", [])
                for img in img_list[:2]:
                    thumb = img.get("thumb", "")
                    if thumb:
                        images.append(thumb)
            if images:
                post_data["images"] = images

            # Extract quote posts
            if "record" in embed_type:
                rec = embed.get("record", {})
                if "record" in rec:
                    rec = rec["record"]
                q_author = rec.get("author", {})
                q_text = rec.get("value", {}).get("text", "") or rec.get("text", "")
                if q_text:
                    post_data["quote"] = {
                        "author": q_author.get("displayName", ""),
                        "handle": q_author.get("handle", ""),
                        "text": q_text[:200],
                    }

            posts.append(post_data)

    except Exception as e:
        log.debug(f"Bluesky fetch failed for {handle}: {e}")

    return posts


def fetch_bluesky_posts():
    """Fetch recent posts from configured Bluesky accounts via public API (parallelized)."""
    global last_good_bluesky

    # Return cached if available
    if "posts" in bluesky_cache:
        return bluesky_cache["posts"]

    # Ensure hoopshypeofficial is always included
    accounts = list(config.BLUESKY_ACCOUNTS)
    if "hoopshypeofficial.bsky.social" not in accounts:
        accounts.append("hoopshypeofficial.bsky.social")

    log.info(f"Fetching Bluesky feeds for {len(accounts)} accounts...")

    all_posts = []
    success_count = 0
    fail_count = 0
    with ThreadPoolExecutor(max_workers=BLUESKY_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_one_feed, handle): handle
            for handle in accounts
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

    # Sort by timestamp (newest first) — keep all posts for full feed
    all_posts.sort(key=lambda p: p.get("timestamp", ""), reverse=True)

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

    # Parse CSV — column A (index 0) is timestamp, column B (index 1) is headline text
    col_index = ord(config.HEADLINES_COLUMN.upper()) - ord("A")
    reader = csv.reader(io.StringIO(resp.text))
    items = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=18)
    skipped_old = 0

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

        # Parse timestamp from column A for 18-hour filter
        time_display = ""
        if len(row) > 0 and row[0].strip():
            ts_str = row[0].strip()
            parsed_ts = None
            for fmt in ["%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y",
                        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
                        "%b %d, %Y %H:%M", "%b %d, %Y"]:
                try:
                    parsed_ts = datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
                    break
                except:
                    continue
            if parsed_ts:
                if parsed_ts < cutoff:
                    skipped_old += 1
                    continue
                diff_mins = (datetime.now(timezone.utc) - parsed_ts).total_seconds() / 60
                time_display = ""  # timestamps removed from ticker display

        items.append({
            "text": text,
            "time": time_display,
            "isNew": len(items) < config.HEADLINES_NEW_COUNT,
        })

        if len(items) >= config.HEADLINES_MAX_ITEMS:
            break

    if items:
        last_good_headlines = items
        headlines_cache["headlines"] = items
        new_count = sum(1 for h in items if h["isNew"])
        log.info(f"Cached {len(items)} headlines from Google Sheet ({new_count} NEW, {skipped_old} skipped as older than 18h)")
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
        team_abbr = team.get("teamTricode", "")
        side = {
            "teamId": team.get("teamId", 0),
            "abbr": team_abbr,
            "city": team.get("teamCity", "").upper(),
            "name": team.get("teamName", ""),
            "record": _format_record(team.get("wins", 0), team.get("losses", 0)),
            "score": team.get("score", 0),
            "logo": _team_logo_url(team_abbr),
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

    # Broadcasters
    broadcasters = []
    bc = sb_game.get("broadcasters", {})
    for key in ("nationalBroadcasters", "nationalTvBroadcasters"):
        for b in bc.get(key, []):
            name = b.get("broadcastDisplay", "")
            if name and name not in broadcasters:
                broadcasters.append(name)
    if not broadcasters:
        # Fallback: home/away local
        for key in ("homeTvBroadcasters", "awayTvBroadcasters"):
            for b in bc.get(key, []):
                name = b.get("broadcastDisplay", "")
                if name and name not in broadcasters:
                    broadcasters.append(name)

    # Officials / Referees
    officials = []
    for off in sb_game.get("officials", []):
        name = off.get("name", "") or off.get("nameI", "")
        if name:
            officials.append(name)
    # Also try boxscore officials
    if not officials and boxscore_data:
        for off in boxscore_data.get("officials", []):
            name = off.get("name", "") or off.get("nameI", "")
            if name:
                officials.append(name)

    return {
        "id": sb_game.get("gameId", ""),
        "status": _game_status_str(game_status),
        "period": _period_label(period, game_status),
        "periodNum": period,
        "clock": game_clock if game_status == 2 else ("" if game_status == 3 else status_text),
        "venue": arena,
        "broadcasters": broadcasters,
        "officials": officials,
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
    "Dante Exum": "au", "Alex Toohey": "au",
    "Johnny Furphy": "au", "Jock Landale": "au", "Joe Ingles": "au",
    "Rocco Zikarsky": "au", "Jakob Poeltl": "at", "Jeremy Sochan": "pl",
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
    "Al Horford": "do", "David Jones Garcia": "do", "Karl-Anthony Towns": "do",
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
    "Bobi Klintman": "se", "Pelle Larsson": "se",
    "Clint Capela": "ch", "Yanic Konan Niederhauser": "ch", "Kyshawn George": "ch",
    "Alperen Sengun": "tr",
    "Svi Mykhailiuk": "ua", "Max Shulga": "ua",
    "Amari Williams": "gb", "OG Anunoby": "gb",
    "Tosan Evbuomwan": "gb", "Jeremy Sochan": "gb",
    # Retired international players (for historical salary screens)
    "Marc Gasol": "es", "Pau Gasol": "es", "Jose Calderon": "es",
    "Juan Hernangomez": "es", "Willy Hernangomez": "es", "Ricky Rubio": "es",
    "Serge Ibaka": "es", "Alex Abrines": "es",
    "Dirk Nowitzki": "de", "Detlef Schrempf": "de", "Chris Kaman": "de",
    "Tony Parker": "fr", "Boris Diaw": "fr", "Evan Fournier": "fr",
    "Frank Ntilikina": "fr", "Ian Mahinmi": "fr", "Rodrigue Beaubois": "fr",
    "Manu Ginobili": "ar", "Luis Scola": "ar", "Carlos Delfino": "ar",
    "Facundo Campazzo": "ar", "Luca Vildoza": "ar",
    "Steve Nash": "ca", "Tristan Thompson": "ca", "Cory Joseph": "ca",
    "Nik Stauskas": "ca", "Trey Lyles": "ca", "Chris Boucher": "ca",
    "Yao Ming": "cn", "Yi Jianlian": "cn", "Zhou Qi": "cn",
    "Andrea Bargnani": "it", "Danilo Gallinari": "it", "Marco Belinelli": "it",
    "Goran Dragic": "si", "Beno Udrih": "si",
    "Peja Stojakovic": "rs", "Vlade Divac": "rs",
    "Hedo Turkoglu": "tr", "Mehmet Okur": "tr", "Ersan Ilyasova": "tr",
    "Enes Kanter": "tr", "Cedi Osman": "tr",
    "Hakeem Olajuwon": "ng", "Dikembe Mutombo": "cd",
    "Patrick Ewing": "jm", "Luol Deng": "gb", "Ben Simmons": "au",
    "Andrew Bogut": "au", "Patty Mills": "au", "Matthew Dellavedova": "au",
    "Aron Baynes": "au", "Thon Maker": "au",
    "Toni Kukoc": "hr", "Drazen Petrovic": "hr",
    "Arvydas Sabonis": "lt", "Zydrunas Ilgauskas": "lt",
    "Andrei Kirilenko": "ru", "Timofey Mozgov": "ru", "Alexey Shved": "ru",
    "Gorgui Dieng": "sn",
    "Nene": "br", "Leandro Barbosa": "br", "Anderson Varejao": "br",
    "Tiago Splitter": "br",
    "Samuel Dalembert": "ht",
    "Gheorghe Muresan": "ro",
    "Manute Bol": "ss",
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
        _player_salary_raw[player] = salary
        _salary_team_lookup[player] = team
        # Build abbreviated → full name map ("N. Alexander-Walker" → "Nickeil Alexander-Walker")
        parts = player.split(" ", 1)
        if len(parts) == 2 and len(parts[0]) > 0:
            abbr = parts[0][0] + ". " + parts[1]
            _full_name_map[abbr] = player
            # Build last name index for fuzzy matching
            last = parts[1]
            if last not in _last_name_map:
                _last_name_map[last] = []
            _last_name_map[last].append((player, parts[0]))

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
    (3,  "Global Rating — Last 365 Days", 24),
    (14, "Global Rating — Season", 24),
    (25, "Global Rating — Rookies", 24),
    (36, "Global Rating — International", 24),
    (47, "Global Rating — Sixth Man of the Year", 24),
    (58, "Global Rating — Last 7 Days", 24),
    (80, "Global Rating — Last 30 Days", 24),
    (91, "Global Rating — Most In Form", 24),
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
            team = _player_team_map.get(name, "")
            if team.lower().startswith("unknown"):
                team = ""

            if is_form:
                # Cols: PLAYER, RAT (current), 2024-25 (old), DIFF
                old_rat = row[start_col + 2].strip() if start_col + 2 < len(row) else ""
                diff = row[start_col + 3].strip() if start_col + 3 < len(row) else ""
                players.append({
                    "rank": len(players) + 1,
                    "name": name,
                    "team": team,
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
                    "team": team,
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

        # Build player rating lookup dicts from specific blocks
        _player_rating_season.clear()
        _player_rating_7d.clear()
        for scr in screens:
            title = scr.get("title", "")
            for p in scr.get("players", []):
                name = p.get("name", "")
                try:
                    rat = float(p.get("rating", 0))
                except (ValueError, TypeError):
                    rat = 0
                if not name or not rat:
                    continue
                if "Season" in title and name not in _player_rating_season:
                    _player_rating_season[name] = rat
                elif "Last 7 Days" in title and name not in _player_rating_7d:
                    _player_rating_7d[name] = rat
        log.info(f"  Rating lookups: season={len(_player_rating_season)}, 7d={len(_player_rating_7d)}")

    return screens


@app.route("/api/ratings")
def api_ratings():
    """Return Global Rating ranking screens."""
    data = fetch_ratings()
    return jsonify({"screens": data, "count": len(data)})


# ═══════════════════════════════════════
# DRAFT CLASS RATINGS (Bio sheet + Ratings)
# ═══════════════════════════════════════

_BIO_SHEET_ID = "1ZrDfzqiC31Hu3YCtxT4aZbZF4QVCVyGe6wBytR2LF30"
_BIO_GID = "1488063724"

draft_class_cache = TTLCache(maxsize=1, ttl=1800)  # 30 min
last_good_draft_classes = []


def fetch_draft_classes():
    """Build draft class rating screens from bio + ratings data."""
    global last_good_draft_classes

    if "dc" in draft_class_cache:
        return draft_class_cache["dc"]

    # 1) Fetch bio sheet → player → draft year map
    bio_url = (
        f"https://docs.google.com/spreadsheets/d/{_BIO_SHEET_ID}"
        f"/export?format=csv&gid={_BIO_GID}"
    )
    log.info("Fetching bio data for draft classes...")

    try:
        resp = requests.get(bio_url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Bio sheet fetch failed: {e}")
        return last_good_draft_classes

    bio_reader = list(csv.reader(io.StringIO(resp.text)))
    # col 0=PLAYER, col 9=DRAFT, col 11=TEAM, col 12=REAL TEAM
    player_draft = {}
    player_bio_team = {}
    # Build case-insensitive lookup for bio names
    bio_name_map = {}  # lowercase → original name
    for row in bio_reader[1:]:
        if len(row) < 10:
            continue
        name = row[0].strip()
        draft_yr = row[9].strip()
        team_abbr = row[11].strip() if len(row) > 11 else ""
        real_team = row[12].strip() if len(row) > 12 else ""
        if name and draft_yr.isdigit():
            player_draft[name] = int(draft_yr)
            bio_name_map[name.lower()] = name
        if name and (real_team or team_abbr):
            player_bio_team[name] = real_team or team_abbr

    # Manual overrides for known mismatches / missing entries
    # Case variant: ratings has "Tristan da Silva", bio has "Tristan Da Silva"
    if "Tristan Da Silva" in player_draft and "Tristan da Silva" not in player_draft:
        player_draft["Tristan da Silva"] = player_draft["Tristan Da Silva"]
        player_bio_team["Tristan da Silva"] = player_bio_team.get("Tristan Da Silva", "")
    # Tyrese Proctor: 2025 draft class (bio sheet has wrong year)
    player_draft["Tyrese Proctor"] = 2025
    # PJ Tucker: hasn't played this season, exclude from draft classes
    if "PJ Tucker" in player_draft:
        del player_draft["PJ Tucker"]
    if "P.J. Tucker" in player_draft:
        del player_draft["P.J. Tucker"]

    log.info(f"  Bio: {len(player_draft)} players with draft year, {len(player_bio_team)} with team")

    # 2) Fetch ratings sheet — read ALL players from Season block (start_col=14)
    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{_RATINGS_SHEET_ID}"
        f"/export?format=csv&gid={_RATINGS_GID}"
    )
    try:
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Ratings fetch for draft classes failed: {e}")
        return last_good_draft_classes

    reader = list(csv.reader(io.StringIO(resp.text)))

    # Read rated players from Season block ONLY (col 14) for consistency
    # with Team Ratings and main Global Rating screens
    _SEASON_COL = 14  # Season block: PLAYER(+0), RAT(+1), G(+2), PTS(+3), REB(+4), AST(+5)
    rated_map = {}  # name → player dict

    for row_idx in range(1, len(reader)):
        row = reader[row_idx]
        if _SEASON_COL >= len(row):
            continue
        name = row[_SEASON_COL].strip() if _SEASON_COL < len(row) else ""
        rat_str = row[_SEASON_COL + 1].strip() if _SEASON_COL + 1 < len(row) else ""
        if not name or not rat_str:
            continue
        try:
            rat = float(rat_str)
        except ValueError:
            continue

        games = row[_SEASON_COL + 2].strip() if _SEASON_COL + 2 < len(row) else ""
        pts = row[_SEASON_COL + 3].strip() if _SEASON_COL + 3 < len(row) else ""
        reb = row[_SEASON_COL + 4].strip() if _SEASON_COL + 4 < len(row) else ""
        ast = row[_SEASON_COL + 5].strip() if _SEASON_COL + 5 < len(row) else ""

        # Use bio sheet team (3-letter abbreviation), fallback to depth chart team
        team = player_bio_team.get(name, "") or _player_team_map.get(name, "")
        if team.lower().startswith("unknown"):
            team = ""
        team = team_city(team)  # Convert abbreviation to city name

        rated_map[name] = {
            "name": name,
            "rating": rat,
            "ratingDisplay": rat_str,
            "games": games,
            "pts": pts,
            "reb": reb,
            "ast": ast,
            "team": team,
            "country": _PLAYER_COUNTRY.get(name, ""),
        }

    rated_players = list(rated_map.values())
    log.info(f"  Ratings: {len(rated_players)} unique rated players from Season block")

    # 3) Group by draft class — use case-insensitive matching for bio lookup
    by_class = {}
    unmatched = 0
    for p in rated_players:
        yr = player_draft.get(p["name"])
        # Fallback: case-insensitive lookup
        if yr is None:
            bio_name = bio_name_map.get(p["name"].lower())
            if bio_name:
                yr = player_draft.get(bio_name)
        if yr is None:
            unmatched += 1
            continue
        if yr not in by_class:
            by_class[yr] = []
        by_class[yr].append(p)

    log.info(f"  Matched: {sum(len(v) for v in by_class.values())}, unmatched: {unmatched}")

    # 4) Build screens — every class gets its own screen
    screens = []

    for year in sorted(by_class.keys(), reverse=True):
        players = sorted(by_class[year], key=lambda p: p["rating"], reverse=True)
        players = players[:24]
        for i, p in enumerate(players):
            p["rank"] = i + 1
        top_n = players[:5]
        avg = sum(p["rating"] for p in top_n) / len(top_n)
        avg_label = f"Top {len(top_n)} avg" if len(top_n) > 1 else "Rating"

        screens.append({
            "title": f"Draft Class {year} — Global Rating",
            "subtitle": f"{len(players)} rated player{'s' if len(players)!=1 else ''} | {avg_label}: {avg:.2f}",
            "year": year,
            "players": players,
        })

    if screens:
        draft_class_cache["dc"] = screens
        last_good_draft_classes = screens
        log.info(f"Cached {len(screens)} draft class screens ({min(by_class.keys())}-{max(by_class.keys())})")

    return screens


@app.route("/api/draft_classes")
def api_draft_classes():
    """Return draft class rating screens."""
    data = fetch_draft_classes()
    return jsonify({"screens": data, "count": len(data)})


# ═══════════════════════════════════════
# TRANSACTIONS
# ═══════════════════════════════════════

_TRANSACTIONS_GID = "2081598055"

_TX_TEAM_NAMES = {
    "ATL": "Atlanta", "BOS": "Boston", "BKN": "Brooklyn",
    "CHA": "Charlotte", "CHI": "Chicago", "CLE": "Cleveland",
    "DAL": "Dallas", "DEN": "Denver", "DET": "Detroit",
    "GSW": "Golden State", "HOU": "Houston", "IND": "Indiana",
    "LAC": "LA Clippers", "LAL": "LA Lakers", "MEM": "Memphis",
    "MIA": "Miami", "MIL": "Milwaukee", "MIN": "Minnesota",
    "NOP": "New Orleans", "NYK": "New York", "OKC": "Oklahoma City",
    "ORL": "Orlando", "PHI": "Philadelphia", "PHX": "Phoenix",
    "POR": "Portland", "SAC": "Sacramento", "SAS": "San Antonio",
    "TOR": "Toronto", "UTA": "Utah", "WAS": "Washington",
}

transactions_cache = TTLCache(maxsize=1, ttl=900)  # 15 min
last_good_transactions = []


def fetch_transactions():
    """Fetch recent NBA transactions from Google Sheet."""
    global last_good_transactions

    if "tx" in transactions_cache:
        return transactions_cache["tx"]

    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{_INJURIES_SHEET_ID}"
        f"/export?format=csv&gid={_TRANSACTIONS_GID}"
    )
    log.info("Fetching transactions from Google Sheet...")

    try:
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Transactions fetch failed: {e}")
        return last_good_transactions

    reader = list(csv.reader(io.StringIO(resp.text)))
    if len(reader) < 2:
        return last_good_transactions

    # Cols: 1=logo_url, 2=DATE, 3=TEAM, 4=PLAYER, 5=NOTES, 6=SALARY
    # Date carries forward when blank
    transactions = []
    current_date = ""
    for row in reader[1:]:
        if len(row) < 5:
            continue
        player = row[4].strip() if len(row) > 4 else ""
        if not player:
            continue

        date = row[2].strip() if len(row) > 2 else ""
        if date:
            current_date = date
        team = row[3].strip() if len(row) > 3 else ""
        notes = row[5].strip() if len(row) > 5 else ""
        salary = row[6].strip() if len(row) > 6 else ""

        # Clean salary — remove #N/A
        if salary in ("#N/A", "N/A", "#REF!", ""):
            salary = ""

        # Map abbreviation to full team name
        team_full = _TX_TEAM_NAMES.get(team.upper(), team)

        country = _PLAYER_COUNTRY.get(player, "")

        transactions.append({
            "date": current_date,
            "team": team_full,
            "player": player,
            "notes": notes,
            "salary": salary,
            "country": country,
        })

    log.info(f"  Transactions: {len(transactions)} total")

    # Single screen with the 20 most recent
    recent = transactions[:20]
    screens = []
    if recent:
        screens.append({
            "title": "NBA Transactions",
            "subtitle": f"Most recent moves",
            "transactions": recent,
        })

    if screens:
        transactions_cache["tx"] = screens
        last_good_transactions = screens
        log.info(f"Cached {len(screens)} transaction screens")

    return screens


@app.route("/api/transactions")
def api_transactions():
    """Return transaction screens."""
    data = fetch_transactions()
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
    rated_players = {}  # name → player dict (both raw AND normalized keys)
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

        entry = {
            "rating": rat,
            "games": games,
            "pts": pts,
            "reb": reb,
            "ast": ast,
        }
        rated_players[name] = entry
        # Also store under normalized name (diacritics stripped) for cross-reference
        norm = _normalize_name(name)
        if norm != name:
            rated_players[norm] = entry

    # Build reverse lookup for fuzzy matching: last_name_lower → {first_initial → entry}
    _rated_by_last = {}
    for rname, rentry in rated_players.items():
        parts = rname.split()
        if len(parts) >= 2:
            last = parts[-1].lower()
            first_init = parts[0][0].upper()
            if last not in _rated_by_last:
                _rated_by_last[last] = {}
            _rated_by_last[last][first_init] = rentry

    def _lookup_rated(player_name):
        """Multi-strategy lookup against rated_players."""
        # 1. Exact
        rp = rated_players.get(player_name)
        if rp:
            return rp
        # 2. Normalized (diacritics)
        rp = rated_players.get(_normalize_name(player_name))
        if rp:
            return rp
        # 3. Case-insensitive
        name_lower = ' '.join(player_name.lower().split())
        for rname, rentry in rated_players.items():
            if ' '.join(rname.lower().split()) == name_lower:
                return rentry
        # 4. Last name + first initial (handles "S. Gilgeous-Alexander" vs "Shai Gilgeous-Alexander")
        parts = player_name.split()
        if len(parts) >= 2:
            last = parts[-1].lower()
            first_init = parts[0][0].upper()
            bucket = _rated_by_last.get(last, {})
            rp = bucket.get(first_init)
            if rp:
                return rp
        return None

    # Build team screens using depth chart rosters
    teams = {}
    unmatched_names = []
    for player_name, team_name in _player_team_map.items():
        if team_name not in teams:
            teams[team_name] = []
        rp = _lookup_rated(player_name)
        if not rp:
            unmatched_names.append(player_name)
        teams[team_name].append({
            "rank": 0,  # will be set after sort
            "name": player_name,
            "rating": f"{rp['rating']:.1f}" if rp else "—",
            "ratingNum": rp["rating"] if rp else 0,
            "games": rp["games"] if rp else "",
            "pts": rp["pts"] if rp else "",
            "reb": rp["reb"] if rp else "",
            "ast": rp["ast"] if rp else "",
            "country": _PLAYER_COUNTRY.get(player_name, ""),
        })

    if unmatched_names:
        log.warning(f"  Team ratings: {len(unmatched_names)} players unmatched in ratings sheet: {unmatched_names[:15]}")

    # Sort players within each team by rating desc, assign ranks
    team_list = []
    for team_name, players in teams.items():
        players.sort(key=lambda x: x["ratingNum"], reverse=True)
        for i, p in enumerate(players):
            p["rank"] = i + 1
        rated = [p for p in players if p["ratingNum"] > 0]
        top6 = rated[:6]
        avg_rat = sum(p["ratingNum"] for p in top6) / len(top6) if top6 else 0
        team_list.append({
            "name": team_name,
            "avgRating": round(avg_rat, 2),
            "avgDisplay": f"{avg_rat:.2f}",
            "ratedCount": len(rated),
            "players": players,
        })

    # Sort teams alphabetically, assign rank by rating
    team_list.sort(key=lambda t: t["avgRating"], reverse=True)
    for i, t in enumerate(team_list):
        t["rank"] = i + 1
    # Re-sort alphabetically for display
    team_list.sort(key=lambda t: t["name"])

    # One screen per team
    screens = []
    for t in team_list:
        screens.append({
            "title": f"{t['name']}",
            "subtitle": f"Team average: {t['avgDisplay']} (Top 6 players)",
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
# HISTORICAL SALARIES (Google Sheets)
# ═══════════════════════════════════════

_HIST_SHEET_ID = "1ZrDfzqiC31Hu3YCtxT4aZbZF4QVCVyGe6wBytR2LF30"
_HIST_GID = "1151460858"
_HIST_START_YEAR = 1991
_HIST_TOP_N = 24

hist_salaries_cache = TTLCache(maxsize=1, ttl=3600)  # 1 hour
last_good_hist_salaries = []


def _parse_salary(s):
    """Parse '$55,761,217' → 55761217"""
    s = s.strip().replace("$", "").replace(",", "")
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0


def fetch_hist_salaries():
    """Fetch historical top salaries by year (1991-present)."""
    global last_good_hist_salaries

    if "hs" in hist_salaries_cache:
        return hist_salaries_cache["hs"]

    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{_HIST_SHEET_ID}"
        f"/export?format=csv&gid={_HIST_GID}"
    )
    log.info("Fetching historical salaries from Google Sheet...")

    try:
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Historical salaries fetch failed: {e}")
        return last_good_hist_salaries

    reader = csv.reader(io.StringIO(resp.text))

    # Col 0=TEAM, 1=YEAR, 2=PLAYER, 3=SALARY
    by_year = {}
    for row_num, row in enumerate(reader):
        if row_num == 0:
            continue
        if len(row) < 4:
            continue
        team = team_city(row[0].strip())
        year_str = row[1].strip()
        player = row[2].strip()
        salary_str = row[3].strip()

        if not year_str or not player or not salary_str:
            continue

        try:
            year = int(year_str)
        except ValueError:
            continue

        if year < _HIST_START_YEAR:
            continue

        salary = _parse_salary(salary_str)
        if salary <= 0:
            continue

        if year not in by_year:
            by_year[year] = []

        by_year[year].append({
            "name": player,
            "team": team,
            "salary": salary,
            "salaryDisplay": salary_str,
            "country": _PLAYER_COUNTRY.get(player, ""),
        })

    # Sort each year by salary desc, take top N, assign ranks
    screens = []
    for year in sorted(by_year.keys()):
        players = by_year[year]
        players.sort(key=lambda p: p["salary"], reverse=True)
        top = players[:_HIST_TOP_N]
        for i, p in enumerate(top):
            p["rank"] = i + 1

        # Display year as season format: 1991 → "1990-91"
        season = f"{year - 1}-{str(year)[-2:]}"
        screens.append({
            "title": f"Highest-Paid Players — {season}",
            "year": year,
            "players": top,
        })

    if screens:
        hist_salaries_cache["hs"] = screens
        last_good_hist_salaries = screens
        log.info(f"Cached {len(screens)} historical salary screens ({min(by_year.keys())}-{max(by_year.keys())})")

    return screens


@app.route("/api/hist_salaries")
def api_hist_salaries():
    """Return historical salary screens."""
    data = fetch_hist_salaries()
    return jsonify({"screens": data, "count": len(data)})


# ═══════════════════════════════════════
# SEASON LEADERS (NBA API - leagueleaders)
# ═══════════════════════════════════════

_NBA_STATS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Origin": "https://www.nba.com",
}

# (api_key, display_label)
_AVG_CATS = [
    ("PTS", "Points Per Game"),
    ("REB", "Rebounds Per Game"),
    ("AST", "Assists Per Game"),
    ("STL", "Steals Per Game"),
    ("BLK", "Blocks Per Game"),
    ("FG3M", "Three-Pointers Per Game"),
    ("FGM", "Field Goals Per Game"),
    ("FTM", "Free Throws Per Game"),
    ("OREB", "Offensive Reb Per Game"),
    ("DREB", "Defensive Reb Per Game"),
    ("TOV", "Turnovers Per Game"),
    ("MIN", "Minutes Per Game"),
]

_PCT_CATS = [
    ("FG_PCT", "Field Goal %", "FGA", 200),
    ("FG3_PCT", "Three-Point %", "FG3A", 100),
    ("FT_PCT", "Free Throw %", "FTA", 100),
]

_STATS_TOP_N = 24
_STATS_MIN_GP = 15  # minimum GP for percentage leaders


def _normalize_name(name):
    """Strip diacritics: Dončić → Doncic, Jokić → Jokic."""
    import unicodedata
    return ''.join(c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn')

counting_stats_cache = TTLCache(maxsize=1, ttl=1800)  # 30 min
last_good_counting_stats = []


def _current_nba_season():
    """Return current NBA season string, e.g. '2025-26'."""
    from datetime import datetime
    now = datetime.now()
    y = now.year if now.month >= 10 else now.year - 1
    return f"{y}-{str(y + 1)[-2:]}"


def _fetch_nba_leaders(season, per_mode):
    """Fetch one dataset from NBA leagueleaders endpoint."""
    url = "https://stats.nba.com/stats/leagueleaders"
    params = {
        "Season": season,
        "SeasonType": "Regular Season",
        "PerMode": per_mode,
        "Scope": "S",
        "StatCategory": "PTS",
        "LeagueID": "00",
        "ActiveFlag": "",
    }
    resp = requests.get(url, params=params, headers=_NBA_STATS_HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    rs = data.get("resultSet", {})
    return rs.get("headers", []), rs.get("rowSet", [])


def _build_leader_screens(api_headers, rows, categories, season, mode_label, min_gp=0):
    """Build screens from a dataset for given stat categories."""
    col_map = {h: i for i, h in enumerate(api_headers)}
    player_i = col_map.get("PLAYER", 2)
    team_i = col_map.get("TEAM", 4)
    gp_i = col_map.get("GP", 5)
    is_avg = mode_label in ("Per Game", "Percentage")

    screens = []
    for stat_key, title in categories:
        stat_i = col_map.get(stat_key)
        if stat_i is None:
            continue

        # Filter by min GP if needed
        eligible = [r for r in rows if r[gp_i] >= min_gp] if min_gp > 0 else rows

        # Sort by this stat descending
        sorted_rows = sorted(eligible, key=lambda r: r[stat_i] or 0, reverse=True)
        top = sorted_rows[:_STATS_TOP_N]

        players = []
        for rank, row in enumerate(top, 1):
            name = _normalize_name(row[player_i])
            val = row[stat_i]
            if isinstance(val, float):
                if stat_key.endswith("_PCT"):
                    disp = f"{val * 100:.1f}%"
                else:
                    disp = f"{val:.1f}"
            elif isinstance(val, int):
                disp = f"{val:,}"
            else:
                disp = str(val)
            players.append({
                "rank": rank,
                "name": name,
                "team": team_city(row[team_i]),
                "gp": row[gp_i],
                "value": val,
                "valueDisplay": disp,
                "country": _PLAYER_COUNTRY.get(name, ""),
            })

        stat_label = stat_key.replace("_PCT", "%").replace("FG3M", "3PM").replace("FG3", "3P")
        screens.append({
            "title": f"{title} — {season}",
            "stat": stat_label,
            "mode": mode_label,
            "players": players,
        })

    return screens


def fetch_counting_stats():
    """Fetch league leaders from NBA stats API (totals + per game + pct)."""
    global last_good_counting_stats

    if "cs" in counting_stats_cache:
        return counting_stats_cache["cs"]

    season = _current_nba_season()
    log.info(f"Fetching season leaders from NBA API (season {season})...")

    screens = []
    try:
        # Per Game
        h_avg, rows_avg = _fetch_nba_leaders(season, "PerGame")
        screens.extend(_build_leader_screens(h_avg, rows_avg, _AVG_CATS, season, "Per Game"))
        log.info(f"  Per Game: {len(rows_avg)} players, {len(_AVG_CATS)} categories")

        # Build full stats cache for comparisons
        col_map_avg = {h: i for i, h in enumerate(h_avg)}
        player_i = col_map_avg.get("PLAYER", 2)
        team_i = col_map_avg.get("TEAM", 4)
        gp_i = col_map_avg.get("GP", 5)
        _player_full_stats.clear()
        stat_keys = ["PTS", "REB", "AST", "STL", "BLK", "FG3M", "TOV", "FG_PCT", "FG3_PCT", "FT_PCT"]
        for row in rows_avg:
            name = _normalize_name(row[player_i])
            d = {"GP": row[gp_i], "TEAM": row[team_i]}
            for sk in stat_keys:
                si = col_map_avg.get(sk)
                if si is not None:
                    d[sk] = row[si]
            _player_full_stats[name] = d
        log.info(f"  Full stats cache: {len(_player_full_stats)} players (from leagueleaders)")

        # Supplement with ALL players (catches LeBron, Giddey, etc. who don't qualify for leaders)
        try:
            all_url = "https://stats.nba.com/stats/leagueleaders"
            all_params = {
                "Season": season,
                "SeasonType": "Regular Season",
                "PerMode": "PerGame",
                "Scope": "",  # empty = ALL players, not just qualified
                "StatCategory": "PTS",
                "LeagueID": "00",
                "ActiveFlag": "",
            }
            all_resp = requests.get(all_url, params=all_params, headers=_NBA_STATS_HEADERS, timeout=60)
            all_resp.raise_for_status()
            all_data = all_resp.json()
            all_rs = all_data.get("resultSet", {})
            all_headers = all_rs.get("headers", [])
            all_rows = all_rs.get("rowSet", [])
            all_col = {h: i for i, h in enumerate(all_headers)}
            all_player_i = all_col.get("PLAYER", 2)
            all_team_i = all_col.get("TEAM", 4)
            all_gp_i = all_col.get("GP", 5)
            supplemented = 0
            for row in all_rows:
                name = _normalize_name(row[all_player_i])
                if name not in _player_full_stats:
                    d = {"GP": row[all_gp_i], "TEAM": row[all_team_i]}
                    for sk in stat_keys:
                        si = all_col.get(sk)
                        if si is not None:
                            d[sk] = row[si]
                    _player_full_stats[name] = d
                    supplemented += 1
            log.info(f"  Supplemented {supplemented} players from leagueleaders(Scope=all) (total: {len(_player_full_stats)})")
        except Exception as e:
            log.warning(f"  Supplementary stats fetch failed: {e}")

        # Diagnostic: check LeBron name variants
        lebron_keys = [k for k in _player_full_stats if 'james' in k.lower() or 'lebron' in k.lower()]
        log.info(f"  LeBron variants in stats: {lebron_keys}")
        for lk in lebron_keys:
            d = _player_full_stats[lk]
            log.info(f"    {lk}: STL={d.get('STL')}, BLK={d.get('BLK')}, FG_PCT={d.get('FG_PCT')}, keys={list(d.keys())}")

        for stat_key, title, attempts_key, min_attempts in _PCT_CATS:
            stat_i = col_map_avg.get(stat_key)
            att_i = col_map_avg.get(attempts_key)
            if stat_i is None:
                continue

            # Filter: min GP AND min attempts (from Totals data for attempt counts)
            # PerGame data has per-game attempts, so we need total attempts = per_game * GP
            eligible = []
            for r in rows_avg:
                if r[gp_i] < _STATS_MIN_GP:
                    continue
                if att_i is not None:
                    att_per_game = r[att_i] or 0
                    total_att = att_per_game * r[gp_i]
                    if total_att < min_attempts:
                        continue
                eligible.append(r)

            sorted_rows = sorted(eligible, key=lambda r: r[stat_i] or 0, reverse=True)
            top = sorted_rows[:_STATS_TOP_N]

            players = []
            for rank, row in enumerate(top, 1):
                name = _normalize_name(row[player_i])
                val = row[stat_i]
                disp = f"{val * 100:.1f}%" if isinstance(val, float) else str(val)
                players.append({
                    "rank": rank,
                    "name": name,
                    "team": team_city(row[team_i]),
                    "gp": row[gp_i],
                    "value": val,
                    "valueDisplay": disp,
                    "country": _PLAYER_COUNTRY.get(name, ""),
                })

            stat_label = stat_key.replace("_PCT", "%").replace("FG3", "3P")
            screens.append({
                "title": f"{title} — {season}",
                "stat": stat_label,
                "mode": "Percentage",
                "players": players,
            })

        log.info(f"  Percentages: {len(_PCT_CATS)} categories (min {_STATS_MIN_GP} GP + min attempts)")

    except Exception as e:
        log.warning(f"Season leaders fetch failed: {e}")
        return last_good_counting_stats

    if screens:
        counting_stats_cache["cs"] = screens
        last_good_counting_stats = screens
        log.info(f"Cached {len(screens)} season leader screens")

    return screens


@app.route("/api/counting_stats")
def api_counting_stats():
    """Return season leader screens."""
    data = fetch_counting_stats()
    return jsonify({"screens": data, "count": len(data)})


# ═══════════════════════════════════════
# VALUE RANKINGS (Rating / Salary)
# ═══════════════════════════════════════

value_cache = TTLCache(maxsize=1, ttl=1800)  # 30 min
last_good_value = []


def fetch_value_rankings():
    """Compute best/worst value players by rating per $1M salary."""
    global last_good_value

    if "val" in value_cache:
        return value_cache["val"]

    log.info("Building value rankings...")

    # Read Season block (col 14) from ratings sheet for rating + GP
    csv_url = (
        f"https://docs.google.com/spreadsheets/d/{_RATINGS_SHEET_ID}"
        f"/export?format=csv&gid={_RATINGS_GID}"
    )
    try:
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
    except Exception as e:
        log.warning(f"Value: ratings fetch failed: {e}")
        return last_good_value

    reader = list(csv.reader(io.StringIO(resp.text)))
    if len(reader) < 2:
        return last_good_value

    # Collect all rated players from Season block (start_col=14)
    start_col = 14
    combined = []
    for row_idx in range(1, len(reader)):
        row = reader[row_idx]
        if start_col >= len(row):
            continue
        name = row[start_col].strip() if start_col < len(row) else ""
        if not name:
            continue
        try:
            rating = float(row[start_col + 1].strip())
        except:
            continue
        try:
            gp = int(row[start_col + 2].strip())
        except:
            gp = 0
        try:
            pts = row[start_col + 3].strip()
        except:
            pts = ""

        salary = _player_salary_raw.get(name, 0)
        if not salary or salary < 500_000:
            continue
        if gp < 10:
            continue

        salary_m = salary / 1_000_000
        value = round(rating / salary_m, 2)
        sal_compact = f"${salary_m:.1f}M" if salary_m >= 1 else f"${salary // 1000}K"
        team = team_city(_player_team_map.get(name, "") or _salary_team_lookup.get(name, ""))
        country = _PLAYER_COUNTRY.get(name, "")

        combined.append({
            "name": name, "team": team, "rating": rating,
            "ratingDisplay": f"{rating:.1f}",
            "salary": salary, "salaryCompact": sal_compact,
            "value": value, "games": gp, "pts": pts,
            "country": country,
        })

    if not combined:
        log.warning("Value: no players with salary + rating + GP found")
        return last_good_value

    # Screen 1: Best Value — highest rating/$1M (all players min $500K)
    best = [dict(p) for p in sorted(combined, key=lambda p: p["value"], reverse=True)[:24]]
    for i, p in enumerate(best):
        p["rank"] = i + 1

    # Screen 2: Worst Value — lowest rating/$1M among $10M+ earners
    big_earners = [p for p in combined if p["salary"] >= 10_000_000]
    worst = [dict(p) for p in sorted(big_earners, key=lambda p: p["value"])[:24]]
    for i, p in enumerate(worst):
        p["rank"] = i + 1

    # Screen 3: Lowest Rated $10M+ Players
    overpaid = [dict(p) for p in sorted(big_earners, key=lambda p: p["rating"])[:24]]
    for i, p in enumerate(overpaid):
        p["rank"] = i + 1

    # Screen 4: Best Bargains — highest-rated under $10M
    bargains_pool = [p for p in combined if p["salary"] < 10_000_000]
    bargains = [dict(p) for p in sorted(bargains_pool, key=lambda p: p["rating"], reverse=True)[:20]]
    for i, p in enumerate(bargains):
        p["rank"] = i + 1

    screens = [
        {"title": "Best Value — Rating per $1M", "subtitle": "Min $500K salary, 10+ GP", "data": best, "isValue": True, "valueType": "best"},
        {"title": "Worst Value — Rating per $1M", "subtitle": "$10M+ earners only", "data": worst, "isValue": True, "valueType": "worst"},
        {"title": "Lowest Rated $10M+ Players", "subtitle": "Sorted by raw rating", "data": overpaid, "isValue": True, "valueType": "overpaid"},
        {"title": "Best Bargains — Under $10M", "subtitle": "Highest-rated affordable players", "data": bargains, "isValue": True, "valueType": "bargain"},
    ]

    value_cache["val"] = screens
    last_good_value = screens
    log.info(f"Cached {len(screens)} value screens ({len(combined)} eligible players)")
    return screens


@app.route("/api/value")
def api_value():
    """Return value ranking screens."""
    data = fetch_value_rankings()
    return jsonify({"screens": data, "count": len(data)})


# ═══════════════════════════════════════
# PLAYER COMPARISONS (Matchups)
# ═══════════════════════════════════════

comparisons_cache = TTLCache(maxsize=1, ttl=300)  # 5 min
last_good_comparisons = {"screens": []}

_ALL_STARS = {
    # 2025 All-Stars  —  name → position
    "Giannis Antetokounmpo": "PF", "Jayson Tatum": "SF", "Karl-Anthony Towns": "C",
    "Donovan Mitchell": "SG", "Jalen Brunson": "PG", "LaMelo Ball": "PG",
    "Cade Cunningham": "PG", "Jaylen Brown": "SG", "Tyler Herro": "SG",
    "Evan Mobley": "PF", "Scottie Barnes": "PF", "Damian Lillard": "PG",
    "Nikola Jokic": "C", "Shai Gilgeous-Alexander": "PG", "LeBron James": "SF",
    "Kevin Durant": "SF", "Stephen Curry": "PG", "Victor Wembanyama": "C",
    "Anthony Edwards": "SG", "James Harden": "PG", "De'Aaron Fox": "PG",
    "Alperen Sengun": "C", "Domantas Sabonis": "C", "Norman Powell": "SG",
    # 2026 All-Stars (add/adjust as selected)
    "Anthony Davis": "PF", "Luka Doncic": "PG", "Trae Young": "PG",
    "Tyrese Haliburton": "PG", "Paolo Banchero": "PF", "Devin Booker": "SG",
    "Bam Adebayo": "C", "Kyrie Irving": "PG", "Jaren Jackson Jr.": "PF",
    "Franz Wagner": "SF", "Jalen Williams": "SG",
}

_POS_LABELS = {"PG": "Point Guard", "SG": "Shooting Guard", "SF": "Small Forward",
               "PF": "Power Forward", "C": "Center"}
_POS_COMPAT = {"PG": ["PG", "SG"], "SG": ["SG", "PG", "SF"], "SF": ["SF", "SG", "PF"],
               "PF": ["PF", "SF", "C"], "C": ["C", "PF"]}


def _find_stats(name):
    """Find full stats for a player with fuzzy matching."""
    if name in _player_full_stats:
        return _player_full_stats[name]
    # Normalize and try
    norm = _normalize_name(name)
    if norm in _player_full_stats:
        return _player_full_stats[norm]
    # Try _full_name_map reverse: "LeBron James" → check if any abbrev maps to this name
    for abbr, full in _full_name_map.items():
        if full == name and abbr in _player_full_stats:
            return _player_full_stats[abbr]
    # Case-insensitive + whitespace-normalized scan
    name_lower = ' '.join(name.lower().split())
    for k, v in _player_full_stats.items():
        if ' '.join(k.lower().split()) == name_lower:
            return v
    # Last name + first initial
    parts = name.split()
    if len(parts) >= 2:
        last = parts[-1].lower()
        first_init = parts[0][0].upper()
        for k, v in _player_full_stats.items():
            kp = k.split()
            if len(kp) >= 2 and kp[-1].lower() == last and kp[0][0].upper() == first_init:
                return v
    # Fallback: build minimal stats from team ratings data (PTS/REB/AST/GP)
    for scr in (last_good_team_ratings or []):
        for p in scr.get("players", []):
            if p["name"] == name and p.get("ratingNum", 0) > 0:
                d = {"GP": 0}
                try: d["GP"] = int(p.get("games", 0))
                except: pass
                try: d["PTS"] = float(p.get("pts", 0))
                except: pass
                try: d["REB"] = float(p.get("reb", 0))
                except: pass
                try: d["AST"] = float(p.get("ast", 0))
                except: pass
                return d
    return {}


def _find_adv(name):
    """Find advanced stats for a player with fuzzy matching."""
    if name in _player_adv_stats:
        return _player_adv_stats[name]
    norm = _normalize_name(name)
    if norm in _player_adv_stats:
        return _player_adv_stats[norm]
    # Try _full_name_map reverse
    for abbr, full in _full_name_map.items():
        if full == name and abbr in _player_adv_stats:
            return _player_adv_stats[abbr]
    # Case-insensitive scan
    name_lower = ' '.join(name.lower().split())
    for k, v in _player_adv_stats.items():
        if ' '.join(k.lower().split()) == name_lower:
            return v
    parts = name.split()
    if len(parts) >= 2:
        last = parts[-1].lower()
        first_init = parts[0][0].upper()
        for k, v in _player_adv_stats.items():
            kp = k.split()
            if len(kp) >= 2 and kp[-1].lower() == last and kp[0][0].upper() == first_init:
                return v
    return {}


def _get_rating(name):
    """Get Season Global Rating for a player from team ratings cache."""
    screens = last_good_team_ratings or []
    # 1. Exact match
    for scr in screens:
        for p in scr.get("players", []):
            if p["name"] == name and p["ratingNum"]:
                return p["ratingNum"]
    # 2. Normalized match (diacritics stripped)
    norm = _normalize_name(name)
    for scr in screens:
        for p in scr.get("players", []):
            if _normalize_name(p["name"]) == norm and p["ratingNum"]:
                return p["ratingNum"]
    # 3. Case-insensitive + whitespace-normalized
    name_lower = ' '.join(name.lower().split())
    for scr in screens:
        for p in scr.get("players", []):
            if ' '.join(p["name"].lower().split()) == name_lower and p["ratingNum"]:
                return p["ratingNum"]
    # 4. Last name + first initial
    parts = name.split()
    if len(parts) >= 2:
        last = parts[-1].lower()
        first_init = parts[0][0].upper()
        for scr in screens:
            for p in scr.get("players", []):
                kp = p["name"].split()
                if len(kp) >= 2 and kp[-1].lower() == last and kp[0][0].upper() == first_init and p["ratingNum"]:
                    return p["ratingNum"]
    return 0


def _s(label, va, vb, fmt="1", invert=False):
    """Build one stat row for a comparison card."""
    va = float(va or 0)
    vb = float(vb or 0)
    if isinstance(va, str):
        try: va = float(va)
        except: va = 0.0
    if isinstance(vb, str):
        try: vb = float(vb)
        except: vb = 0.0
    if invert:
        winner = "A" if va < vb else ("B" if vb < va else "tie")
    else:
        winner = "A" if va > vb else ("B" if vb > va else "tie")
    return {"label": label, "a": round(float(va), 1), "b": round(float(vb), 1), "fmt": fmt, "winner": winner}


def _build_comparison(name_a, name_b):
    """Build comprehensive comparison between two players."""
    stats_a = _find_stats(name_a)
    stats_b = _find_stats(name_b)
    adv_a = _find_adv(name_a)
    adv_b = _find_adv(name_b)
    rat_a = _get_rating(name_a)
    rat_b = _get_rating(name_b)
    team_a = team_city(_player_team_map.get(name_a, ""))
    team_b = team_city(_player_team_map.get(name_b, ""))

    # Diagnostic: log missing data
    for label, name, stats, adv, rat in [("A", name_a, stats_a, adv_a, rat_a), ("B", name_b, stats_b, adv_b, rat_b)]:
        missing_counting = [k for k in ["PTS", "STL", "BLK", "FG_PCT", "FG3_PCT", "FT_PCT"] if not stats.get(k)]
        missing_adv = [k for k in ["PLUS_MINUS", "NET_RATING", "D_FG_PCT", "CLUTCH_GP", "HUSTLE_GP"] if not adv.get(k)]
        if missing_counting or missing_adv or not rat:
            log.warning(f"  Comparison {label} ({name}): missing_counting={missing_counting}, missing_adv={missing_adv}, rating={rat}")
            log.warning(f"    stats_keys={list(stats.keys())[:8]}, adv_keys={list(adv.keys())}, adv_size={len(adv)}")

    sections = []

    # COUNTING STATS
    counting = []
    counting.append(_s("Points", stats_a.get("PTS"), stats_b.get("PTS")))
    counting.append(_s("Rebounds", stats_a.get("REB"), stats_b.get("REB")))
    counting.append(_s("Assists", stats_a.get("AST"), stats_b.get("AST")))
    counting.append(_s("Steals", stats_a.get("STL"), stats_b.get("STL")))
    counting.append(_s("Blocks", stats_a.get("BLK"), stats_b.get("BLK")))
    fg_a = (stats_a.get("FG_PCT") or 0) * 100 if stats_a.get("FG_PCT") else 0
    fg_b = (stats_b.get("FG_PCT") or 0) * 100 if stats_b.get("FG_PCT") else 0
    counting.append(_s("FG%", fg_a, fg_b, "1"))
    fg3_a = (stats_a.get("FG3_PCT") or 0) * 100 if stats_a.get("FG3_PCT") else 0
    fg3_b = (stats_b.get("FG3_PCT") or 0) * 100 if stats_b.get("FG3_PCT") else 0
    counting.append(_s("3P%", fg3_a, fg3_b, "1"))
    ft_a = (stats_a.get("FT_PCT") or 0) * 100 if stats_a.get("FT_PCT") else 0
    ft_b = (stats_b.get("FT_PCT") or 0) * 100 if stats_b.get("FT_PCT") else 0
    counting.append(_s("FT%", ft_a, ft_b, "1"))
    counting.append(_s("Turnovers", stats_a.get("TOV"), stats_b.get("TOV"), "1", invert=True))
    sections.append({"label": "COUNTING STATS", "stats": counting})

    # ADVANCED
    advanced = []
    advanced.append(_s("Rating", rat_a, rat_b, "1"))
    advanced.append(_s("+/-", adv_a.get("PLUS_MINUS"), adv_b.get("PLUS_MINUS"), "s1"))
    advanced.append(_s("Net Rating", adv_a.get("NET_RATING"), adv_b.get("NET_RATING"), "s1"))
    sections.append({"label": "ADVANCED", "stats": advanced})

    # DEFENSE
    defense = []
    dfg_a = adv_a.get("D_FG_PCT") or 0
    dfg_b = adv_b.get("D_FG_PCT") or 0
    xfg_a = adv_a.get("NORMAL_FG_PCT") or 0
    xfg_b = adv_b.get("NORMAL_FG_PCT") or 0
    defense.append(_s("Def FG%", dfg_a * 100 if dfg_a else 0, dfg_b * 100 if dfg_b else 0, "1", invert=True))
    defense.append({"label": "Exp FG%", "a": round(float(xfg_a * 100 if xfg_a else 0), 1),
                     "b": round(float(xfg_b * 100 if xfg_b else 0), 1), "fmt": "1", "winner": "tie"})
    diff_a = round(float(((dfg_a or 0) - (xfg_a or 0)) * 100), 1)
    diff_b = round(float(((dfg_b or 0) - (xfg_b or 0)) * 100), 1)
    defense.append({"label": "FG +/-", "a": diff_a, "b": diff_b, "fmt": "s1",
                     "winner": "A" if diff_a < diff_b else ("B" if diff_b < diff_a else "tie")})
    sections.append({"label": "DEFENSE", "stats": defense})

    # CLUTCH
    clutch = []
    cgp_a = adv_a.get("CLUTCH_GP", 1) or 1
    cgp_b = adv_b.get("CLUTCH_GP", 1) or 1
    for lbl, key in [("Points", "CLUTCH_PTS"), ("Rebounds", "CLUTCH_REB"),
                      ("Assists", "CLUTCH_AST"), ("Steals", "CLUTCH_STL"), ("Blocks", "CLUTCH_BLK")]:
        va = round(float((adv_a.get(key, 0) or 0) / cgp_a), 1)
        vb = round(float((adv_b.get(key, 0) or 0) / cgp_b), 1)
        clutch.append(_s(lbl, va, vb))
    sections.append({"label": "CLUTCH", "stats": clutch})

    # HUSTLE
    hustle = []
    hgp_a = adv_a.get("HUSTLE_GP", 1) or 1
    hgp_b = adv_b.get("HUSTLE_GP", 1) or 1
    for lbl, key in [("Contested", "CONTESTED_SHOTS"), ("Deflections", "DEFLECTIONS"),
                      ("Charges", "CHARGES_DRAWN")]:
        va = round(float((adv_a.get(key, 0) or 0) / hgp_a), 1)
        vb = round(float((adv_b.get(key, 0) or 0) / hgp_b), 1)
        fmt = "1"  # All hustle stats: 1 decimal
        hustle.append(_s(lbl, va, vb, fmt))
    sections.append({"label": "HUSTLE", "stats": hustle})

    # Score each section
    total_a = total_b = 0
    for sec in sections:
        sa = sb = 0
        for st in sec["stats"]:
            if st["winner"] == "A": sa += 1
            elif st["winner"] == "B": sb += 1
        sec["scoreA"] = sa
        sec["scoreB"] = sb
        total_a += sa
        total_b += sb

    gp_a = stats_a.get("GP", 0) or 0
    gp_b = stats_b.get("GP", 0) or 0
    sal_a = _player_salary_map.get(name_a, "—")
    sal_b = _player_salary_map.get(name_b, "—")

    return {
        "nameA": name_a, "nameB": name_b,
        "teamA": team_a, "teamB": team_b,
        "posA": _ALL_STARS.get(name_a) or _player_position_map.get(name_a, ""),
        "posB": _ALL_STARS.get(name_b) or _player_position_map.get(name_b, ""),
        "countryA": _PLAYER_COUNTRY.get(name_a, ""),
        "countryB": _PLAYER_COUNTRY.get(name_b, ""),
        "gpA": gp_a, "gpB": gp_b,
        "salaryA": sal_a, "salaryB": sal_b,
        "scoreA": total_a, "scoreB": total_b,
        "sections": sections,
    }


def fetch_advanced_stats():
    """Fetch advanced/defense/clutch/hustle stats from GitHub JSON."""
    global _player_adv_stats

    url = "https://raw.githubusercontent.com/jsierrahoopshype/nba-player-data/main/nba-2025-26-data.json"
    try:
        resp = requests.get(url, headers=_HTTP_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Advanced stats fetch failed: {e}")
        return

    _player_adv_stats.clear()
    datasets = [
        ("advanced", {"NET_RATING": "NET_RATING"}),
        ("defense", {"D_FG_PCT": "D_FG_PCT", "NORMAL_FG_PCT": "NORMAL_FG_PCT", "PCT_PLUSMINUS": "PCT_PLUSMINUS"}),
    ]
    # Advanced + Defense
    for ds_key, fields in datasets:
        ds = data.get(ds_key, [])
        for row in ds:
            name = _normalize_name(row.get("PLAYER_NAME") or row.get("PLAYER", ""))
            if not name:
                continue
            if name not in _player_adv_stats:
                _player_adv_stats[name] = {}
            for src_key, dst_key in fields.items():
                val = row.get(src_key)
                if val is not None:
                    _player_adv_stats[name][dst_key] = val

    # Clutch
    for row in data.get("clutch", []):
        name = _normalize_name(row.get("PLAYER_NAME") or row.get("PLAYER", ""))
        if not name:
            continue
        if name not in _player_adv_stats:
            _player_adv_stats[name] = {}
        d = _player_adv_stats[name]
        d["CLUTCH_GP"] = row.get("GP") or row.get("G")
        # PLUS_MINUS lives in clutch data, not advanced
        pm = row.get("PLUS_MINUS")
        if pm is not None:
            d["PLUS_MINUS"] = pm
        for k in ["PTS", "REB", "AST", "STL", "BLK"]:
            d[f"CLUTCH_{k}"] = row.get(k)

    # Hustle
    for row in data.get("hustle", []):
        name = _normalize_name(row.get("PLAYER_NAME") or row.get("PLAYER", ""))
        if not name:
            continue
        if name not in _player_adv_stats:
            _player_adv_stats[name] = {}
        d = _player_adv_stats[name]
        d["HUSTLE_GP"] = row.get("GP") or row.get("G")
        d["CONTESTED_SHOTS"] = row.get("CONTESTED_SHOTS")
        d["DEFLECTIONS"] = row.get("DEFLECTIONS")
        d["CHARGES_DRAWN"] = row.get("CHARGES_DRAWN")

    log.info(f"Advanced stats: {len(_player_adv_stats)} players loaded")
    # Diagnostic: show sample player's full adv data
    for check_name in ["James Harden", "Anthony Edwards", "LeBron James"]:
        d = _player_adv_stats.get(check_name, {})
        log.info(f"  ADV CHECK {check_name}: {len(d)} keys — NET_RATING={d.get('NET_RATING')}, PLUS_MINUS={d.get('PLUS_MINUS')}, D_FG_PCT={d.get('D_FG_PCT')}, CLUTCH_GP={d.get('CLUTCH_GP')}, HUSTLE_GP={d.get('HUSTLE_GP')}")
    lebron_adv = [k for k in _player_adv_stats if 'james' in k.lower() or 'lebron' in k.lower()]
    log.info(f"  LeBron variants in adv stats: {lebron_adv}")


def fetch_comparisons():
    """Build comparison screens: All-Star H2H and tonight's matchup previews."""
    global last_good_comparisons

    if "comp" in comparisons_cache:
        return comparisons_cache["comp"]

    if not last_good_team_ratings:
        log.warning("Comparisons: team ratings not loaded yet, skipping")
        return last_good_comparisons

    if not _player_adv_stats:
        log.warning("Comparisons: advanced stats not loaded yet, skipping")
        return last_good_comparisons

    screens = []

    # 1) TONIGHT'S MATCHUPS — best starter vs starter per game
    try:
        score_data = fetch_scores()
        games = score_data.get("games", [])
        preview_games = [g for g in games]  # all today's games
        if not preview_games:
            # No games today — get next game date
            upcoming = _get_upcoming_games()
            preview_games = upcoming
    except:
        preview_games = []

    for pg in preview_games:
        away_abbr = pg.get("away", {}).get("abbr", "")
        home_abbr = pg.get("home", {}).get("abbr", "")
        away_city = team_city(away_abbr)
        home_city = team_city(home_abbr)
        # Find starters for these teams
        away_starters = _depth_starters.get(away_city, [])
        home_starters = _depth_starters.get(home_city, [])
        if not away_starters or not home_starters:
            continue

        # Find best matchup: highest combined rating at compatible positions
        best = None
        best_combined = 0
        for a in away_starters:
            a_pos = a["pos"]
            a_rat = _get_rating(a["name"])
            if not a_rat:
                continue
            for h in home_starters:
                if h["pos"] not in _POS_COMPAT.get(a_pos, [a_pos]):
                    continue
                h_rat = _get_rating(h["name"])
                if not h_rat:
                    continue
                combined = a_rat + h_rat
                if combined > best_combined:
                    best_combined = combined
                    best = (a["name"], h["name"])

        if best:
            comp = _build_comparison(best[0], best[1])
            label = pg.get("label", "") or ""
            tip = pg.get("tip", "") or pg.get("clock", "") or ""
            subtitle = (label + " · " + tip).strip(" ·") or "Upcoming"
            screens.append({
                "title": f"{away_city} vs {home_city}",
                "subtitle": subtitle,
                "isComparison": True,
                "isPreview": True,
                "comparison": comp,
            })

    # 2) ALL-STAR HEAD-TO-HEAD — random pairs using static positions
    available = [n for n in _ALL_STARS if _get_rating(n)]
    no_rating = [n for n in _ALL_STARS if not _get_rating(n)]
    if no_rating:
        log.warning(f"  All-Stars excluded (no rating): {no_rating}")

    # Also exclude players with incomplete counting stats (missing STL/BLK/FG%)
    incomplete = []
    full_available = []
    for n in available:
        st = _find_stats(n)
        if st.get("STL") is not None and st.get("BLK") is not None:
            full_available.append(n)
        else:
            incomplete.append(n)
    if incomplete:
        log.warning(f"  All-Stars excluded (incomplete stats): {incomplete}")
    available = full_available
    log.info(f"  All-Stars available for H2H: {len(available)}/{len(_ALL_STARS)}")

    random.shuffle(available)
    used = set()
    allstar_pairs = []
    for a in available:
        if a in used:
            continue
        a_pos = _ALL_STARS[a]
        compat = _POS_COMPAT.get(a_pos, [a_pos])
        for b in available:
            if b == a or b in used:
                continue
            b_pos = _ALL_STARS[b]
            if b_pos in compat:
                allstar_pairs.append((a, b))
                used.add(a)
                used.add(b)
                break
        if len(allstar_pairs) >= 20:
            break

    for a, b in allstar_pairs:
        comp = _build_comparison(a, b)
        a_pos = _ALL_STARS.get(a, "")
        b_pos = _ALL_STARS.get(b, "")
        screens.append({
            "title": f"{a} vs {b}",
            "subtitle": f"All-Star H2H · {a_pos} vs {b_pos}",
            "isComparison": True,
            "comparison": comp,
        })

    result = {"screens": screens}
    comparisons_cache["comp"] = result
    last_good_comparisons = result

    log.info(f"Comparisons: {len(screens)} screens ("
             f"{sum(1 for s in screens if s.get('isPreview'))} preview, "
             f"{len(allstar_pairs)} all-star H2H)")
    return result


def _extract_broadcasters(bc_data):
    """Extract broadcaster names from NBA CDN broadcaster dict."""
    names = []
    for key in ("nationalBroadcasters", "nationalTvBroadcasters"):
        for b in bc_data.get(key, []):
            name = b.get("broadcastDisplay", "")
            if name and name not in names:
                names.append(name)
    if not names:
        for key in ("homeTvBroadcasters", "awayTvBroadcasters"):
            for b in bc_data.get(key, []):
                name = b.get("broadcastDisplay", "")
                if name and name not in names:
                    names.append(name)
    return names


def _get_upcoming_games():
    """Fetch next game date from NBA schedule when no games today."""
    try:
        url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
        log.info(f"Fetching upcoming games from {url}")
        resp = requests.get(url, headers=_NBA_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        _schedule_cache["data"] = data  # Cache for season series / rest days
        today_str = _now_et().strftime("%Y-%m-%d")
        log.info(f"  Schedule JSON loaded, looking for dates after {today_str} (ET)")
        dates = data.get("leagueSchedule", {}).get("gameDates", [])
        log.info(f"  Found {len(dates)} game dates in schedule")
        if not dates:
            log.warning("  No gameDates found in schedule JSON")
            return []

        # Log first few dates to see format
        for i, gd in enumerate(dates[:3]):
            log.info(f"  Sample date [{i}]: {gd.get('gameDate', '?')}")
        if len(dates) > 3:
            log.info(f"  ... and last date: {dates[-1].get('gameDate', '?')}")

        for gd in dates:
            raw_date = gd.get("gameDate", "")

            # Parse various date formats
            game_date = ""
            try:
                # Try ISO format: "2026-02-20T00:00:00Z" or "2026-02-20"
                if raw_date[:4].isdigit() and raw_date[4] == '-':
                    game_date = raw_date[:10]  # "2026-02-20"
                # Try US format: "02/20/2026 12:00:00 AM" or "2/20/2026"
                elif '/' in raw_date:
                    parts = raw_date.split(' ')[0].split('/')
                    if len(parts) == 3:
                        m, d, y = parts
                        game_date = f"{y}-{int(m):02d}-{int(d):02d}"
                # Try other formats with datetime parser
                else:
                    from dateutil import parser as dateparser
                    parsed = dateparser.parse(raw_date)
                    if parsed:
                        game_date = parsed.strftime("%Y-%m-%d")
            except Exception:
                # Last resort: just try to extract any 10-char date
                game_date = raw_date[:10]

            if not game_date or game_date <= today_str:
                continue
            games = []
            for g in gd.get("games", []):
                away = g.get("awayTeam", {})
                home = g.get("homeTeam", {})
                gt = g.get("gameDateTimeUTC", "")
                tip = ""
                if gt:
                    try:
                        from datetime import datetime as dt2
                        utc = dt2.fromisoformat(gt.replace("Z", "+00:00"))
                        try:
                            from zoneinfo import ZoneInfo
                            et = utc.astimezone(ZoneInfo("America/New_York"))
                        except Exception:
                            # Fallback: subtract 5 hours (EST approximation)
                            from datetime import timedelta
                            et = utc - timedelta(hours=5)
                        tip = et.strftime("%I:%M %p ET").lstrip("0")
                    except Exception as ex:
                        log.debug(f"  Time parse failed for {gt}: {ex}")
                month_day = ""
                try:
                    parts = game_date.split("-")
                    months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                    month_day = f"{months[int(parts[1])]} {int(parts[2])}"
                except:
                    pass

                # Extract broadcasters from schedule
                bc_data = g.get("broadcasters", {})
                bc_list = _extract_broadcasters(bc_data)

                games.append({
                    "away": {"abbr": away.get("teamTricode", ""), "teamId": away.get("teamId")},
                    "home": {"abbr": home.get("teamTricode", ""), "teamId": home.get("teamId")},
                    "status": "scheduled",
                    "label": month_day,
                    "gameDate": game_date,
                    "tip": tip,
                    "broadcasters": bc_list,
                })
            if games:
                log.info(f"  Found {len(games)} upcoming games on {game_date}")
                return games
        log.warning(f"  No future game dates found after {today_str} (checked {len(dates)} dates)")
    except Exception as e:
        log.warning(f"Upcoming games fetch failed: {e}")
        import traceback
        log.warning(traceback.format_exc())
    return []


@app.route("/api/comparisons")
def api_comparisons():
    """Return comparison/matchup screens."""
    data = fetch_comparisons()
    return jsonify(data)


# ═══════════════════════════════════════
# INJURY REPORTS (Google Sheets)
# ═══════════════════════════════════════

_INJURIES_SHEET_ID = "14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY"
_INJURIES_GID = "306285159"
_INJURIES_JSON_URL = "https://aderoa.github.io/Injuries/injuries.json"

injuries_cache = TTLCache(maxsize=1, ttl=900)  # 15 min
last_good_injuries = []
_questionable_players = set()  # Players with Questionable/Doubtful/Game Time Decision status


def fetch_injuries():
    """Fetch injury report data from GitHub JSON (primary) with team lookup from depth charts."""
    global last_good_injuries

    if "injuries" in injuries_cache:
        return injuries_cache["injuries"]

    log.info("Fetching injury reports from GitHub JSON...")

    try:
        resp = requests.get(_INJURIES_JSON_URL, timeout=20)
        resp.raise_for_status()
        entries = resp.json()
    except Exception as e:
        log.warning(f"GitHub injuries fetch failed: {e}")
        return last_good_injuries

    if not entries:
        log.warning("GitHub injuries JSON empty")
        return last_good_injuries

    # Build latest status per player (last entry wins, chronological order)
    latest = {}
    for e in entries:
        player = e.get("player", "").strip()
        if not player:
            continue
        latest[player] = {
            "status": e.get("status", ""),
            "injury": e.get("injury", ""),
            "date": e.get("date", ""),
        }

    log.info(f"  GitHub injuries: {len(latest)} unique players from {len(entries)} entries")

    # Group by team using _player_team_map (from depth charts)
    teams = {}
    unmatched = []
    for player, info in latest.items():
        status = info["status"].strip()
        injury = info["injury"].strip()

        # Skip healthy/available players
        if status.lower() in ("available", ""):
            if not injury:
                continue

        # Look up team
        team = _player_team_map.get(player, "")
        if not team:
            # Try resolve_player_name for alternate spellings
            resolved = resolve_player_name(player)
            if resolved != player:
                team = _player_team_map.get(resolved, "")
        if not team:
            unmatched.append(player)
            continue

        if team not in teams:
            teams[team] = []
        teams[team].append({
            "name": player,
            "status": status,
            "injury": injury,
            "date": info["date"],
            "salary": _player_salary_map.get(player, ""),
            "country": _PLAYER_COUNTRY.get(player, ""),
        })

    if unmatched:
        log.info(f"  Injuries: {len(unmatched)} players not matched to teams: {unmatched[:15]}")

    if not teams:
        log.warning("Injuries: no teams found — depth charts may not be loaded yet")
        return last_good_injuries

    # Build screens from teams dict
    MAX_PER_COL = 14

    # Update global questionable set for depth chart cross-reference
    _questionable_players.clear()
    for team_name, team_players in teams.items():
        for p in team_players:
            st = (p["status"] or "").lower()
            if st in ("questionable", "doubtful", "game time decision", "day-to-day"):
                raw_name = p["name"]
                _questionable_players.add(raw_name)
                resolved = resolve_player_name(raw_name)
                if resolved != raw_name:
                    _questionable_players.add(resolved)
    log.info(f"  Questionable/Doubtful players: {len(_questionable_players)} — {list(_questionable_players)[:10]}")

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
# TEAM STANDINGS (NBA stats API)
# ═══════════════════════════════════════

standings_cache = TTLCache(maxsize=1, ttl=3600)  # 1 hour


def fetch_standings():
    """Fetch current NBA standings from stats.nba.com."""
    global _team_standings

    if "st" in standings_cache:
        return standings_cache["st"]

    log.info("Fetching NBA standings from stats.nba.com...")
    url = "https://stats.nba.com/stats/leaguestandings"
    params = {
        "LeagueID": "00",
        "Season": "2025-26",
        "SeasonType": "Regular Season",
        "SeasonYear": "",
    }

    try:
        # Try with longer timeout and retry
        for attempt in range(2):
            try:
                resp = requests.get(url, params=params, headers=_NBA_STATS_HEADERS, timeout=45)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception:
                if attempt == 0:
                    log.info("Standings: retrying stats.nba.com...")
                    import time; time.sleep(2)
                else:
                    raise
    except Exception as e:
        log.warning(f"Standings fetch from stats.nba.com failed: {e}")
        cdn_urls = [
            "https://cdn.nba.com/static/json/liveData/standings/standings_00.json",
            "https://cdn.nba.com/static/json/staticData/standings/standings_00.json",
        ]
        for cdn_url in cdn_urls:
            try:
                log.info(f"Trying standings fallback: {cdn_url}")
                resp2 = requests.get(cdn_url, headers=_NBA_STATS_HEADERS, timeout=15)
                resp2.raise_for_status()
                cdn = resp2.json()
                standings_list = cdn.get("standings", cdn.get("league", {}).get("standard", {}).get("teams", []))
                if isinstance(standings_list, dict):
                    entries = standings_list.get("entries", [])
                elif isinstance(standings_list, list):
                    entries = standings_list
                else:
                    entries = []
                _team_standings.clear()
                for team in entries:
                    team_id = team.get("teamId", 0)
                    abbr = _TEAM_ID_TO_ABBR.get(team_id, "")
                    if not abbr:
                        abbr = team.get("teamTricode", "") or team.get("teamAbbreviation", "")
                    wins = team.get("wins", team.get("w", 0))
                    losses = team.get("losses", team.get("l", 0))
                    conf = team.get("conference", team.get("confName", ""))
                    conf_rank = team.get("playoffRank", team.get("confRank", team.get("seed", 0)))
                    ppg = team.get("pointsFor", 0)
                    opp_ppg = team.get("pointsAgainst", 0)
                    gp = wins + losses
                    if gp > 0:
                        ppg_avg = round(ppg / gp, 1) if ppg > 100 else ppg
                        opp_avg = round(opp_ppg / gp, 1) if opp_ppg > 100 else opp_ppg
                    else:
                        ppg_avg = ppg
                        opp_avg = opp_ppg
                    diff = round(ppg_avg - opp_avg, 1)
                    streak_val = team.get("streak", team.get("strCurrentStreak", ""))
                    l10_val = team.get("last10", team.get("l10", team.get("L10Record", "")))
                    home_rec = team.get("home", team.get("homeRecord", ""))
                    road_rec = team.get("road", team.get("awayRecord", team.get("roadRecord", "")))
                    entry = {
                        "teamId": team_id, "city": team.get("teamCity", ""),
                        "name": team.get("teamName", ""), "abbr": abbr,
                        "wins": wins, "losses": losses, "conf": conf,
                        "confRank": conf_rank, "ppg": ppg_avg, "oppPpg": opp_avg,
                        "diffPpg": diff, "streak": str(streak_val), "l10": str(l10_val),
                        "home": str(home_rec), "road": str(road_rec),
                    }
                    if abbr:
                        _team_standings[abbr] = entry
                    city = team.get("teamCity", "")
                    if city:
                        _team_standings[city] = entry
                if _team_standings:
                    standings_cache["st"] = _team_standings
                    log.info(f"Standings (CDN fallback): loaded {len(entries)} teams from {cdn_url}")
                    return _team_standings
                else:
                    log.warning(f"CDN standings: no teams parsed from {cdn_url}")
            except Exception as e2:
                log.warning(f"Standings fallback failed ({cdn_url}): {e2}")

        # Last resort: ESPN API
        try:
            log.info("Trying ESPN standings fallback...")
            espn_url = "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings"
            resp3 = requests.get(espn_url, timeout=15)
            resp3.raise_for_status()
            espn = resp3.json()
            _team_standings.clear()
            # ESPN structure: children[] → standings → entries[]
            for conf_group in espn.get("children", []):
                conf_name = conf_group.get("name", "")  # "Eastern Conference" etc.
                conf_short = "East" if "east" in conf_name.lower() else "West"
                for entry in conf_group.get("standings", {}).get("entries", []):
                    team_data = entry.get("team", {})
                    espn_abbr = team_data.get("abbreviation", "")
                    # Map ESPN abbreviations to NBA tricodes
                    espn_to_nba = {"GS": "GSW", "SA": "SAS", "NY": "NYK", "NO": "NOP",
                                    "WSH": "WAS", "PHO": "PHX", "UTAH": "UTA", "BKN": "BKN"}
                    abbr = espn_to_nba.get(espn_abbr, espn_abbr)
                    stats_dict = {}
                    for st in entry.get("stats", []):
                        stats_dict[st.get("name", "")] = st.get("value", 0)
                        if st.get("displayValue"):
                            stats_dict[st.get("name", "") + "_disp"] = st.get("displayValue", "")
                    wins = int(stats_dict.get("wins", 0))
                    losses = int(stats_dict.get("losses", 0))
                    # Fallback: parse from overall record if wins/losses stats missing
                    if not wins and not losses:
                        overall = stats_dict.get("overall_disp", "") or stats_dict.get("Overall_disp", "")
                        if not overall:
                            # Try entry-level record
                            for rec in entry.get("records", []):
                                if rec.get("type") == "total" or rec.get("name") == "overall":
                                    overall = rec.get("displayValue", rec.get("summary", ""))
                                    break
                        if overall and "-" in overall:
                            try:
                                parts = overall.split("-")
                                wins = int(parts[0])
                                losses = int(parts[1])
                            except (ValueError, IndexError):
                                pass
                    gp = wins + losses or 1  # Avoid division by zero
                    pts_total = stats_dict.get("pointsFor", 0)
                    opp_total = stats_dict.get("pointsAgainst", 0)
                    # ESPN gives season totals — convert to per-game
                    ppg = round(pts_total / gp, 1)
                    opp_ppg = round(opp_total / gp, 1)
                    diff = round(ppg - opp_ppg, 1)
                    streak_disp = stats_dict.get("streak_disp", "")
                    # L10: try several ESPN stat name variants
                    l10 = (stats_dict.get("Last Ten Games Record_disp", "")
                           or stats_dict.get("record_disp", "")
                           or stats_dict.get("last10Record_disp", ""))
                    home_rec = (stats_dict.get("Home_disp", "")
                                or stats_dict.get("homeRecord_disp", "")
                                or stats_dict.get("Home Record_disp", ""))
                    road_rec = (stats_dict.get("Road_disp", "")
                                or stats_dict.get("awayRecord_disp", "")
                                or stats_dict.get("Away Record_disp", "")
                                or stats_dict.get("Road Record_disp", ""))
                    rank = int(stats_dict.get("playoffSeed", 0))
                    e = {
                        "teamId": 0, "city": team_data.get("location", ""),
                        "name": team_data.get("name", ""), "abbr": abbr,
                        "wins": wins, "losses": losses, "conf": conf_short,
                        "confRank": rank, "ppg": ppg, "oppPpg": opp_ppg,
                        "diffPpg": diff, "streak": streak_disp, "l10": l10,
                        "home": home_rec, "road": road_rec,
                    }
                    if abbr:
                        _team_standings[abbr] = e
                    loc = team_data.get("location", "")
                    if loc:
                        _team_standings[loc] = e
            if _team_standings:
                # Log one team's stat names for debugging
                sample_abbr = next(iter(_team_standings), "")
                if sample_abbr:
                    s = _team_standings[sample_abbr]
                    log.info(f"  ESPN sample ({sample_abbr}): {s['wins']}-{s['losses']}, PPG={s['ppg']}, OPP={s['oppPpg']}, L10={s['l10']}, Home={s['home']}, Road={s['road']}, Streak={s['streak']}")
                # Log all stat names from first entry for future reference
                _first_logged = False
                for conf_group2 in espn.get("children", []):
                    for entry2 in conf_group2.get("standings", {}).get("entries", []):
                        stat_names = [st.get("name", "") for st in entry2.get("stats", [])]
                        log.info(f"  ESPN stat names: {stat_names}")
                        # Log all stat values for first team
                        all_vals = {st.get("name", ""): st.get("value", "?") for st in entry2.get("stats", [])}
                        log.info(f"  ESPN stat values (first team): {all_vals}")
                        # Also check records array
                        recs = entry2.get("records", [])
                        if recs:
                            log.info(f"  ESPN records: {recs[:3]}")
                        _first_logged = True
                        break
                    if _first_logged:
                        break
                standings_cache["st"] = _team_standings
                log.info(f"Standings (ESPN fallback): loaded {len(_team_standings)//2} teams")
                return _team_standings
            else:
                log.warning("ESPN standings: no teams parsed")
        except Exception as e3:
            log.warning(f"ESPN standings fallback failed: {e3}")

        # All fallbacks failed
        log.warning("All standings sources failed")
        return _team_standings

    rs = data.get("resultSets", [{}])[0]
    headers = rs.get("headers", [])
    rows = rs.get("rowSet", [])
    col = {h: i for i, h in enumerate(headers)}

    _team_standings.clear()
    for row in rows:
        abbr = ""
        # Build tricode from TeamCity + TeamName
        team_city_raw = row[col["TeamCity"]] if "TeamCity" in col else ""
        team_name_raw = row[col["TeamName"]] if "TeamName" in col else ""
        team_id = row[col["TeamID"]] if "TeamID" in col else 0

        # Map teamId to tricode
        abbr = _TEAM_ID_TO_ABBR.get(team_id, "")

        entry = {
            "teamId": team_id,
            "city": team_city_raw,
            "name": team_name_raw,
            "abbr": abbr,
            "wins": row[col.get("WINS", 0)] if "WINS" in col else 0,
            "losses": row[col.get("LOSSES", 0)] if "LOSSES" in col else 0,
            "winPct": row[col.get("WinPCT", 0)] if "WinPCT" in col else 0,
            "conf": row[col.get("Conference", 0)] if "Conference" in col else "",
            "confRank": row[col.get("PlayoffRank", 0)] if "PlayoffRank" in col else 0,
            "streak": row[col.get("strCurrentStreak", 0)] if "strCurrentStreak" in col else "",
            "l10": row[col.get("L10", 0)] if "L10" in col else "",
            "ppg": row[col.get("PointsPG", 0)] if "PointsPG" in col else 0,
            "oppPpg": row[col.get("OppPointsPG", 0)] if "OppPointsPG" in col else 0,
            "diffPpg": row[col.get("DiffPointsPG", 0)] if "DiffPointsPG" in col else 0,
            "home": row[col.get("HOME", 0)] if "HOME" in col else "",
            "road": row[col.get("ROAD", 0)] if "ROAD" in col else "",
        }
        if abbr:
            _team_standings[abbr] = entry
        # Also key by city name for lookups
        if team_city_raw:
            _team_standings[team_city_raw] = entry

    standings_cache["st"] = _team_standings
    log.info(f"Standings: loaded {len(rows)} teams")
    return _team_standings


# TeamID → tricode mapping
_TEAM_ID_TO_ABBR = {
    1610612737: "ATL", 1610612738: "BOS", 1610612751: "BKN", 1610612766: "CHA",
    1610612741: "CHI", 1610612739: "CLE", 1610612742: "DAL", 1610612743: "DEN",
    1610612765: "DET", 1610612744: "GSW", 1610612745: "HOU", 1610612754: "IND",
    1610612746: "LAC", 1610612747: "LAL", 1610612763: "MEM", 1610612748: "MIA",
    1610612749: "MIL", 1610612750: "MIN", 1610612740: "NOP", 1610612752: "NYK",
    1610612760: "OKC", 1610612753: "ORL", 1610612755: "PHI", 1610612756: "PHX",
    1610612757: "POR", 1610612758: "SAC", 1610612759: "SAS", 1610612761: "TOR",
    1610612762: "UTA", 1610612764: "WAS",
}

# Reverse: tricode → teamID (for logo URLs)
_ABBR_TO_TEAM_ID = {v: k for k, v in _TEAM_ID_TO_ABBR.items()}


def _team_logo_url(abbr):
    """Return NBA CDN logo URL for a team abbreviation."""
    tid = _ABBR_TO_TEAM_ID.get(abbr, 0)
    if tid:
        return f"https://cdn.nba.com/logos/nba/{tid}/primary/L/logo.svg"
    return ""


# ═══════════════════════════════════════
# PREVIEW HELPERS: Season Series, Rest Days, Team Stats, Win Prob
# ═══════════════════════════════════════

def _parse_schedule_date(raw_date):
    """Parse a schedule date string into YYYY-MM-DD format."""
    try:
        if raw_date[:4].isdigit() and raw_date[4] == '-':
            return raw_date[:10]
        elif '/' in raw_date:
            parts = raw_date.split(' ')[0].split('/')
            if len(parts) == 3:
                m, d, y = parts
                return f"{y}-{int(m):02d}-{int(d):02d}"
    except Exception:
        pass
    return ""


def _get_season_series(away_abbr, home_abbr):
    """Get H2H record and game results from the cached schedule."""
    data = _schedule_cache.get("data")
    if not data:
        return None
    today_str = datetime.now().strftime("%Y-%m-%d")
    dates = data.get("leagueSchedule", {}).get("gameDates", [])
    away_wins = 0
    home_wins = 0
    game_results = []
    for gd in dates:
        game_date = _parse_schedule_date(gd.get("gameDate", ""))
        if not game_date or game_date >= today_str:
            continue
        for g in gd.get("games", []):
            status = g.get("gameStatus", 0)
            if status != 3:  # Only completed games
                continue
            a_tri = g.get("awayTeam", {}).get("teamTricode", "")
            h_tri = g.get("homeTeam", {}).get("teamTricode", "")
            a_score = g.get("awayTeam", {}).get("score", 0)
            h_score = g.get("homeTeam", {}).get("score", 0)
            # Check if this game involves both teams
            is_match = False
            if a_tri == away_abbr and h_tri == home_abbr:
                is_match = True
            elif a_tri == home_abbr and h_tri == away_abbr:
                is_match = True
            if not is_match:
                continue
            winner = a_tri if a_score > h_score else h_tri
            if winner == away_abbr:
                away_wins += 1
            else:
                home_wins += 1
            game_results.append({
                "date": game_date,
                "away": a_tri,
                "home": h_tri,
                "awayScore": a_score,
                "homeScore": h_score,
                "winner": winner,
            })
    if not game_results:
        return None
    total = away_wins + home_wins
    if away_wins > home_wins:
        summary = f"{team_city(away_abbr)} leads {away_wins}-{home_wins}"
    elif home_wins > away_wins:
        summary = f"{team_city(home_abbr)} leads {home_wins}-{away_wins}"
    else:
        summary = f"Tied {away_wins}-{home_wins}"
    return {
        "awayWins": away_wins,
        "homeWins": home_wins,
        "total": total,
        "summary": summary,
        "games": game_results[-4:],  # Last 4 games max
    }


def _get_rest_days(team_abbr, target_date_str):
    """Calculate rest days for a team before a specific date."""
    data = _schedule_cache.get("data")
    if not data:
        return {"days": None, "isB2B": False, "label": ""}
    dates = data.get("leagueSchedule", {}).get("gameDates", [])
    last_game_date = ""
    for gd in dates:
        game_date = _parse_schedule_date(gd.get("gameDate", ""))
        if not game_date or game_date >= target_date_str:
            continue
        for g in gd.get("games", []):
            status = g.get("gameStatus", 0)
            if status < 2:  # Preseason etc. may be status 1 scheduled
                continue
            a_tri = g.get("awayTeam", {}).get("teamTricode", "")
            h_tri = g.get("homeTeam", {}).get("teamTricode", "")
            if team_abbr in (a_tri, h_tri):
                if game_date > last_game_date:
                    last_game_date = game_date
    if not last_game_date:
        return {"days": None, "isB2B": False, "label": ""}
    try:
        from datetime import datetime as dt2
        d1 = dt2.strptime(last_game_date, "%Y-%m-%d")
        d2 = dt2.strptime(target_date_str, "%Y-%m-%d")
        rest = (d2 - d1).days - 1  # Days off between games
        is_b2b = rest == 0
        if is_b2b:
            label = "B2B ⚠️"
        elif rest == 1:
            label = "1 day rest"
        elif rest >= 3:
            label = f"{rest} days rest ✅"
        else:
            label = f"{rest} days rest"
        return {"days": rest, "isB2B": is_b2b, "label": label}
    except Exception:
        return {"days": None, "isB2B": False, "label": ""}


def _get_last5(team_abbr, target_date_str):
    """Get last 5 game results for a team: 'WWLWL'."""
    data = _schedule_cache.get("data")
    if not data:
        return ""
    dates = data.get("leagueSchedule", {}).get("gameDates", [])
    results = []
    for gd in dates:
        game_date = _parse_schedule_date(gd.get("gameDate", ""))
        if not game_date or game_date >= target_date_str:
            continue
        for g in gd.get("games", []):
            if g.get("gameStatus", 0) != 3:
                continue
            a_tri = g.get("awayTeam", {}).get("teamTricode", "")
            h_tri = g.get("homeTeam", {}).get("teamTricode", "")
            if team_abbr not in (a_tri, h_tri):
                continue
            a_sc = g.get("awayTeam", {}).get("score", 0)
            h_sc = g.get("homeTeam", {}).get("score", 0)
            try:
                a_sc, h_sc = int(a_sc), int(h_sc)
            except (ValueError, TypeError):
                continue
            if team_abbr == h_tri:
                results.append((game_date, "W" if h_sc > a_sc else "L"))
            else:
                results.append((game_date, "W" if a_sc > h_sc else "L"))
    results.sort(key=lambda x: x[0], reverse=True)
    return "".join(r[1] for r in results[:5])


team_adv_cache = TTLCache(maxsize=1, ttl=3600)  # 1 hour


def _fetch_team_advanced_stats():
    """Fetch team ORTG/DRTG/Pace from stats.nba.com or ESPN."""
    global _team_adv_stats

    if "ta" in team_adv_cache:
        return _team_adv_stats

    # Try stats.nba.com advanced team stats
    try:
        log.info("Fetching team advanced stats from stats.nba.com...")
        url = "https://stats.nba.com/stats/leaguedashteamstats"
        params = {
            "Conference": "", "DateFrom": "", "DateTo": "",
            "Division": "", "GameScope": "", "GameSegment": "",
            "Height": "", "ISTRound": "", "LastNGames": "0",
            "LeagueID": "00", "Location": "", "MeasureType": "Advanced",
            "Month": "0", "OpponentTeamID": "0", "Outcome": "",
            "PORound": "0", "PaceAdjust": "N", "PerMode": "PerGame",
            "Period": "0", "PlayerExperience": "", "PlayerPosition": "",
            "PlusMinus": "N", "Rank": "N", "Season": "2025-26",
            "SeasonSegment": "", "SeasonType": "Regular Season",
            "ShotClockRange": "", "StarterBench": "", "TeamID": "0",
            "TwoWay": "0", "VsConference": "", "VsDivision": "",
        }
        resp = requests.get(url, params=params, headers=_NBA_STATS_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rs = data.get("resultSets", [{}])[0]
        headers = rs.get("headers", [])
        rows = rs.get("rowSet", [])
        col = {h: i for i, h in enumerate(headers)}
        _team_adv_stats.clear()
        for row in rows:
            tid = row[col.get("TEAM_ID", 0)] if "TEAM_ID" in col else 0
            abbr = _TEAM_ID_TO_ABBR.get(tid, "")
            if not abbr:
                continue
            _team_adv_stats[abbr] = {
                "ortg": round(float(row[col["OFF_RATING"]] or 0), 1) if "OFF_RATING" in col else 0,
                "drtg": round(float(row[col["DEF_RATING"]] or 0), 1) if "DEF_RATING" in col else 0,
                "netRtg": round(float(row[col["NET_RATING"]] or 0), 1) if "NET_RATING" in col else 0,
                "pace": round(float(row[col["PACE"]] or 0), 1) if "PACE" in col else 0,
            }
        if _team_adv_stats:
            team_adv_cache["ta"] = True
            log.info(f"Team advanced stats: loaded {len(_team_adv_stats)} teams")
            return _team_adv_stats
    except Exception as e:
        log.warning(f"Team advanced stats (stats.nba.com) failed: {e}")

    # Fallback: try NBA CDN team stats
    try:
        log.info("Trying team stats from NBA CDN...")
        url2 = "https://cdn.nba.com/static/json/liveData/odds/odds_todaysGames.json"
        # This might not have advanced stats — skip for now
    except Exception:
        pass

    log.warning("Team advanced stats unavailable")
    team_adv_cache["ta"] = True  # Don't retry constantly
    return _team_adv_stats


clutch_cache = TTLCache(maxsize=1, ttl=3600)
_team_clutch_stats = {}


def _fetch_team_clutch_stats():
    """Fetch team clutch records from stats.nba.com."""
    global _team_clutch_stats
    if "cl" in clutch_cache:
        return _team_clutch_stats
    try:
        log.info("Fetching team clutch stats...")
        url = "https://stats.nba.com/stats/leaguedashteamclutch"
        params = {
            "AheadBehind": "Ahead or Behind",
            "ClutchTime": "Last 5 Minutes",
            "Conference": "", "DateFrom": "", "DateTo": "",
            "Division": "", "GameScope": "", "GameSegment": "",
            "ISTRound": "", "LastNGames": "0",
            "LeagueID": "00", "Location": "", "MeasureType": "Base",
            "Month": "0", "OpponentTeamID": "0", "Outcome": "",
            "PORound": "0", "PaceAdjust": "N", "PerMode": "Totals",
            "Period": "0", "PlayerExperience": "", "PlayerPosition": "",
            "PlusMinus": "N", "PointDiff": "5",
            "Rank": "N", "Season": "2025-26",
            "SeasonSegment": "", "SeasonType": "Regular Season",
            "ShotClockRange": "", "StarterBench": "", "TeamID": "0",
            "TwoWay": "0", "VsConference": "", "VsDivision": "",
        }
        resp = requests.get(url, params=params, headers=_NBA_STATS_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rs = data.get("resultSets", [{}])[0]
        headers = rs.get("headers", [])
        rows = rs.get("rowSet", [])
        col = {h: i for i, h in enumerate(headers)}
        _team_clutch_stats.clear()
        for row in rows:
            tid = row[col.get("TEAM_ID", 0)] if "TEAM_ID" in col else 0
            abbr = _TEAM_ID_TO_ABBR.get(tid, "")
            if not abbr:
                continue
            w = int(row[col.get("W", 0)] or 0) if "W" in col else 0
            l = int(row[col.get("L", 0)] or 0) if "L" in col else 0
            _team_clutch_stats[abbr] = {
                "wins": w,
                "losses": l,
                "record": f"{w}-{l}",
                "gp": w + l,
            }
        if _team_clutch_stats:
            clutch_cache["cl"] = True
            log.info(f"Team clutch stats: loaded {len(_team_clutch_stats)} teams")
    except Exception as e:
        log.warning(f"Team clutch stats failed: {e}")
        clutch_cache["cl"] = True
    return _team_clutch_stats


def _get_rating_trend(team_abbr):
    """Compute average GR trend (7d vs season) for team's starters."""
    city = team_city(team_abbr)
    starters = _depth_starters.get(city, [])
    if not starters:
        return None
    diffs = []
    for s in starters:
        name = s["name"]
        r7 = _player_rating_7d.get(name)
        rs = _player_rating_season.get(name) or _get_rating(name)
        if r7 and rs:
            diffs.append(r7 - rs)
    if not diffs:
        return None
    avg_diff = sum(diffs) / len(diffs)
    if avg_diff > 1.0:
        label = "Trending Up 📈"
    elif avg_diff < -1.0:
        label = "Trending Down 📉"
    else:
        label = "Steady ➡️"
    return {
        "avgDiff": round(avg_diff, 1),
        "label": label,
        "playersUp": sum(1 for d in diffs if d > 1.0),
        "playersDown": sum(1 for d in diffs if d < -1.0),
        "total": len(diffs),
    }


espn_pred_cache = TTLCache(maxsize=1, ttl=600)  # 10 min
_espn_team_records = {}  # "OKC" → {"wins": 38, "losses": 9, "record": "38-9"}


def _fetch_espn_predictions(game_date_str):
    """Fetch win probabilities AND team records from ESPN scoreboard API."""
    global _espn_team_records
    if "ep" in espn_pred_cache:
        return espn_pred_cache.get("data", {})
    try:
        date_compact = game_date_str.replace("-", "")  # "20260220"
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_compact}"
        log.info(f"Fetching ESPN predictions for {game_date_str}...")
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        predictions = {}
        espn_to_nba = {"GS": "GSW", "SA": "SAS", "NY": "NYK", "NO": "NOP",
                        "WSH": "WAS", "PHO": "PHX", "UTAH": "UTA"}
        for event in data.get("events", []):
            comp = (event.get("competitions") or [{}])[0]
            teams = comp.get("competitors", [])
            if len(teams) < 2:
                continue
            home_data = teams[0] if teams[0].get("homeAway") == "home" else teams[1]
            away_data = teams[1] if teams[0].get("homeAway") == "home" else teams[0]
            home_abbr_espn = home_data.get("team", {}).get("abbreviation", "")
            away_abbr_espn = away_data.get("team", {}).get("abbreviation", "")
            home_abbr = espn_to_nba.get(home_abbr_espn, home_abbr_espn)
            away_abbr = espn_to_nba.get(away_abbr_espn, away_abbr_espn)
            # Extract team records from competitors
            for cd, ab in [(home_data, home_abbr), (away_data, away_abbr)]:
                for rec in cd.get("records", []):
                    rec_name = rec.get("name", "") or rec.get("type", "")
                    summary = rec.get("summary", "")
                    if not summary or "-" not in summary:
                        continue
                    try:
                        w, l = summary.split("-")
                        w, l = int(w), int(l)
                    except (ValueError, IndexError):
                        continue
                    if rec_name in ("overall", "total", ""):
                        _espn_team_records.setdefault(ab, {})
                        _espn_team_records[ab]["wins"] = w
                        _espn_team_records[ab]["losses"] = l
                        _espn_team_records[ab]["record"] = summary
                    elif rec_name in ("Home", "home"):
                        _espn_team_records.setdefault(ab, {})
                        _espn_team_records[ab]["home"] = summary
                    elif rec_name in ("Road", "road", "Away", "away"):
                        _espn_team_records.setdefault(ab, {})
                        _espn_team_records[ab]["road"] = summary
            # Win probability
            predictor = comp.get("predictor", {})
            home_pct = predictor.get("homeTeam", {}).get("gameProjection")
            away_pct = predictor.get("awayTeam", {}).get("gameProjection")
            # Spread and Over/Under
            spread = None
            over_under = None
            for line in comp.get("odds", []):
                if "details" in line and not spread:
                    spread = line.get("details", "")
                if "overUnder" in line and not over_under:
                    over_under = line.get("overUnder")
                if spread and over_under:
                    break
            key = f"{away_abbr}@{home_abbr}"
            predictions[key] = {
                "homeWinPct": round(float(home_pct), 1) if home_pct else None,
                "awayWinPct": round(float(away_pct), 1) if away_pct else None,
                "spread": spread,
                "overUnder": round(float(over_under), 1) if over_under else None,
            }
        espn_pred_cache["ep"] = True
        espn_pred_cache["data"] = predictions
        log.info(f"ESPN predictions: {len(predictions)} games, records for {len(_espn_team_records)} teams")
        if _espn_team_records:
            sample = next(iter(_espn_team_records.items()))
            log.info(f"  ESPN record sample: {sample[0]} → {sample[1]}")
        return predictions
    except Exception as e:
        log.warning(f"ESPN predictions fetch failed: {e}")
        espn_pred_cache["ep"] = True
        espn_pred_cache["data"] = {}
        return {}


# ═══════════════════════════════════════
# GAME PREVIEWS
# ═══════════════════════════════════════

preview_cache = TTLCache(maxsize=1, ttl=600)  # 10 min
last_good_previews = {"screens": []}


def _get_team_injuries(team_abbr):
    """Get injured players for a team from cached injury data."""
    city = team_city(team_abbr)
    result = []
    for scr in last_good_injuries:
        for side in ("left", "right"):
            current_team = None
            for entry in scr.get(side, []):
                if entry.get("isHeader"):
                    current_team = entry.get("team", "")
                elif current_team and (
                    current_team == city or
                    team_abbr in current_team or
                    city.lower() in current_team.lower()
                ):
                    status = (entry.get("status") or "").lower()
                    if status in ("out", "doubtful", "questionable", "day-to-day", "game time decision"):
                        result.append({
                            "name": entry.get("name", ""),
                            "status": entry.get("status", ""),
                            "injury": entry.get("injury", ""),
                        })
    return result


def _build_matchups(away_abbr, home_abbr):
    """Build position-by-position starter matchups for two teams."""
    away_city = team_city(away_abbr)
    home_city = team_city(home_abbr)
    away_starters = _depth_starters.get(away_city, [])
    home_starters = _depth_starters.get(home_city, [])

    if not away_starters or not home_starters:
        return []

    matchups = []
    pos_order = ["PG", "SG", "SF", "PF", "C"]

    for pos in pos_order:
        away_p = next((s for s in away_starters if s["pos"] == pos), None)
        home_p = next((s for s in home_starters if s["pos"] == pos), None)
        if not away_p or not home_p:
            continue

        a_name = away_p["name"]
        h_name = home_p["name"]
        a_rat = _get_rating(a_name) or 0
        h_rat = _get_rating(h_name) or 0
        a_stats = _find_stats(a_name)
        h_stats = _find_stats(h_name)

        matchups.append({
            "pos": pos,
            "away": {
                "name": a_name,
                "rating": round(a_rat, 1),
                "pts": round(float(a_stats.get("PTS", 0) or 0), 1),
                "reb": round(float(a_stats.get("REB", 0) or 0), 1),
                "ast": round(float(a_stats.get("AST", 0) or 0), 1),
            },
            "home": {
                "name": h_name,
                "rating": round(h_rat, 1),
                "pts": round(float(h_stats.get("PTS", 0) or 0), 1),
                "reb": round(float(h_stats.get("REB", 0) or 0), 1),
                "ast": round(float(h_stats.get("AST", 0) or 0), 1),
            },
            "winner": "away" if a_rat > h_rat else ("home" if h_rat > a_rat else "tie"),
        })

    return matchups


def _get_hot_hand(away_abbr, home_abbr):
    """Find players who are surging in Last 7 Days vs Season rating."""
    hot = []
    away_city = team_city(away_abbr)
    home_city = team_city(home_abbr)

    for city, abbr in [(away_city, away_abbr), (home_city, home_abbr)]:
        starters = _depth_starters.get(city, [])
        for s in starters:
            name = s["name"]
            r7 = _player_rating_7d.get(name)
            rs = _player_rating_season.get(name) or _get_rating(name)
            if r7 and rs and r7 > rs + 2.0:  # At least +2.0 surge
                hot.append({
                    "name": name,
                    "team": city,
                    "abbr": abbr,
                    "rating7d": round(r7, 1),
                    "ratingSeason": round(rs, 1),
                    "diff": round(r7 - rs, 1),
                })
    # Sort by biggest surge
    hot.sort(key=lambda x: x["diff"], reverse=True)
    return hot[:3]  # Top 3 surging players per game


def fetch_game_previews():
    """Build game preview screens for today's (or next) games."""
    global last_good_previews

    if "gp" in preview_cache:
        return preview_cache["gp"]

    # Get today's games or upcoming
    try:
        score_data = fetch_scores()
        if isinstance(score_data, list):
            games = score_data
        else:
            games = score_data.get("games", []) if isinstance(score_data, dict) else []
    except Exception:
        games = []

    # Use ALL today's games for previews (scheduled, live, or final)
    # Only fall back to upcoming when there are literally no games today
    preview_games = [g for g in games if g.get("away", {}).get("abbr")]
    upcoming_label = ""

    if not preview_games:
        # No games today at all — get next day's games
        log.info("Game previews: no games today, looking for upcoming...")
        upcoming = _get_upcoming_games()
        if upcoming:
            preview_games = upcoming
            upcoming_label = upcoming[0].get("label", "")
            log.info(f"Game previews: found {len(upcoming)} upcoming games ({upcoming_label})")
        else:
            log.info("Game previews: no upcoming games returned")

    if not preview_games:
        log.info("Game previews: no upcoming games found")
        return last_good_previews

    # Ensure standings are loaded
    fetch_standings()

    # Fetch team advanced stats (ORTG/DRTG/Pace)
    _fetch_team_advanced_stats()

    # Fetch clutch stats
    _fetch_team_clutch_stats()

    # Determine game date for ESPN predictions
    game_date = ""
    for g in preview_games:
        gd = g.get("gameDate", "")
        if gd:
            game_date = gd
            break
    if not game_date:
        game_date = _now_et().strftime("%Y-%m-%d")

    # Fetch ESPN predictions / win probabilities
    predictions = _fetch_espn_predictions(game_date)

    screens = []
    for g in preview_games:
        away_abbr = g.get("away", {}).get("abbr", "")
        home_abbr = g.get("home", {}).get("abbr", "")
        if not away_abbr or not home_abbr:
            continue

        away_city = team_city(away_abbr)
        home_city = team_city(home_abbr)

        # Standings data
        away_st = _team_standings.get(away_abbr, {})
        home_st = _team_standings.get(home_abbr, {})

        # Records: prefer ESPN scoreboard records (reliable), fall back to standings
        away_espn = _espn_team_records.get(away_abbr, {})
        home_espn = _espn_team_records.get(home_abbr, {})

        def _get_wl(espn_rec, st_rec):
            """Get wins/losses from best available source."""
            # ESPN scoreboard records are most reliable
            if espn_rec.get("wins"):
                return espn_rec["wins"], espn_rec["losses"]
            # Standings: only use if wins > 0 (catches the ESPN standings 0-X bug)
            sw, sl = st_rec.get("wins", 0), st_rec.get("losses", 0)
            if sw and sw > 0:
                return int(sw), int(sl)
            return 0, 0

        away_wins, away_losses = _get_wl(away_espn, away_st)
        home_wins, home_losses = _get_wl(home_espn, home_st)

        # Home/Road records from ESPN scoreboard or standings
        away_road = away_espn.get("road", "") or away_st.get("road", "")
        home_home = home_espn.get("home", "") or home_st.get("home", "")

        # Tip-off time
        tipoff = g.get("clock", "") or g.get("tip", "") or ""

        # Venue
        venue = g.get("venue", "") or ""

        # Broadcasters and officials
        broadcasters = g.get("broadcasters", [])
        officials = g.get("officials", [])

        # Date label for upcoming games
        date_label = g.get("label", "") or upcoming_label or ""

        # Position matchups
        matchups = _build_matchups(away_abbr, home_abbr)

        # Count matchup wins
        away_pos_wins = sum(1 for m in matchups if m["winner"] == "away")
        home_pos_wins = sum(1 for m in matchups if m["winner"] == "home")

        # Injuries
        away_injuries = _get_team_injuries(away_abbr)
        home_injuries = _get_team_injuries(home_abbr)

        # Hot hand
        hot_hand = _get_hot_hand(away_abbr, home_abbr)

        # === NEW DATA ===

        # Season series (H2H)
        season_series = _get_season_series(away_abbr, home_abbr)

        # Rest days / B2B
        gd_str = g.get("gameDate", "") or game_date
        away_rest = _get_rest_days(away_abbr, gd_str)
        home_rest = _get_rest_days(home_abbr, gd_str)

        # Team advanced stats
        away_adv = _team_adv_stats.get(away_abbr, {})
        home_adv = _team_adv_stats.get(home_abbr, {})

        # Clutch records
        away_clutch = _team_clutch_stats.get(away_abbr, {})
        home_clutch = _team_clutch_stats.get(home_abbr, {})

        # Rating trends → replaced with last 5
        away_last5 = _get_last5(away_abbr, gd_str)
        home_last5 = _get_last5(home_abbr, gd_str)

        # Key matchup: position with biggest GR differential
        key_mu = None
        if matchups:
            best_diff = 0
            for mu in matchups:
                diff = abs((mu.get("away", {}).get("rating") or 0) - (mu.get("home", {}).get("rating") or 0))
                if diff > best_diff:
                    best_diff = diff
                    key_mu = mu

        # Win probability
        pred_key = f"{away_abbr}@{home_abbr}"
        prediction = predictions.get(pred_key, {})

        # Top scorers per team (from starters + player stats)
        def _team_leaders(city):
            starters = _depth_starters.get(city, [])
            leaders = []
            for s in starters:
                ps = _player_full_stats.get(s["name"], {})
                if ps:
                    leaders.append({
                        "name": s["name"].split()[-1],  # Last name only
                        "pts": round(float(ps.get("PTS", 0) or 0), 1),
                        "reb": round(float(ps.get("REB", 0) or 0), 1),
                        "ast": round(float(ps.get("AST", 0) or 0), 1),
                    })
            leaders.sort(key=lambda x: x["pts"], reverse=True)
            return leaders[:3]

        away_leaders = _team_leaders(away_city)
        home_leaders = _team_leaders(home_city)

        screen = {
            "isGamePreview": True,
            "previewPage": 1,
            "title": f"{away_city} @ {home_city}",
            "source": date_label or "",
            "dateLabel": date_label,
            "tipoff": tipoff,
            "venue": venue,
            "broadcasters": broadcasters,
            "officials": officials,
            "away": {
                "city": away_city,
                "abbr": away_abbr,
                "logo": _team_logo_url(away_abbr),
                "wins": away_wins,
                "losses": away_losses,
                "confRank": away_st.get("confRank", 0),
                "conf": away_st.get("conf", ""),
                "ppg": round(float(away_st.get("ppg", 0) or 0), 1),
                "oppPpg": round(float(away_st.get("oppPpg", 0) or 0), 1),
                "diffPpg": round(float(away_st.get("diffPpg", 0) or 0), 1),
                "streak": away_st.get("streak", ""),
                "homeRec": away_espn.get("home", "") or away_st.get("home", ""),
                "roadRec": away_road,
                "injuries": away_injuries,
                "ortg": away_adv.get("ortg", 0),
                "drtg": away_adv.get("drtg", 0),
                "netRtg": away_adv.get("netRtg", 0),
                "pace": away_adv.get("pace", 0),
                "clutch": away_clutch.get("record", ""),
                "rest": away_rest,
                "last5": away_last5,
                "leaders": away_leaders,
            },
            "home": {
                "city": home_city,
                "abbr": home_abbr,
                "logo": _team_logo_url(home_abbr),
                "wins": home_wins,
                "losses": home_losses,
                "confRank": home_st.get("confRank", 0),
                "conf": home_st.get("conf", ""),
                "ppg": round(float(home_st.get("ppg", 0) or 0), 1),
                "oppPpg": round(float(home_st.get("oppPpg", 0) or 0), 1),
                "diffPpg": round(float(home_st.get("diffPpg", 0) or 0), 1),
                "streak": home_st.get("streak", ""),
                "homeRec": home_home,
                "roadRec": home_espn.get("road", "") or home_st.get("road", ""),
                "injuries": home_injuries,
                "ortg": home_adv.get("ortg", 0),
                "drtg": home_adv.get("drtg", 0),
                "netRtg": home_adv.get("netRtg", 0),
                "pace": home_adv.get("pace", 0),
                "clutch": home_clutch.get("record", ""),
                "rest": home_rest,
                "last5": home_last5,
                "leaders": home_leaders,
            },
            "matchups": matchups,
            "awayPosWins": away_pos_wins,
            "homePosWins": home_pos_wins,
            "hotHand": hot_hand,
            "seasonSeries": season_series,
            "prediction": prediction,
            "keyMatchup": key_mu,
        }
        # Emit 2 screens per game: page 1 = overview, page 2 = matchups
        screens.append(screen)
        screen2 = dict(screen)
        screen2["previewPage"] = 2
        screen2["source"] = f"{away_city} @ {home_city} · Matchups"
        screens.append(screen2)

    result = {"screens": screens}
    preview_cache["gp"] = result
    last_good_previews = result

    log.info(f"Game previews: {len(screens)} games")
    return result


@app.route("/api/game-previews")
def api_game_previews():
    """Return game preview screens."""
    data = fetch_game_previews()
    return jsonify(data)


# ═══════════════════════════════════════
# CAREER MILESTONES — LIVE FROM NBA API
# ═══════════════════════════════════════

# Category display labels
_CAT_LABELS = {"PTS": "Points", "REB": "Rebounds", "AST": "Assists",
               "STL": "Steals", "BLK": "Blocks", "FG3M": "3-Pointers Made"}

# All-time leaderboards — Google Sheet is primary source
_alltime_boards = {}  # cat -> [(name, career_total, is_active), ...] sorted desc
_alltime_cache = TTLCache(maxsize=1, ttl=3600)  # 1 hour
_ALLTIME_DISK_CACHE = os.path.join(os.path.dirname(__file__), "alltime_leaders_cache.json")
_ALLTIME_SHEET_ID = "1Q1DgQJipIFWjcnqIX3s4sHkLb2dhS_hjmFOP0mreqgA"
_ALLTIME_GID = "0"

def _save_boards_to_disk():
    """Persist all-time boards to JSON file so data survives restarts."""
    try:
        data = {}
        for cat, board in _alltime_boards.items():
            data[cat] = [[name, total, active] for name, total, active in board]
        with open(_ALLTIME_DISK_CACHE, "w") as f:
            json.dump(data, f)
        log.info(f"All-time leaders saved to disk: {_ALLTIME_DISK_CACHE}")
    except Exception as e:
        log.warning(f"Failed to save all-time leaders to disk: {e}")

def _load_boards_from_disk():
    """Load all-time boards from disk cache. Returns True if successful."""
    global _alltime_boards
    try:
        if not os.path.exists(_ALLTIME_DISK_CACHE):
            return False
        age_hours = (time.time() - os.path.getmtime(_ALLTIME_DISK_CACHE)) / 3600
        if age_hours > 168:  # 7 days
            log.info(f"Disk cache too old ({age_hours:.0f}h) — will refetch")
            return False
        with open(_ALLTIME_DISK_CACHE, "r") as f:
            data = json.load(f)
        for cat, entries in data.items():
            _alltime_boards[cat] = [(e[0], e[1], e[2]) for e in entries]
        total = sum(len(b) for b in _alltime_boards.values())
        log.info(f"Loaded all-time leaders from disk ({age_hours:.1f}h old): {total} entries across {len(_alltime_boards)} cats")
        return total > 500
    except Exception as e:
        log.warning(f"Failed to load disk cache: {e}")
        return False

def _fetch_alltime_leaders():
    """Fetch all-time leaders. Priority: 1) Google Sheet → 2) Disk cache → 3) Hardcoded fallback"""
    global _alltime_boards
    if "at" in _alltime_cache:
        return

    # Step 1: Try Google Sheet
    sheet_ok = _fetch_alltime_from_sheet()
    if sheet_ok:
        _save_boards_to_disk()
    else:
        # Step 2: Disk cache
        if _load_boards_from_disk():
            log.info("Using disk-cached all-time leaders (Google Sheet failed)")
        else:
            # Step 3: Hardcoded fallback
            _load_fallback_boards()
            log.warning(f"Using HARDCODED fallback — rankings WILL be inaccurate!")

    _alltime_cache["at"] = True
    for cat in ["PTS", "REB", "AST", "STL", "BLK"]:
        board = _alltime_boards.get(cat, [])
        if board:
            log.info(f"  All-time {cat}: {len(board)} entries, #1={board[0][0]} ({board[0][1]:,})")

def _fetch_alltime_from_sheet():
    """Fetch all-time leaders from the Milestones Google Sheet.
    The sheet has a detailed stats table with columns:
    #, PLAYER, GP, MIN, PTS, FGM, FGA, FG%, 3PM, 3PA, 3P%, FTM, FTA, FT%, OREB, DREB, REB, AST, STL, BLK, ...
    """
    global _alltime_boards
    csv_url = f"https://docs.google.com/spreadsheets/d/{_ALLTIME_SHEET_ID}/export?format=csv&gid={_ALLTIME_GID}"
    log.info(f"Fetching all-time leaders from Google Sheet...")

    try:
        resp = requests.get(csv_url, timeout=30)
        resp.raise_for_status()
        text = resp.text
        if not text or len(text) < 500:
            log.warning("Google Sheet returned empty/short response")
            return False

        reader = csv.reader(io.StringIO(text))
        all_rows = list(reader)
        log.info(f"  Sheet has {len(all_rows)} rows, first row has {len(all_rows[0]) if all_rows else 0} cols")

        # Find the DETAILED stats header row (the one in the right table with PTS, REB, AST, STL, BLK columns)
        # This table starts around column S (index 18) and has headers like: #, PLAYER, GP, MIN, PTS, ...
        header_row_idx = None
        col_offset = None  # column index where the right table starts

        for ri, row in enumerate(all_rows):
            # Look for a row that has "PLAYER" and "PTS" and "REB" and "AST" and "STL" and "BLK"
            # in the RIGHT table portion (column S onwards, index 18+)
            for ci in range(10, len(row)):
                if row[ci].strip().upper() == "PLAYER":
                    # Check if subsequent columns have stats headers
                    remaining = [c.strip().upper() for c in row[ci:ci+25]]
                    has_pts = "PTS" in remaining
                    has_reb = "REB" in remaining
                    has_ast = "AST" in remaining
                    has_stl = "STL" in remaining
                    has_blk = "BLK" in remaining
                    if has_pts and has_reb and has_ast and has_stl and has_blk:
                        header_row_idx = ri
                        col_offset = ci
                        log.info(f"  Found stats header at row {ri}, col offset {ci}")
                        break
            if header_row_idx is not None:
                break

        if header_row_idx is None:
            log.warning("Could not find stats header row in sheet")
            return False

        # Map column indices relative to col_offset
        header = [c.strip().upper() for c in all_rows[header_row_idx][col_offset:col_offset+25]]
        log.info(f"  Header cols: {header[:22]}")

        def find_col(name):
            for i, h in enumerate(header):
                if h == name:
                    return col_offset + i
            return None

        c_name = find_col("PLAYER")
        c_pts = find_col("PTS")
        c_reb = find_col("REB")
        c_ast = find_col("AST")
        c_stl = find_col("STL")
        c_blk = find_col("BLK")
        c_3pm = find_col("3PM") or find_col("3:00 PM") or find_col("3P")  # sheets may rename "3PM"

        if not all([c_name, c_pts, c_reb, c_ast, c_stl, c_blk]):
            log.warning(f"Missing required columns. Found: name={c_name} pts={c_pts} reb={c_reb} ast={c_ast} stl={c_stl} blk={c_blk}")
            return False

        # Also check for the "3:00 PM" alias that Google Sheets sometimes creates
        if c_3pm is None:
            for i, h in enumerate(header):
                if "3" in h and ("PM" in h or "P" in h.upper()):
                    c_3pm = col_offset + i
                    break

        log.info(f"  Column indices: PLAYER={c_name} PTS={c_pts} REB={c_reb} AST={c_ast} STL={c_stl} BLK={c_blk} 3PM={c_3pm}")

        # Parse player rows
        players = []
        for ri in range(header_row_idx + 1, len(all_rows)):
            row = all_rows[ri]
            if c_name >= len(row):
                continue
            name = row[c_name].strip()
            if not name or name.upper() in ("PLAYER", "TOTAL", "TEAM", ""):
                continue

            def parse_int(col_idx):
                if col_idx is None or col_idx >= len(row):
                    return 0
                val = row[col_idx].strip().replace(",", "").replace("-", "0")
                try:
                    return int(float(val))
                except (ValueError, TypeError):
                    return 0

            pts = parse_int(c_pts)
            reb = parse_int(c_reb)
            ast = parse_int(c_ast)
            stl = parse_int(c_stl)
            blk = parse_int(c_blk)
            tpm = parse_int(c_3pm) if c_3pm else 0

            if pts == 0 and reb == 0 and ast == 0:
                continue  # skip empty rows

            # Detect active status by checking if player has current season stats
            norm = _normalize_name(name)
            is_active = norm in _player_full_stats or name in _player_full_stats

            players.append({
                "name": name, "pts": pts, "reb": reb, "ast": ast,
                "stl": stl, "blk": blk, "tpm": tpm, "active": is_active,
            })

        if len(players) < 100:
            log.warning(f"Only found {len(players)} players in sheet (expected 200+)")
            return False

        log.info(f"  Parsed {len(players)} players from Google Sheet")

        # Build boards sorted by each stat
        for cat, key in [("PTS", "pts"), ("REB", "reb"), ("AST", "ast"), ("STL", "stl"), ("BLK", "blk"), ("FG3M", "tpm")]:
            board = [(p["name"], p[key], p["active"]) for p in players if p[key] > 0]
            board.sort(key=lambda x: x[1], reverse=True)
            _alltime_boards[cat] = board

        total = sum(len(b) for b in _alltime_boards.values())
        log.info(f"  Google Sheet loaded: {total} total entries across {len(_alltime_boards)} categories")
        return True

    except Exception as e:
        log.warning(f"Failed to fetch all-time leaders from Google Sheet: {e}")
        return False


def _load_fallback_boards():
    """Load comprehensive hardcoded all-time leaders as baseline."""
    global _alltime_boards
    log.info("Loading hardcoded all-time leaders fallback (source: landofbasketball.com / Wikipedia, Feb 2026)")
    _alltime_boards["PTS"] = [
        ("LeBron James", 42975, True), ("Kareem Abdul-Jabbar", 38387, False), ("Karl Malone", 36928, False),
        ("Kobe Bryant", 33643, False), ("Michael Jordan", 32292, False), ("Kevin Durant", 31862, True),
        ("Dirk Nowitzki", 31560, False), ("Wilt Chamberlain", 31419, False), ("James Harden", 28863, True),
        ("Shaquille O'Neal", 28596, False), ("Carmelo Anthony", 28289, False), ("Moses Malone", 27409, False),
        ("Elvin Hayes", 27313, False), ("Russell Westbrook", 27001, True), ("Hakeem Olajuwon", 26946, False),
        ("Oscar Robertson", 26710, False), ("Dominique Wilkins", 26668, False), ("Tim Duncan", 26496, False),
        ("Stephen Curry", 26447, True), ("Paul Pierce", 26397, False), ("John Havlicek", 26395, False),
        ("DeMar DeRozan", 26339, True), ("Kevin Garnett", 26071, False), ("Vince Carter", 25728, False),
        ("Alex English", 25613, False), ("Reggie Miller", 25279, False), ("Jerry West", 25192, False),
        ("Patrick Ewing", 24815, False), ("Ray Allen", 24505, False), ("Allen Iverson", 24368, False),
        ("Charles Barkley", 23757, False), ("Robert Parish", 23334, False), ("Adrian Dantley", 23177, False),
        ("Dwyane Wade", 23165, False), ("Elgin Baylor", 23149, False), ("Chris Paul", 23058, True),
        ("Damian Lillard", 22598, True), ("Clyde Drexler", 22195, False), ("Gary Payton", 21813, False),
        ("Larry Bird", 21791, False), ("Hal Greer", 21586, False), ("Giannis Antetokounmpo", 21377, True),
        ("Walt Bellamy", 20941, False), ("Pau Gasol", 20894, False), ("Bob Pettit", 20880, False),
        ("David Robinson", 20790, False), ("George Gervin", 20708, False), ("LaMarcus Aldridge", 20558, False),
        ("Mitch Richmond", 20497, False), ("Joe Johnson", 20407, False),
        ("Tom Chambers", 20049, False), ("Antawn Jamison", 20042, False), ("Kyrie Irving", 19600, True),
        ("Paul George", 19200, True), ("Tony Parker", 19473, False), ("Jamal Crawford", 19419, False),
        ("Nikola Jokic", 18400, True), ("Jimmy Butler", 17800, True), ("Bradley Beal", 17200, True),
        ("Devin Booker", 16800, True), ("Jayson Tatum", 16500, True), ("Anthony Davis", 16400, True),
        ("Donovan Mitchell", 15000, True), ("Karl-Anthony Towns", 15890, True),
        ("Zach LaVine", 14600, True), ("Jrue Holiday", 14800, True), ("Khris Middleton", 14400, True),
        ("Brook Lopez", 14300, True), ("Al Horford", 14960, True), ("Mike Conley", 14200, True),
        ("Shai Gilgeous-Alexander", 13500, True), ("De'Aaron Fox", 12800, True),
        ("Trae Young", 12400, True), ("Luka Doncic", 12300, True), ("Domantas Sabonis", 12100, True),
        ("Pascal Siakam", 12000, True), ("Julius Randle", 11800, True), ("Bam Adebayo", 11500, True),
        ("Andrew Wiggins", 11400, True), ("Fred VanVleet", 9600, True),
        ("Anthony Edwards", 9800, True), ("Tyrese Maxey", 8500, True),
    ]
    _alltime_boards["AST"] = [
        ("John Stockton", 15806, False), ("Jason Kidd", 12091, False), ("Chris Paul", 11700, True),
        ("LeBron James", 11500, True), ("Steve Nash", 10335, False), ("Mark Jackson", 10334, False),
        ("Magic Johnson", 10141, False), ("Russell Westbrook", 10100, True), ("Oscar Robertson", 9887, False),
        ("Isiah Thomas", 9061, False), ("Gary Payton", 8966, False), ("Andre Miller", 8524, False),
        ("James Harden", 8200, True), ("Rod Strickland", 7987, False), ("Rajon Rondo", 7584, False),
        ("Maurice Cheeks", 7392, False), ("Lenny Wilkens", 7211, False), ("Tim Hardaway", 7095, False),
        ("Tony Parker", 7036, False), ("Damian Lillard", 7200, True), ("Bob Cousy", 6955, False),
        ("Guy Rodgers", 6917, False), ("Muggsy Bogues", 6726, False), ("Kevin Johnson", 6711, False),
        ("Kyle Lowry", 6500, True), ("Reggie Theus", 6453, False), ("John Lucas", 6454, False),
        ("Norm Nixon", 6386, False), ("Clyde Drexler", 6125, False), ("Scottie Pippen", 6135, False),
        ("Stephen Curry", 6000, True), ("DeMar DeRozan", 5700, True), ("Derek Harper", 5765, False),
        ("Deron Williams", 5765, False), ("Dwyane Wade", 5701, False), ("Nikola Jokic", 6600, True),
        ("Chauncey Billups", 5633, False), ("Trae Young", 5600, True), ("John Wall", 5282, False),
        ("Jrue Holiday", 5400, True), ("Mike Conley", 5600, True),
        ("Baron Davis", 5044, False), ("Luka Doncic", 4800, True), ("Kyrie Irving", 5000, True),
        ("Tyrese Haliburton", 3700, True), ("De'Aaron Fox", 4200, True),
    ]
    _alltime_boards["REB"] = [
        ("Wilt Chamberlain", 23924, False), ("Bill Russell", 21620, False), ("Kareem Abdul-Jabbar", 17440, False),
        ("Elvin Hayes", 16279, False), ("Moses Malone", 16212, False), ("Tim Duncan", 15091, False),
        ("Karl Malone", 14968, False), ("Robert Parish", 14715, False), ("Kevin Garnett", 14662, False),
        ("Dwight Howard", 14627, False), ("Nate Thurmond", 14464, False), ("Walt Bellamy", 14241, False),
        ("Wes Unseld", 13769, False), ("Hakeem Olajuwon", 13748, False), ("Shaquille O'Neal", 13099, False),
        ("Buck Williams", 13017, False), ("Jerry Lucas", 12942, False), ("Bob Pettit", 12849, False),
        ("Charles Barkley", 12546, False), ("Dikembe Mutombo", 12359, False), ("Paul Silas", 12357, False),
        ("Charles Oakley", 12205, False), ("Dennis Rodman", 11954, False), ("Kevin Willis", 11901, False),
        ("LeBron James", 11757, True), ("Patrick Ewing", 11607, False), ("Dirk Nowitzki", 11489, False),
        ("Elgin Baylor", 11463, False), ("Pau Gasol", 11305, False), ("Dolph Schayes", 11256, False),
        ("Andre Drummond", 11169, True), ("Bill Bridges", 11054, False), ("Jack Sikma", 10816, False),
        ("DeAndre Jordan", 10795, True), ("David Robinson", 10497, False), ("Ben Wallace", 10482, False),
        ("Tyson Chandler", 10467, False), ("Dave Cowens", 10444, False), ("Bill Laimbeer", 10400, False),
        ("Otis Thorpe", 10370, False), ("Nikola Vucevic", 10368, True), ("Zach Randolph", 10208, False),
        ("Shawn Marion", 10101, False), ("Red Kerr", 10092, False), ("Rudy Gobert", 9927, True),
        ("Bob Lanier", 9698, False), ("Sam Lacey", 9687, False), ("Dave DeBusschere", 9618, False),
        ("Kevin Love", 9553, True), ("Marcus Camby", 9513, False),
        ("Al Horford", 9200, True), ("Russell Westbrook", 8700, True), ("Domantas Sabonis", 8000, True),
        ("Nikola Jokic", 8100, True), ("Giannis Antetokounmpo", 8400, True), ("Anthony Davis", 7500, True),
        ("Clint Capela", 6800, True), ("Jonas Valanciunas", 7200, True), ("Karl-Anthony Towns", 7200, True),
        ("Bam Adebayo", 6400, True), ("Carmelo Anthony", 6924, False),
    ]
    _alltime_boards["STL"] = [
        ("John Stockton", 3265, False), ("Chris Paul", 2726, True), ("Jason Kidd", 2684, False),
        ("Michael Jordan", 2514, False), ("Gary Payton", 2445, False), ("LeBron James", 2346, True),
        ("Maurice Cheeks", 2310, False), ("Scottie Pippen", 2307, False), ("Clyde Drexler", 2207, False),
        ("Hakeem Olajuwon", 2162, False), ("Alvin Robertson", 2112, False), ("Karl Malone", 2085, False),
        ("Mookie Blaylock", 2075, False), ("Allen Iverson", 1983, False), ("Russell Westbrook", 1969, True),
        ("Derek Harper", 1957, False), ("Kobe Bryant", 1944, False), ("Isiah Thomas", 1861, False),
        ("Kevin Garnett", 1859, False), ("Andre Iguodala", 1765, False), ("Shawn Marion", 1759, False),
        ("Paul Pierce", 1752, False), ("James Harden", 1727, True), ("Magic Johnson", 1724, False),
        ("Metta World Peace", 1721, False), ("Ron Harper", 1716, False), ("Fat Lever", 1666, False),
        ("Charles Barkley", 1648, False), ("Gus Williams", 1638, False), ("Trevor Ariza", 1628, False),
        ("Hersey Hawkins", 1622, False), ("Eddie Jones", 1620, False), ("Dwyane Wade", 1620, False),
        ("Rod Strickland", 1616, False), ("Mike Conley", 1614, True), ("Thaddeus Young", 1612, False),
        ("Mark Jackson", 1608, False), ("Jason Terry", 1603, False), ("Terry Porter", 1583, False),
        ("Stephen Curry", 1573, True), ("Doc Rivers", 1563, False), ("Larry Bird", 1556, False),
        ("Doug Christie", 1555, False), ("Andre Miller", 1546, False), ("Nate McMillan", 1544, False),
        ("Paul George", 1539, True), ("Jeff Hornacek", 1535, False), ("John Havlicek", 1512, False),
        ("Rick Barry", 1504, False), ("Rajon Rondo", 1494, False), ("Tim Duncan", 1488, False),
        ("Tony Parker", 1474, False), ("Kevin Durant", 1460, True), ("Reggie Miller", 1451, False),
        ("Penny Hardaway", 1448, False), ("Tony Allen", 1441, False), ("Grant Hill", 1436, False),
        ("Vince Carter", 1423, False), ("Tim Hardaway", 1428, False), ("Kyle Lowry", 1415, True),
        ("Dirk Nowitzki", 1384, False), ("Chauncey Billups", 1372, False), ("Eric Snow", 1360, False),
        ("Robert Horry", 1359, False), ("DeMar DeRozan", 1350, True), ("Jimmy Butler", 1340, True),
        ("Jrue Holiday", 1330, True), ("Marcus Smart", 1200, True), ("Draymond Green", 1180, True),
        ("Al Horford", 1160, True), ("Nikola Jokic", 1050, True), ("Giannis Antetokounmpo", 1000, True),
    ]
    _alltime_boards["BLK"] = [
        ("Hakeem Olajuwon", 3830, False), ("Dikembe Mutombo", 3289, False),
        ("Kareem Abdul-Jabbar", 3189, False), ("Mark Eaton", 3064, False), ("Tim Duncan", 3020, False),
        ("David Robinson", 2954, False), ("Patrick Ewing", 2894, False), ("Shaquille O'Neal", 2732, False),
        ("Tree Rollins", 2542, False), ("Robert Parish", 2361, False),
        ("Alonzo Mourning", 2356, False), ("Marcus Camby", 2331, False), ("Dwight Howard", 2228, False),
        ("Ben Wallace", 2137, False), ("Shawn Bradley", 2119, False), ("Manute Bol", 2086, False),
        ("George T. Johnson", 2082, False), ("Brook Lopez", 2071, True), ("Kevin Garnett", 2037, False),
        ("Larry Nance", 2027, False), ("Theo Ratliff", 1968, False), ("Pau Gasol", 1941, False),
        ("Elton Brand", 1828, False), ("Anthony Davis", 1821, True), ("Jermaine O'Neal", 1820, False),
        ("Elvin Hayes", 1771, False), ("Serge Ibaka", 1759, False), ("Artis Gilmore", 1747, False),
        ("Rudy Gobert", 1743, True), ("Moses Malone", 1733, False), ("Josh Smith", 1713, False),
        ("Kevin McHale", 1690, False), ("Vlade Divac", 1631, False), ("Herb Williams", 1605, False),
        ("Elden Campbell", 1602, False), ("Benoit Benjamin", 1581, False), ("Rasheed Wallace", 1530, False),
        ("Clifford Robinson", 1517, False), ("Myles Turner", 1510, True), ("LaMarcus Aldridge", 1500, False),
        ("DeAndre Jordan", 1498, False), ("Clint Capela", 1460, True), ("Andrew Bogut", 1455, False),
        ("Samuel Dalembert", 1453, False), ("Giannis Antetokounmpo", 1400, True),
        ("Jaren Jackson Jr.", 1050, True), ("Bam Adebayo", 900, True), ("Al Horford", 1350, True),
        ("LeBron James", 1150, True), ("Joel Embiid", 950, True), ("Victor Wembanyama", 450, True),
        ("Robert Williams III", 600, True), ("Kristaps Porzingis", 850, True), ("Nikola Vucevic", 850, True),
        ("Jakob Poeltl", 800, True), ("Walker Kessler", 500, True), ("Ivica Zubac", 700, True),
    ]
    _alltime_boards["FG3M"] = [
        ("Stephen Curry", 4233, True), ("Ray Allen", 2973, False), ("James Harden", 3318, True),
        ("Damian Lillard", 2804, True), ("Reggie Miller", 2560, False), ("LeBron James", 2610, True),
        ("Kyle Korver", 2450, False), ("Kevin Durant", 2307, True), ("Vince Carter", 2290, False),
        ("Jason Terry", 2282, False), ("Jamal Crawford", 2221, False), ("Paul Pierce", 2143, False),
        ("Jason Kidd", 1988, False), ("Klay Thompson", 2100, True), ("Joe Johnson", 1978, False),
        ("Chris Paul", 1870, True), ("Kyle Lowry", 2060, True), ("J.J. Redick", 1950, False),
        ("Wesley Matthews", 1890, False), ("Chauncey Billups", 1830, False),
        ("Rashard Lewis", 1787, False), ("Steve Nash", 1685, False), ("Peja Stojakovic", 1760, False),
        ("Eric Gordon", 1800, True), ("Dale Ellis", 1719, False), ("Russell Westbrook", 1500, True),
        ("Glen Rice", 1559, False), ("Nick Van Exel", 1528, False), ("Tim Hardaway", 1542, False),
        ("Danny Green", 1530, False), ("Buddy Hield", 1650, True), ("Donovan Mitchell", 1600, True),
        ("CJ McCollum", 1550, True), ("Fred VanVleet", 1450, True), ("Jayson Tatum", 1500, True),
        ("Manu Ginobili", 1495, False), ("DeMar DeRozan", 656, True), ("Trae Young", 1350, True),
        ("Luka Doncic", 1300, True), ("Devin Booker", 1400, True),
    ]
    # Sort all fallback boards descending
    for cat in _alltime_boards:
        _alltime_boards[cat].sort(key=lambda x: x[1], reverse=True)
    log.info(f"Hardcoded fallback loaded: {list(_alltime_boards.keys())}")


milestone_cache = TTLCache(maxsize=1, ttl=1800)  # 30 min
last_good_milestones = {"screens": []}


def _calculate_milestones():
    """Calculate who recently passed or is about to pass all-time leaders.
    - Recently Passed: within 3 games, Top 200, 5 categories (no 3PM)
    - Approaching: 5 separate screens (PTS, REB, AST, STL, BLK), within 3 games, Top 200
    """
    global last_good_milestones
    if "ms" in milestone_cache:
        return milestone_cache["ms"]

    _fetch_alltime_leaders()

    if not _alltime_boards:
        log.warning("Milestones: No all-time boards available")
        return last_good_milestones

    log.info(f"Milestones: _alltime_boards has {len(_alltime_boards)} categories, _player_full_stats has {len(_player_full_stats)} players")

    if not _player_full_stats:
        log.warning("Milestones: _player_full_stats empty — skipping (will retry on next call)")
        return last_good_milestones

    MILESTONE_CATS = ["PTS", "REB", "AST", "STL", "BLK"]  # No FG3M
    TOP_N = 200
    PASSED_WINDOW = 10   # recently passed: scan within 10 games of production
    # Category-specific approach windows (wider for low per-game stats)
    APPROACH_WINDOWS = {
        "PTS": 10,   # ~25ppg × 10 = 250
        "REB": 15,   # ~8rpg × 15 = 120
        "AST": 15,   # ~6apg × 15 = 90
        "STL": 30,   # ~1spg × 30 = 30
        "BLK": 30,   # ~1bpg × 30 = 30
    }

    recently_passed = []
    upcoming_by_cat = {cat: [] for cat in MILESTONE_CATS}

    for cat in MILESTONE_CATS:
        board = _alltime_boards.get(cat, [])
        if not board:
            continue
        cat_label = _CAT_LABELS.get(cat, cat)
        # Only top 200
        board_200 = board[:TOP_N]
        matched = 0

        for rank_idx, (name, career_total, is_active) in enumerate(board_200):
            my_rank = rank_idx + 1

            norm_name = _normalize_name(name)
            season = _player_full_stats.get(norm_name, {}) or _player_full_stats.get(name, {})
            gp = int(season.get("GP", 0) or 0)
            per_game = float(season.get(cat, 0) or 0)

            if gp == 0 or per_game <= 0:
                continue
            matched += 1

            three_game_prod = per_game * PASSED_WINDOW

            # Recently passed: scan all positions behind, break when gap too large
            for behind_idx in range(rank_idx + 1, len(board_200)):
                behind_name, behind_total, _ = board_200[behind_idx]
                behind_rank = behind_idx + 1
                gap = career_total - behind_total
                if gap > three_game_prod:
                    break  # all further players will have bigger gaps
                if gap > 0:
                    recently_passed.append({
                        "name": name, "cat": cat, "catLabel": cat_label,
                        "current": career_total, "passedName": behind_name,
                        "passedTotal": behind_total, "myRank": my_rank,
                        "theirRank": behind_rank, "perGame": round(per_game, 1),
                        "gap": gap, "type": "passed",
                    })

            # Approaching: scan all positions ahead, break when gap too large
            approach_prod = per_game * APPROACH_WINDOWS.get(cat, 15)
            for ahead_idx in range(rank_idx - 1, -1, -1):
                ahead_name, ahead_total, _ = board_200[ahead_idx]
                ahead_rank = ahead_idx + 1
                gap = ahead_total - career_total
                if gap > approach_prod:
                    break  # all further players will have bigger gaps
                if gap > 0:
                    games_needed = max(1, round(gap / per_game))
                    upcoming_by_cat[cat].append({
                        "name": name, "cat": cat, "catLabel": cat_label,
                        "current": career_total, "targetName": ahead_name,
                        "targetTotal": ahead_total, "remaining": gap,
                        "myRank": my_rank, "theirRank": ahead_rank,
                        "perGame": round(per_game, 1), "gamesNeeded": games_needed,
                        "type": "upcoming",
                    })

        log.info(f"  Milestones {cat}: {matched} active players matched of {len(board_200)} in board (approach window={APPROACH_WINDOWS.get(cat, 15)} games)")

    # Deduplicate
    seen = set()
    def dedup(items):
        result = []
        for m in items:
            key = (m["name"], m["cat"], m.get("passedName", m.get("targetName", "")))
            if key not in seen:
                seen.add(key)
                result.append(m)
        return result

    recently_passed = dedup(recently_passed)
    for cat in MILESTONE_CATS:
        upcoming_by_cat[cat] = dedup(upcoming_by_cat[cat])

    # Sort: most recent passings first (fewest games ago = gap / perGame)
    recently_passed.sort(key=lambda x: (x["gap"] / x["perGame"] if x["perGame"] > 0 else 999, x["myRank"]))
    for cat in MILESTONE_CATS:
        upcoming_by_cat[cat].sort(key=lambda x: x["gamesNeeded"])

    log.info(f"Milestones: {len(recently_passed)} recently passed")
    for cat in MILESTONE_CATS:
        log.info(f"  Approaching {cat}: {len(upcoming_by_cat[cat])} items")

    # Build screens
    source_note = ""
    screens = []
    per_screen = 8

    # Screen(s) for recently passed (all 5 cats mixed, max 2 screens)
    if recently_passed:
        capped = recently_passed[:8]  # max 8 items per screen
        screens.append({
            "isMilestone": True,
            "title": "Career Milestones — Recently Passed",
            "source": source_note,
            "items": capped,
        })

    # 5 approaching screens — one per category (max 8 items each)
    cat_titles = {"PTS": "Scoring", "REB": "Rebounds", "AST": "Assists", "STL": "Steals", "BLK": "Blocks"}
    for cat in MILESTONE_CATS:
        items = upcoming_by_cat[cat][:per_screen]  # cap at 8
        if items:
            screens.append({
                "isMilestone": True,
                "title": f"Approaching — All-Time {cat_titles[cat]}",
                "source": source_note,
                "items": items,
            })

    result = {"screens": screens}
    if screens:
        milestone_cache["ms"] = result
        last_good_milestones = result
    else:
        log.warning("Milestones: 0 screens produced — NOT caching (will retry next call)")
    total_approaching = sum(len(upcoming_by_cat[c]) for c in MILESTONE_CATS)
    log.info(f"Milestones: {len(recently_passed)} recently passed, {total_approaching} approaching, {len(screens)} screens")
    return result if screens else last_good_milestones


@app.route("/api/milestones")
def api_milestones():
    """Return career milestone screens."""
    data = _calculate_milestones()
    return jsonify(data)


@app.route("/api/refresh-alltime")
def api_refresh_alltime():
    """Force-refresh all-time leaders from Google Sheet."""
    global _alltime_boards
    _alltime_cache.clear()
    milestone_cache.clear()
    old_count = sum(len(b) for b in _alltime_boards.values())
    _alltime_boards = {}
    _fetch_alltime_leaders()
    new_count = sum(len(b) for b in _alltime_boards.values())
    return jsonify({"status": "ok", "source": "google_sheet", "old_entries": old_count, "new_entries": new_count,
                    "categories": {cat: len(b) for cat, b in _alltime_boards.items()}})


def _background_alltime_retry():
    """Background thread: if initial API fetch failed, retry every 5 minutes."""
    time.sleep(60)  # Wait 1 min after startup
    for attempt in range(12):  # Try for up to 1 hour
        total = sum(len(b) for b in _alltime_boards.values())
        if total > 500:
            log.info(f"Background alltime retry: data looks good ({total} entries), stopping")
            return
        log.info(f"Background alltime retry attempt {attempt+1}/12...")
        _alltime_cache.clear()
        _alltime_boards.clear()
        _fetch_alltime_leaders()
        total = sum(len(b) for b in _alltime_boards.values())
        if total > 500:
            milestone_cache.clear()  # Force milestones recalculation
            log.info(f"Background alltime retry SUCCESS! {total} entries")
            return
        time.sleep(300)  # Wait 5 min
    log.warning("Background alltime retry exhausted all attempts")


# ═══════════════════════════════════════
# DEPTH CHARTS (Google Sheets)
# ═══════════════════════════════════════

_DEPTH_SHEET_ID = "14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY"
_DEPTH_GID = "24771201"
_DEPTH_JSON_URL = "https://aderoa.github.io/DepthCharts/order.json"

depth_cache = TTLCache(maxsize=1, ttl=1800)  # 30 min
last_good_depth = []

_POSITIONS = ["PG", "SG", "SF", "PF", "C"]
_MAX_LEVELS = 10  # effectively unlimited
_TEAMS_PER_SCREEN = 2

# Abbreviation → full team name
_ABBR_TO_FULL = {
    "ATL": "Atlanta Hawks", "BOS": "Boston Celtics", "BKN": "Brooklyn Nets",
    "CHA": "Charlotte Hornets", "CHI": "Chicago Bulls", "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks", "DEN": "Denver Nuggets", "DET": "Detroit Pistons",
    "GSW": "Golden State Warriors", "HOU": "Houston Rockets", "IND": "Indiana Pacers",
    "LAC": "LA Clippers", "LAL": "Los Angeles Lakers", "MEM": "Memphis Grizzlies",
    "MIA": "Miami Heat", "MIL": "Milwaukee Bucks", "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans", "NYK": "New York Knicks", "OKC": "Oklahoma City Thunder",
    "ORL": "Orlando Magic", "PHI": "Philadelphia 76ers", "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers", "SAC": "Sacramento Kings", "SAS": "San Antonio Spurs",
    "TOR": "Toronto Raptors", "UTA": "Utah Jazz", "WAS": "Washington Wizards",
}

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
    """Fetch depth chart data from GitHub JSON, packed 2 teams per screen."""
    global last_good_depth

    if "depth" in depth_cache:
        return depth_cache["depth"]

    log.info("Fetching depth charts from GitHub JSON...")

    try:
        resp = requests.get(_DEPTH_JSON_URL, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"Depth charts JSON fetch failed: {e}")
        return last_good_depth

    if not data or len(data) < 20:
        log.warning(f"Depth charts JSON too small ({len(data) if data else 0} teams)")
        return last_good_depth

    all_teams = []

    for abbr, positions in data.items():
        team_name = _ABBR_TO_FULL.get(abbr, abbr)

        # For each position, split players into active vs out (by __SEPARATOR__)
        active_by_pos = {}  # pos -> [active players]
        out_by_pos = {}     # pos -> [out players]

        for pos in _POSITIONS:
            players = positions.get(pos, [])
            active = []
            out = []
            past_sep = False
            for p in players:
                if p == "__SEPARATOR__":
                    past_sep = True
                    continue
                if p == "__SPACER__":
                    continue
                if past_sep:
                    out.append(p)
                else:
                    active.append(p)
            active_by_pos[pos] = active
            out_by_pos[pos] = out

        # Build levels by row depth across positions
        # Level 0 = starters (first active player per position)
        # Level 1 = bench (second active player per position)
        # etc.
        max_active = max(len(v) for v in active_by_pos.values()) if active_by_pos else 0
        levels = []

        for depth in range(max_active):
            label = "Starters" if depth == 0 else "Bench" if depth == 1 else "Reserve"
            level = {"label": label, "players": []}
            for pos in _POSITIONS:
                actives = active_by_pos.get(pos, [])
                if depth < len(actives):
                    raw_name = actives[depth]
                    full_name = resolve_player_name(raw_name)
                    level["players"].append({
                        "pos": pos,
                        "name": full_name,
                        "salary": _player_salary_map.get(full_name, _player_salary_map.get(raw_name, "")),
                        "country": _PLAYER_COUNTRY.get(full_name, _PLAYER_COUNTRY.get(raw_name, "")),
                        "questionable": full_name in _questionable_players,
                    })
            if level["players"]:
                levels.append(level)

        # Out level — players after __SEPARATOR__
        out_players = []
        for pos in _POSITIONS:
            for raw_name in out_by_pos.get(pos, []):
                full_name = resolve_player_name(raw_name)
                out_players.append({
                    "pos": pos,
                    "name": full_name,
                    "salary": _player_salary_map.get(full_name, _player_salary_map.get(raw_name, "")),
                    "country": _PLAYER_COUNTRY.get(full_name, _PLAYER_COUNTRY.get(raw_name, "")),
                    "questionable": full_name in _questionable_players,
                })
        if out_players:
            levels.append({"label": "Out", "players": out_players})

        if levels:
            all_teams.append({"name": team_name, "levels": levels})

    # Sort teams alphabetically
    all_teams.sort(key=lambda t: t["name"])

    # Build player→team cross-reference
    _player_team_map.clear()
    _player_position_map.clear()
    _depth_starters.clear()
    for t in all_teams:
        for level in t["levels"]:
            for p in level["players"]:
                _player_team_map[p["name"]] = t["name"]
                if p["name"] not in _player_position_map:
                    _player_position_map[p["name"]] = p["pos"]
        # Populate starters (first level)
        if t["levels"]:
            starters = t["levels"][0]["players"]
            _depth_starters[t["name"]] = [{"name": p["name"], "pos": p["pos"]} for p in starters]

    log.info(f"  Position map: {len(_player_position_map)} players (starters: {sum(len(v) for v in _depth_starters.values())})")
    team_names = [t["name"] for t in all_teams]
    log.info(f"Depth charts: {len(all_teams)} teams identified: {team_names}")

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
    """Return depth chart screens (multiple teams per screen).
    Overlays questionable flags from current injury data."""
    data = fetch_depth()
    # Dynamically apply questionable flags from latest injury data
    for screen in data:
        for team in screen.get("teams", []):
            for level in team.get("levels", []):
                for p in level.get("players", []):
                    p["questionable"] = p["name"] in _questionable_players
    return jsonify({"screens": data, "count": len(data)})


@app.route("/")
def serve_index():
    """Serve the broadcast overlay page."""
    return send_from_directory(PROJECT_ROOT, "index.html")


@app.route("/embed")
def serve_embed():
    """Serve the embeddable widget for HoopsHype.com."""
    resp = send_from_directory(PROJECT_ROOT, "embed.html")
    resp.headers["X-Frame-Options"] = "ALLOWALL"
    resp.headers["Content-Security-Policy"] = "frame-ancestors *"
    return resp


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
        fetch_depth()  # Must be FIRST — populates _player_team_map for everything else
    except Exception as e:
        log.warning(f"Pre-warm depth charts failed: {e}")

    # Invalidate ratings cache so it rebuilds with team names from depth charts
    ratings_cache.clear()

    try:
        fetch_advanced_stats()  # Must be before comparisons — populates defense/clutch/hustle
    except Exception as e:
        log.warning(f"Pre-warm advanced stats failed: {e}")

    try:
        fetch_counting_stats()  # Must be before comparisons — populates counting stats
    except Exception as e:
        log.warning(f"Pre-warm counting stats failed: {e}")

    try:
        fetch_ratings()  # Needs _player_team_map for team names
    except Exception as e:
        log.warning(f"Pre-warm ratings failed: {e}")

    try:
        fetch_injuries()  # Needs _player_team_map from depth charts
    except Exception as e:
        log.warning(f"Pre-warm injuries failed: {e}")

    try:
        fetch_team_ratings()  # Needs _player_team_map
    except Exception as e:
        log.warning(f"Pre-warm team ratings failed: {e}")

    try:
        fetch_hist_salaries()
    except Exception as e:
        log.warning(f"Pre-warm historical salaries failed: {e}")

    try:
        fetch_draft_classes()
    except Exception as e:
        log.warning(f"Pre-warm draft classes failed: {e}")

    try:
        fetch_transactions()
    except Exception as e:
        log.warning(f"Pre-warm transactions failed: {e}")

    try:
        fetch_value_rankings()
    except Exception as e:
        log.warning(f"Pre-warm value rankings failed: {e}")

    # Comparisons MUST come after: depth, advanced_stats, counting_stats, ratings, team_ratings
    comparisons_cache.clear()
    try:
        fetch_comparisons()
    except Exception as e:
        log.warning(f"Pre-warm comparisons failed: {e}")

    try:
        fetch_standings()
    except Exception as e:
        log.warning(f"Pre-warm standings failed: {e}")

    try:
        fetch_game_previews()
    except Exception as e:
        log.warning(f"Pre-warm game previews failed: {e}")

    try:
        _calculate_milestones()
    except Exception as e:
        log.warning(f"Pre-warm milestones failed: {e}")

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
        threading.Thread(target=_background_alltime_retry, daemon=True).start()

    app.run(
        host=config.SERVER_HOST,
        port=config.SERVER_PORT,
        debug=config.DEBUG,
        threaded=True,  # handle concurrent requests (Bluesky fetch blocks for 30s+)
    )
