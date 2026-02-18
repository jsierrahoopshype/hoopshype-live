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
import random
import re
import threading
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
                time_display = f"{int(diff_mins)}m" if diff_mins < 60 else f"{int(diff_mins // 60)}h"

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
        players = players[:20]
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
_HIST_TOP_N = 20

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
        team = row[0].strip()
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

_STATS_TOP_N = 20
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
                "team": row[team_i],
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
        stat_keys = ["PTS", "REB", "AST", "STL", "BLK", "TOV", "FG_PCT", "FG3_PCT", "FT_PCT"]
        for row in rows_avg:
            name = _normalize_name(row[player_i])
            d = {"GP": row[gp_i], "TEAM": row[team_i]}
            for sk in stat_keys:
                si = col_map_avg.get(sk)
                if si is not None:
                    d[sk] = row[si]
            _player_full_stats[name] = d
        log.info(f"  Full stats cache: {len(_player_full_stats)} players")
        # Diagnostic: check LeBron name variants
        lebron_keys = [k for k in _player_full_stats if 'james' in k.lower() or 'lebron' in k.lower()]
        log.info(f"  LeBron variants in stats: {lebron_keys}")

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
                    "team": row[team_i],
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
            "ratingDisplay": f"{rating:.2f}",
            "salary": salary, "salaryCompact": sal_compact,
            "value": value, "games": gp, "pts": pts,
            "country": country,
        })

    if not combined:
        log.warning("Value: no players with salary + rating + GP found")
        return last_good_value

    # Screen 1: Best Value — highest rating/$1M (all players min $500K)
    best = [dict(p) for p in sorted(combined, key=lambda p: p["value"], reverse=True)[:20]]
    for i, p in enumerate(best):
        p["rank"] = i + 1

    # Screen 2: Worst Value — lowest rating/$1M among $10M+ earners
    big_earners = [p for p in combined if p["salary"] >= 10_000_000]
    worst = [dict(p) for p in sorted(big_earners, key=lambda p: p["value"])[:20]]
    for i, p in enumerate(worst):
        p["rank"] = i + 1

    # Screen 3: Lowest Rated $10M+ Players
    overpaid = [dict(p) for p in sorted(big_earners, key=lambda p: p["rating"])[:20]]
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
    # 2025 All-Stars
    "Giannis Antetokounmpo", "Jayson Tatum", "Karl-Anthony Towns", "Donovan Mitchell",
    "Jalen Brunson", "LaMelo Ball", "Cade Cunningham", "Jaylen Brown",
    "Tyler Herro", "Evan Mobley", "Scottie Barnes", "Damian Lillard",
    "Nikola Jokic", "Shai Gilgeous-Alexander", "LeBron James", "Kevin Durant",
    "Stephen Curry", "Victor Wembanyama", "Anthony Edwards", "James Harden",
    "De'Aaron Fox", "Alperen Sengun", "Domantas Sabonis", "Norman Powell",
    # 2026 All-Stars (add/adjust as selected)
    "Anthony Davis", "Luka Doncic", "Trae Young", "Tyrese Haliburton",
    "Paolo Banchero", "Devin Booker", "Bam Adebayo", "Kyrie Irving",
    "Jaren Jackson Jr.", "Franz Wagner", "Jalen Williams",
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
    for scr in screens:
        for p in scr.get("players", []):
            if p["name"] == name and p["ratingNum"]:
                return p["ratingNum"]
    return 0


def _s(label, va, vb, fmt="1", invert=False):
    """Build one stat row for a comparison card."""
    va = va or 0
    vb = vb or 0
    if isinstance(va, str):
        try: va = float(va)
        except: va = 0
    if isinstance(vb, str):
        try: vb = float(vb)
        except: vb = 0
    if invert:
        winner = "A" if va < vb else ("B" if vb < va else "tie")
    else:
        winner = "A" if va > vb else ("B" if vb > va else "tie")
    return {"label": label, "a": round(va, 1), "b": round(vb, 1), "fmt": fmt, "winner": winner}


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
    advanced.append(_s("Rating", rat_a, rat_b, "2"))
    advanced.append(_s("Plus/Minus", adv_a.get("PLUS_MINUS"), adv_b.get("PLUS_MINUS"), "s1"))
    advanced.append(_s("Net Rating", adv_a.get("NET_RATING"), adv_b.get("NET_RATING"), "s1"))
    sections.append({"label": "ADVANCED", "stats": advanced})

    # DEFENSE
    defense = []
    dfg_a = adv_a.get("D_FG_PCT") or 0
    dfg_b = adv_b.get("D_FG_PCT") or 0
    xfg_a = adv_a.get("NORMAL_FG_PCT") or 0
    xfg_b = adv_b.get("NORMAL_FG_PCT") or 0
    defense.append(_s("Def FG%", dfg_a * 100 if dfg_a else 0, dfg_b * 100 if dfg_b else 0, "1", invert=True))
    defense.append({"label": "Exp FG%", "a": round(xfg_a * 100, 1) if xfg_a else 0,
                     "b": round(xfg_b * 100, 1) if xfg_b else 0, "fmt": "1", "winner": "tie"})
    diff_a = round(((dfg_a or 0) - (xfg_a or 0)) * 100, 1)
    diff_b = round(((dfg_b or 0) - (xfg_b or 0)) * 100, 1)
    defense.append({"label": "FG +/-", "a": diff_a, "b": diff_b, "fmt": "s1",
                     "winner": "A" if diff_a < diff_b else ("B" if diff_b < diff_a else "tie")})
    sections.append({"label": "DEFENSE", "stats": defense})

    # CLUTCH
    clutch = []
    cgp_a = adv_a.get("CLUTCH_GP", 1) or 1
    cgp_b = adv_b.get("CLUTCH_GP", 1) or 1
    for lbl, key in [("Points", "CLUTCH_PTS"), ("Rebounds", "CLUTCH_REB"),
                      ("Assists", "CLUTCH_AST"), ("Steals", "CLUTCH_STL"), ("Blocks", "CLUTCH_BLK")]:
        va = round((adv_a.get(key, 0) or 0) / cgp_a, 1)
        vb = round((adv_b.get(key, 0) or 0) / cgp_b, 1)
        clutch.append(_s(lbl, va, vb))
    sections.append({"label": "CLUTCH", "stats": clutch})

    # HUSTLE
    hustle = []
    hgp_a = adv_a.get("HUSTLE_GP", 1) or 1
    hgp_b = adv_b.get("HUSTLE_GP", 1) or 1
    for lbl, key in [("Contested", "CONTESTED_SHOTS"), ("Deflections", "DEFLECTIONS"),
                      ("Charges", "CHARGES_DRAWN")]:
        va = round((adv_a.get(key, 0) or 0) / hgp_a, 2)
        vb = round((adv_b.get(key, 0) or 0) / hgp_b, 2)
        fmt = "2" if key == "CHARGES_DRAWN" else "1"
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
        "posA": _player_position_map.get(name_a, ""),
        "posB": _player_position_map.get(name_b, ""),
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
        ("advanced", {"PLUS_MINUS": "PLUS_MINUS", "NET_RATING": "NET_RATING"}),
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
    lebron_adv = [k for k in _player_adv_stats if 'james' in k.lower() or 'lebron' in k.lower()]
    log.info(f"  LeBron variants in adv stats: {lebron_adv}")


def fetch_comparisons():
    """Build comparison screens: positional rankings, tonight's matchups, All-Star H2H."""
    global last_good_comparisons

    if "comp" in comparisons_cache:
        return comparisons_cache["comp"]

    if not _depth_starters or not _player_full_stats:
        log.warning("Comparisons: missing depth/stats data, skipping")
        return last_good_comparisons

    screens = []

    # 1) POSITIONAL POWER RANKINGS — top 20 starters per position
    for pos in ["PG", "SG", "SF", "PF", "C"]:
        players_at_pos = []
        for name, p_pos in _player_position_map.items():
            if p_pos != pos:
                continue
            rat = _get_rating(name)
            stats = _find_stats(name)
            if not rat:
                continue
            players_at_pos.append({
                "rank": 0,
                "name": name,
                "team": team_city(_player_team_map.get(name, "")),
                "rating": f"{rat:.2f}",
                "ratingNum": rat,
                "games": stats.get("GP", ""),
                "pts": f"{stats.get('PTS', 0):.1f}" if stats.get("PTS") else "",
                "reb": f"{stats.get('REB', 0):.1f}" if stats.get("REB") else "",
                "ast": f"{stats.get('AST', 0):.1f}" if stats.get("AST") else "",
                "country": _PLAYER_COUNTRY.get(name, ""),
            })
        players_at_pos.sort(key=lambda x: x["ratingNum"], reverse=True)
        top = players_at_pos[:20]
        for i, p in enumerate(top):
            p["rank"] = i + 1
        if top:
            screens.append({
                "title": f"{_POS_LABELS[pos]} Rankings — Global Rating",
                "subtitle": f"Starting {pos}s only | Season rating",
                "isPositional": True,
                "players": top,
            })

    # 2) TONIGHT'S MATCHUPS — best starter vs starter per game
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

    # 3) ALL-STAR HEAD-TO-HEAD — random pairs
    available = [n for n in _ALL_STARS if n in _player_position_map and _get_rating(n)]
    random.shuffle(available)
    used = set()
    allstar_pairs = []
    for a in available:
        if a in used:
            continue
        a_pos = _player_position_map.get(a, "")
        compat = _POS_COMPAT.get(a_pos, [a_pos])
        for b in available:
            if b == a or b in used:
                continue
            b_pos = _player_position_map.get(b, "")
            if b_pos in compat:
                allstar_pairs.append((a, b))
                used.add(a)
                used.add(b)
                break
        if len(allstar_pairs) >= 20:
            break

    for a, b in allstar_pairs:
        comp = _build_comparison(a, b)
        screens.append({
            "title": "All-Star Head-to-Head",
            "subtitle": f"{_player_position_map.get(a, '')} vs {_player_position_map.get(b, '')}",
            "isComparison": True,
            "comparison": comp,
        })

    result = {"screens": screens}
    comparisons_cache["comp"] = result
    last_good_comparisons = result

    # Diagnostic: log any All-Star with missing stats
    for name in _ALL_STARS:
        if name in _player_position_map:
            s = _find_stats(name)
            a = _find_adv(name)
            if not s.get("PTS") and not s.get("STL"):
                log.warning(f"  Comparison missing stats for {name}")

    log.info(f"Comparisons: {len(screens)} screens ({sum(1 for s in screens if s.get('isPositional'))} positional, "
             f"{sum(1 for s in screens if s.get('isPreview'))} preview, "
             f"{sum(1 for s in screens if 'All-Star' in s.get('title',''))} all-star)")
    return result


def _get_upcoming_games():
    """Fetch next game date from NBA schedule when no games today."""
    try:
        url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
        resp = requests.get(url, headers=_NBA_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        today_str = datetime.now().strftime("%Y-%m-%d")
        dates = data.get("leagueSchedule", {}).get("gameDates", [])
        for gd in dates:
            game_date = gd.get("gameDate", "")[:10]
            if game_date <= today_str:
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
                        from zoneinfo import ZoneInfo
                        et = utc.astimezone(ZoneInfo("America/New_York"))
                        tip = et.strftime("%-I:%M %p ET")
                    except:
                        pass
                month_day = ""
                try:
                    parts = game_date.split("-")
                    months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                    month_day = f"{months[int(parts[1])]} {int(parts[2])}"
                except:
                    pass
                games.append({
                    "away": {"abbr": away.get("teamTricode", ""), "teamId": away.get("teamId")},
                    "home": {"abbr": home.get("teamTricode", ""), "teamId": home.get("teamId")},
                    "status": "scheduled",
                    "label": month_day,
                    "tip": tip,
                })
            if games:
                log.info(f"  Found {len(games)} upcoming games on {game_date}")
                return games
    except Exception as e:
        log.warning(f"Upcoming games fetch failed: {e}")
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

injuries_cache = TTLCache(maxsize=1, ttl=900)  # 15 min
last_good_injuries = []
_questionable_players = set()  # Players with Questionable/Doubtful/Game Time Decision status


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

    # Update global questionable set for depth chart cross-reference
    _questionable_players.clear()
    for team_name, team_players in teams.items():
        for p in team_players:
            st = (p["status"] or "").lower()
            if st in ("questionable", "doubtful", "game time decision", "day-to-day"):
                raw_name = p["name"]
                _questionable_players.add(raw_name)
                # Also add resolved name for depth chart cross-reference
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
    pos_set_detect = {"PG", "SG", "SF", "PF", "C"}
    for ri, row in enumerate(reader):
        if len(row) >= 5:
            cols = [row[i].strip() for i in range(5)]
            if all(c in pos_set_detect for c in cols):
                header_rows.append(ri)

    log.info(f"Depth charts: found {len(header_rows)} header rows (salary lookup: {len(_salary_team_lookup)} players, name maps: {len(_full_name_map)} abbrevs, {len(_last_name_map)} last names)")

    all_teams = []
    pos_set = set(_POSITIONS)  # {"PG", "SG", "SF", "PF", "C"}
    for hi, hrow in enumerate(header_rows):
        end = header_rows[hi + 1] if hi + 1 < len(header_rows) else len(reader)
        # Read actual position labels from this header row (but force standard for display)
        # Memphis has PF/PF instead of PF/C, but structurally col 5 is always the center

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
            # Detect next team's position header row
            if all(c in pos_set_detect for c in cols):
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
                        full_name = resolve_player_name(names[pi])
                        level["players"].append({
                            "pos": _POSITIONS[pi],
                            "name": full_name,
                            "salary": salaries[pi],
                            "country": _PLAYER_COUNTRY.get(full_name, _PLAYER_COUNTRY.get(names[pi], "")),
                            "questionable": full_name in _questionable_players,
                        })
                if level["players"]:
                    levels.append(level)

            ri += 1

        if levels:
            # Identify team by looking up player names in salary data
            team_votes = {}
            unmatched = []
            for level in levels:
                for p in level["players"]:
                    t = _salary_team_lookup.get(p["name"], "")
                    if t:
                        team_votes[t] = team_votes.get(t, 0) + 1
                    else:
                        unmatched.append(p["name"])
            if team_votes:
                team_name = max(team_votes, key=team_votes.get)
            else:
                team_name = f"Unknown Team {hi + 1}"
            if unmatched:
                log.info(f"  Block {hi}: {team_name} ({team_votes.get(team_name,0)} votes, unmatched: {unmatched})")
            else:
                log.info(f"  Block {hi}: {team_name} ({team_votes.get(team_name,0)} votes, all matched)")
            all_teams.append({"name": team_name, "levels": levels})

    # Sort teams alphabetically for display
    all_teams.sort(key=lambda t: t["name"])

    # Build player→team cross-reference for team ratings
    _player_team_map.clear()
    _player_position_map.clear()
    _depth_starters.clear()
    for t in all_teams:
        if t["name"].startswith("Unknown"):
            continue  # Don't pollute map with unidentified teams
        for level in t["levels"]:
            for p in level["players"]:
                _player_team_map[p["name"]] = t["name"]
        # Populate starters (first level)
        if t["levels"]:
            starters = t["levels"][0]["players"]
            _depth_starters[t["name"]] = [{"name": p["name"], "pos": p["pos"]} for p in starters]
            for p in starters:
                _player_position_map[p["name"]] = p["pos"]

    # Log all identified teams
    team_names = [t["name"] for t in all_teams]
    log.info(f"Depth charts: {len(all_teams)} teams identified: {team_names}")
    has_memphis = any("emphis" in n for n in team_names)
    log.info(f"Memphis present: {has_memphis}")
    if not has_memphis:
        # Log all vote results for debugging
        log.warning("Memphis NOT found! Check team votes above.")

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
        fetch_injuries()
    except Exception as e:
        log.warning(f"Pre-warm injuries failed: {e}")

    try:
        fetch_depth()
    except Exception as e:
        log.warning(f"Pre-warm depth charts failed: {e}")

    try:
        fetch_team_ratings()
    except Exception as e:
        log.warning(f"Pre-warm team ratings failed: {e}")

    try:
        fetch_hist_salaries()
    except Exception as e:
        log.warning(f"Pre-warm historical salaries failed: {e}")

    try:
        fetch_counting_stats()
    except Exception as e:
        log.warning(f"Pre-warm counting stats failed: {e}")

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

    try:
        fetch_advanced_stats()
    except Exception as e:
        log.warning(f"Pre-warm advanced stats failed: {e}")

    try:
        fetch_comparisons()
    except Exception as e:
        log.warning(f"Pre-warm comparisons failed: {e}")

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
