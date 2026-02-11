"""
HoopsHype Live — Flask Server
Serves the broadcast page and API endpoints for live data.

Phase 1: Bluesky feed + HoopsHype headlines
Phase 2: Live NBA scores via nba.com CDN
Phase 3: Google Sheets rankings (TODO)
"""

import logging
import os
import re
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
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
# HOOPSHYPE HEADLINES
# ═══════════════════════════════════════

def _fetch_headlines_rss():
    """Fetch headlines from HoopsHype RSS feed (primary strategy).

    Tries the rumors-specific feed first, then the main site feed.
    WordPress always exposes RSS at /feed/ — no auto-discovery needed.
    """
    rss_urls = [
        config.HEADLINES_RSS_URL,          # https://hoopshype.com/rumors/feed/
        config.HEADLINES_RSS_FALLBACK_URL,  # https://hoopshype.com/feed/
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    for rss_url in rss_urls:
        log.info(f"Trying RSS feed: {rss_url}")
        try:
            resp = requests.get(rss_url, headers=headers, timeout=15)
            resp.raise_for_status()

            # Sanity check: RSS/XML should not start with <!DOCTYPE html
            content = resp.content
            if content.lstrip()[:20].lower().startswith(b"<!doctype"):
                log.warning(f"RSS URL returned HTML instead of XML: {rss_url}")
                continue

            root = ET.fromstring(content)
            items = []

            for item_el in root.iter("item"):
                title = (item_el.findtext("title") or "").strip()
                if not title or len(title) < 10:
                    continue

                # Deduplicate
                if any(it["text"] == title for it in items):
                    continue

                # Parse pubDate (RFC 822 format: "Mon, 10 Feb 2025 12:34:56 +0000")
                pub_date = (item_el.findtext("pubDate") or "").strip()
                time_str = ""
                is_new = False

                if pub_date:
                    try:
                        dt = parsedate_to_datetime(pub_date)
                        iso_str = dt.isoformat()
                        time_str = _time_ago(iso_str)
                        is_new = _is_recent(iso_str, config.HEADLINES_NEW_THRESHOLD_MINUTES)
                    except Exception:
                        pass

                items.append({
                    "text": title,
                    "time": time_str,
                    "isNew": is_new,
                })

                if len(items) >= config.HEADLINES_MAX_ITEMS:
                    break

            if items:
                log.info(f"Fetched {len(items)} headlines via RSS from {rss_url}")
                return items
            else:
                log.warning(f"RSS feed parsed but contained no usable items: {rss_url}")

        except ET.ParseError as e:
            log.warning(f"RSS XML parse error for {rss_url}: {e}")
        except Exception as e:
            log.warning(f"RSS fetch failed for {rss_url}: {e}")

    return None  # signal to try HTML fallback


def _fetch_headlines_html():
    """Scrape headlines from HoopsHype HTML page (fallback strategy).

    HoopsHype is on Gannett's Presto CMS (USA TODAY Network) which uses
    gnt-* prefixed CSS classes. We try multiple strategies in order of
    specificity, falling back to broader link-based extraction.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        items = []
        seen_texts = set()

        def _add_item(text, time_str="", is_new=False):
            """Add a headline if it's unique and long enough."""
            text = text.strip()
            if not text or len(text) < 20:
                return False
            # Deduplicate by normalized text
            norm = text.lower()
            if norm in seen_texts:
                return False
            seen_texts.add(norm)
            items.append({"text": text, "time": time_str, "isNew": is_new})
            return True

        # Try both the rumors page and main page for more headlines
        urls_to_try = [config.HEADLINES_URL, "https://hoopshype.com/"]

        for page_url in urls_to_try:
            if len(items) >= config.HEADLINES_MAX_ITEMS:
                break

            try:
                resp = requests.get(page_url, headers=headers, timeout=15)
                resp.raise_for_status()
            except Exception as e:
                log.warning(f"HTML fetch failed for {page_url}: {e}")
                continue

            soup = BeautifulSoup(resp.text, "lxml")

            # Strategy 1: Gannett CMS article elements with gnt-* classes
            for el in soup.select("[class*='gnt-'] a, a[class*='gnt-']"):
                text = el.get_text(strip=True)
                _add_item(text)
                if len(items) >= config.HEADLINES_MAX_ITEMS:
                    break

            if len(items) >= config.HEADLINES_MAX_ITEMS:
                break

            # Strategy 2: Article/section headline elements
            for sel in ["article h2 a", "article h3 a", "article h2", "article h3",
                        "section h2 a", "section h3 a",
                        "h2 a[href*='/story/']", "h3 a[href*='/story/']",
                        "h2 a[href*='/rumors/']", "h3 a[href*='/rumors/']"]:
                for el in soup.select(sel):
                    text = el.get_text(strip=True)
                    # Try to find a time element nearby
                    time_str = ""
                    is_new = False
                    parent = el.find_parent(["article", "section", "div"])
                    if parent:
                        time_el = parent.select_one("time[datetime]") or parent.select_one("[data-date]")
                        if time_el:
                            dt_attr = time_el.get("datetime") or time_el.get("data-date", "")
                            if dt_attr:
                                is_new = _is_recent(dt_attr, config.HEADLINES_NEW_THRESHOLD_MINUTES)
                                time_str = _time_ago(dt_attr)
                    _add_item(text, time_str, is_new)
                    if len(items) >= config.HEADLINES_MAX_ITEMS:
                        break
                if len(items) >= config.HEADLINES_MAX_ITEMS:
                    break

            if len(items) >= config.HEADLINES_MAX_ITEMS:
                break

            # Strategy 3: All links to /story/ or /rumors/ pages (Gannett URL pattern)
            for link in soup.select("a[href]"):
                href = link.get("href", "")
                if not any(p in href for p in ["/story/sports/nba/", "/rumors/", "/rumor/"]):
                    continue
                # Skip nav/footer/social links
                if any(skip in href for skip in ["facebook.com", "twitter.com", "#", "javascript:"]):
                    continue
                text = link.get_text(strip=True)
                _add_item(text)
                if len(items) >= config.HEADLINES_MAX_ITEMS:
                    break

        if items:
            log.info(f"Fetched {len(items)} headlines via HTML scraping from {len(urls_to_try)} pages")
        else:
            log.warning("HTML scraper found no headlines — site structure may have changed")

        return items if items else None

    except Exception as e:
        log.error(f"HTML headlines fetch failed: {e}")
        return None


def fetch_headlines():
    """Fetch headlines from HoopsHype — tries RSS first, then HTML scraping."""
    global last_good_headlines

    if "headlines" in headlines_cache:
        return headlines_cache["headlines"]

    log.info("Fetching HoopsHype headlines...")

    # Strategy 1: RSS feed (reliable, structured data)
    items = _fetch_headlines_rss()

    # Strategy 2: HTML scraping (fallback for when RSS is unavailable)
    if not items:
        log.info("RSS unavailable, trying HTML scraper...")
        items = _fetch_headlines_html()

    if items:
        last_good_headlines = items
        headlines_cache["headlines"] = items
    else:
        log.warning("No headlines from any source")

    return last_good_headlines


def _is_recent(iso_str, threshold_minutes):
    """Check if a timestamp is within the threshold."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        diff_minutes = (now - dt).total_seconds() / 60
        return diff_minutes <= threshold_minutes
    except Exception:
        return False


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
        return resp.json().get("game", {})
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
    pm_str = f"+{plus_minus}" if plus_minus > 0 else str(plus_minus)

    return {
        "num": player.get("jerseyNum", ""),
        "name": player.get("nameI", player.get("firstName", "") + " " + player.get("familyName", "")),
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
    starters = []
    bench = []
    for p in players:
        transformed = _transform_player(p)
        status = p.get("status", "ACTIVE")
        if status != "ACTIVE":
            continue
        if p.get("starter", "") == "1":
            starters.append(transformed)
        else:
            bench.append(transformed)
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
    """Transform gameLeaders from scoreboard → frontend leaders format."""
    return {
        "pts": {
            "name": _leader_name(leaders_data.get("name", "")),
            "val": leaders_data.get("points", 0),
        },
        "reb": {
            "name": _leader_name(leaders_data.get("name", "")),
            "val": leaders_data.get("rebounds", 0),
        },
        "ast": {
            "name": _leader_name(leaders_data.get("name", "")),
            "val": leaders_data.get("assists", 0),
        },
    }


def _leaders_from_boxscore_players(players):
    """Compute game leaders from boxscore player stats (more accurate than scoreboard leaders)."""
    pts_leader = max(players, key=lambda p: p.get("statistics", {}).get("points", 0), default={})
    reb_leader = max(players, key=lambda p: p.get("statistics", {}).get("reboundsTotal", 0), default={})
    ast_leader = max(players, key=lambda p: p.get("statistics", {}).get("assists", 0), default={})

    def _leader(player, stat_key):
        name = player.get("nameI", "") or (player.get("firstName", "") + " " + player.get("familyName", ""))
        return {
            "name": _leader_name(name),
            "val": player.get("statistics", {}).get(stat_key, 0),
        }

    return {
        "pts": _leader(pts_leader, "points"),
        "reb": _leader(reb_leader, "reboundsTotal"),
        "ast": _leader(ast_leader, "assists"),
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
            }

        # Stats and boxscore: from boxscore data if available
        if box_team:
            side["stats"] = _team_stats_from_boxscore(box_team)
            side["boxscore"] = _transform_team_boxscore(box_team)
        else:
            side["stats"] = {"fgPct": "0.0", "threePct": "0.0", "ftPct": "0.0", "reb": 0, "ast": 0, "stl": 0, "blk": 0, "to": 0}
            side["boxscore"] = {"starters": [], "bench": []}

        return side

    # Boxscore team data
    box_away = boxscore_data.get("awayTeam", {}) if boxscore_data else None
    box_home = boxscore_data.get("homeTeam", {}) if boxscore_data else None

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
    log.info(f"Headlines source: RSS → {config.HEADLINES_RSS_URL}")
    log.info(f"Headlines fallback: HTML → {config.HEADLINES_URL}")
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
