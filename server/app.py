"""
HoopsHype Live — Flask Server
Serves the broadcast page and API endpoints for live data.

Phase 1: Bluesky feed + HoopsHype headlines
Phase 2: nba_api scores (TODO)
Phase 3: Google Sheets rankings (TODO)
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
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

# Fallback data (served when fetch fails)
last_good_bluesky = []
last_good_headlines = []

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
        # getAuthorFeed accepts handles directly — no need to resolve DID first
        feed_url = (
            f"https://bsky.social/xrpc/app.bsky.feed.getAuthorFeed"
            f"?actor={handle}&limit=3&filter=posts_no_replies"
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
    with ThreadPoolExecutor(max_workers=BLUESKY_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_one_feed, handle): handle
            for handle in config.BLUESKY_ACCOUNTS
        }
        for future in as_completed(futures):
            try:
                posts = future.result()
                all_posts.extend(posts)
            except Exception as e:
                log.debug(f"Bluesky worker error: {e}")

    # Sort by timestamp (newest first) and limit
    all_posts.sort(key=lambda p: p.get("timestamp", ""), reverse=True)
    all_posts = all_posts[:config.BLUESKY_MAX_POSTS]

    if all_posts:
        last_good_bluesky = all_posts
        bluesky_cache["posts"] = all_posts
        log.info(f"Fetched {len(all_posts)} Bluesky posts")
    else:
        log.info("No new Bluesky posts, serving last good data")

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

def fetch_headlines():
    """Scrape headlines from HoopsHype Rumors page."""
    global last_good_headlines

    if "headlines" in headlines_cache:
        return headlines_cache["headlines"]

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        resp = requests.get(config.HEADLINES_URL, headers=headers, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        items = []

        # Strategy 1: WordPress post-loop / article entries (most common HoopsHype structure)
        # HoopsHype is a USA Today / Gannett WordPress site.
        # Rumors page lists posts as <article> or <div> entries with headlines in <h2>/<h3> links.
        selectors = [
            "article",                    # standard WP article tags
            "div.post-loop-item",         # common WP theme pattern
            "div.entry-content li",       # sometimes rumors are listed as <li> items
            "div[class*='post']",         # any div with 'post' in class
        ]

        articles = []
        for sel in selectors:
            articles = soup.select(sel)
            if articles:
                break

        for article in articles:
            # Find headline text — try multiple patterns
            title_el = (
                article.select_one("h2 a") or
                article.select_one("h3 a") or
                article.select_one("h2") or
                article.select_one("h3") or
                article.select_one("a[class*='title']") or
                article.select_one("a[class*='headline']")
            )
            if not title_el:
                continue

            text = title_el.get_text(strip=True)
            if not text or len(text) < 10:
                continue

            # Deduplicate
            if any(item["text"] == text for item in items):
                continue

            # Find timestamp
            time_el = article.select_one("time[datetime]")
            if not time_el:
                time_el = article.select_one("time, .date, .timestamp, .post-date, span[class*='date'], span[class*='time']")

            time_str = ""
            is_new = False

            if time_el:
                dt_attr = time_el.get("datetime", "")
                time_str = time_el.get_text(strip=True)

                if dt_attr:
                    is_new = _is_recent(dt_attr, config.HEADLINES_NEW_THRESHOLD_MINUTES)
                    time_str = _time_ago(dt_attr) or time_str

            items.append({
                "text": text,
                "time": time_str,
                "isNew": is_new,
            })

            if len(items) >= config.HEADLINES_MAX_ITEMS:
                break

        # Strategy 2: If no articles found, try to find headlines in a simpler flat list
        if not items:
            for link in soup.select("a[href*='/rumors/'], a[href*='/rumor/']"):
                text = link.get_text(strip=True)
                if text and len(text) >= 20 and not any(item["text"] == text for item in items):
                    items.append({"text": text, "time": "", "isNew": False})
                    if len(items) >= config.HEADLINES_MAX_ITEMS:
                        break

        if items:
            last_good_headlines = items
            headlines_cache["headlines"] = items
            log.info(f"Fetched {len(items)} headlines from HoopsHype")
        else:
            log.warning("No headlines parsed — selectors may need updating")

    except Exception as e:
        log.error(f"Headlines fetch failed: {e}")

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
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


# ═══════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("HoopsHype Live — Starting server")
    log.info(f"Bluesky accounts: {len(config.BLUESKY_ACCOUNTS)}")
    log.info(f"Headlines source: {config.HEADLINES_URL}")
    log.info(f"Server: http://localhost:{config.SERVER_PORT}")
    log.info("=" * 50)

    app.run(
        host=config.SERVER_HOST,
        port=config.SERVER_PORT,
        debug=config.DEBUG,
        threaded=True,  # handle concurrent requests (Bluesky fetch blocks for 30s+)
    )
