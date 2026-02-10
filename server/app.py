"""
HoopsHype Live — Flask Server
Serves the broadcast page and API endpoints for live data.

Phase 1: Bluesky feed + HoopsHype headlines
Phase 2: nba_api scores (TODO)
Phase 3: Google Sheets rankings (TODO)
"""

import time
import logging
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
from cachetools import TTLCache

import config

# ─── Setup ───
app = Flask(__name__, static_folder="..")
CORS(app)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hoopshype-live")

# ─── Caches ───
bluesky_cache = TTLCache(maxsize=1, ttl=config.BLUESKY_CACHE_TTL_SECONDS)
headlines_cache = TTLCache(maxsize=1, ttl=config.HEADLINES_CACHE_TTL_SECONDS)

# Fallback data (served when fetch fails)
last_good_bluesky = []
last_good_headlines = []


# ═══════════════════════════════════════
# BLUESKY FEED
# ═══════════════════════════════════════

def fetch_bluesky_posts():
    """Fetch recent posts from configured Bluesky accounts via public API."""
    global last_good_bluesky

    # Return cached if available
    if "posts" in bluesky_cache:
        return bluesky_cache["posts"]

    all_posts = []
    for handle in config.BLUESKY_ACCOUNTS:
        try:
            # Resolve DID
            resolve_url = f"https://bsky.social/xrpc/com.atproto.identity.resolveHandle?handle={handle}"
            resp = requests.get(resolve_url, timeout=10)
            resp.raise_for_status()
            did = resp.json().get("did")
            if not did:
                continue

            # Fetch author feed
            feed_url = f"https://bsky.social/xrpc/app.bsky.feed.getAuthorFeed?actor={did}&limit=5&filter=posts_no_replies"
            resp = requests.get(feed_url, timeout=10)
            resp.raise_for_status()
            feed = resp.json().get("feed", [])

            for item in feed:
                post = item.get("post", {})
                record = post.get("record", {})
                author = post.get("author", {})

                # Skip reposts if configured
                if not config.BLUESKY_SHOW_REPOSTS and item.get("reason"):
                    continue

                # Parse timestamp
                created = record.get("createdAt", "")
                time_ago = _time_ago(created)

                all_posts.append({
                    "author": author.get("displayName", handle),
                    "handle": f"@{author.get('handle', handle)}",
                    "avatar": _initials(author.get("displayName", handle)),
                    "text": record.get("text", ""),
                    "time": time_ago,
                    "timestamp": created,
                })

        except Exception as e:
            log.warning(f"Bluesky fetch failed for {handle}: {e}")
            continue

    # Sort by timestamp (newest first) and limit
    all_posts.sort(key=lambda p: p.get("timestamp", ""), reverse=True)
    all_posts = all_posts[:config.BLUESKY_MAX_POSTS]

    if all_posts:
        last_good_bluesky = all_posts
        bluesky_cache["posts"] = all_posts
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
    except:
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
            "User-Agent": "HoopsHypeLive/1.0 (internal broadcast tool)"
        }
        resp = requests.get(config.HEADLINES_URL, headers=headers, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        items = []

        # HoopsHype rumors page structure — look for article headlines
        # This selector may need updating if the page structure changes
        for article in soup.select("div.rumor-block, article, .post-loop"):
            # Try multiple selectors for resilience
            title_el = (
                article.select_one("h2 a") or
                article.select_one("h3 a") or
                article.select_one(".rumor-title a") or
                article.select_one("a.title")
            )
            if not title_el:
                continue

            text = title_el.get_text(strip=True)
            if not text or len(text) < 10:
                continue

            # Try to get timestamp
            time_el = article.select_one("time, .date, .timestamp, .post-date")
            time_str = ""
            is_new = False

            if time_el:
                time_str = time_el.get_text(strip=True)
                # Check if it has a datetime attribute
                dt_attr = time_el.get("datetime", "")
                if dt_attr:
                    is_new = _is_recent(dt_attr, config.HEADLINES_NEW_THRESHOLD_MINUTES)

            items.append({
                "text": text,
                "time": time_str,
                "isNew": is_new,
            })

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
    except:
        return False


# ═══════════════════════════════════════
# API ROUTES
# ═══════════════════════════════════════

@app.route("/")
def serve_index():
    """Serve the broadcast overlay page."""
    return send_from_directory("..", "index.html")


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
    )
