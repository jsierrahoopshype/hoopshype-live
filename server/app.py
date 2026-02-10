"""
HoopsHype Live — Flask Server
Serves the broadcast page and API endpoints for live data.

Phase 1: Bluesky feed + HoopsHype headlines
Phase 2: nba_api scores (TODO)
Phase 3: Google Sheets rankings (TODO)
"""

import logging
import os
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

def _fetch_headlines_rss():
    """Fetch headlines from HoopsHype RSS feed (primary strategy)."""
    rss_urls = [
        config.HEADLINES_RSS_URL,          # rumors-specific feed
        config.HEADLINES_RSS_FALLBACK_URL,  # main site feed
    ]

    for rss_url in rss_urls:
        try:
            resp = requests.get(rss_url, headers=_HTTP_HEADERS, timeout=15)
            resp.raise_for_status()

            root = ET.fromstring(resp.content)
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

        except Exception as e:
            log.debug(f"RSS fetch failed for {rss_url}: {e}")

    return None  # signal to try HTML fallback


def _fetch_headlines_html():
    """Scrape headlines from HoopsHype HTML page (fallback strategy)."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        resp = requests.get(config.HEADLINES_URL, headers=headers, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "lxml")
        items = []

        # Try multiple CSS selector strategies for Gannett/WordPress layouts
        selectors = [
            "article",
            "div.gnt-story-hp",           # Gannett CMS story cards
            "a.gnt-story-hp__headline",   # Gannett headline links
            "div.post-loop-item",
            "div.entry-content li",
        ]

        articles = []
        for sel in selectors:
            articles = soup.select(sel)
            if articles:
                log.debug(f"HTML scraper matched selector: {sel} ({len(articles)} elements)")
                break

        for article in articles:
            title_el = (
                article.select_one("h2 a") or
                article.select_one("h3 a") or
                article.select_one("a.gnt-story-hp__headline") or
                article.select_one("h2") or
                article.select_one("h3") or
                article.select_one("a[class*='title']") or
                article.select_one("a[class*='headline']")
            )

            # If the matched element itself is an <a> with headline text, use it
            if not title_el and article.name == "a":
                title_el = article

            if not title_el:
                continue

            text = title_el.get_text(strip=True)
            if not text or len(text) < 15:
                continue

            if any(it["text"] == text for it in items):
                continue

            time_el = (
                article.select_one("time[datetime]") or
                article.select_one("time") or
                article.select_one("[class*='date']") or
                article.select_one("[data-date]")
            )

            time_str = ""
            is_new = False

            if time_el:
                dt_attr = time_el.get("datetime") or time_el.get("data-date", "")
                if dt_attr:
                    is_new = _is_recent(dt_attr, config.HEADLINES_NEW_THRESHOLD_MINUTES)
                    time_str = _time_ago(dt_attr) or time_el.get_text(strip=True)
                else:
                    time_str = time_el.get_text(strip=True)

            items.append({"text": text, "time": time_str, "isNew": is_new})
            if len(items) >= config.HEADLINES_MAX_ITEMS:
                break

        # Last resort: find any rumor links
        if not items:
            for link in soup.select("a[href*='/rumors/'], a[href*='/rumor/'], a[href*='/story/']"):
                text = link.get_text(strip=True)
                if text and len(text) >= 25 and not any(it["text"] == text for it in items):
                    items.append({"text": text, "time": "", "isNew": False})
                    if len(items) >= config.HEADLINES_MAX_ITEMS:
                        break

        if items:
            log.info(f"Fetched {len(items)} headlines via HTML scraping")
        else:
            log.warning("HTML scraper found no headlines — selectors may need updating")

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
