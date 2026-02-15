import requests, json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json",
}

# Approach 1: CDN static JSON (like we use for scores)
urls = [
    ("CDN leaguedashplayerstats", "https://cdn.nba.com/static/json/liveData/playerstats/leagueDashPlayerStats_00_2025-26_RegularSeason_Totals.json"),
    ("CDN allplayers", "https://cdn.nba.com/static/json/staticData/allPlayers/allPlayers_00_2025-26_RegularSeason_Totals.json"),
]

for label, url in urls:
    try:
        r = requests.get(url, headers=headers, timeout=15)
        print(f"{label}: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            keys = list(data.keys())[:5]
            print(f"  Top keys: {keys}")
            # Try to find player data
            for k in data:
                if isinstance(data[k], list) and len(data[k]) > 0:
                    print(f"  '{k}': {len(data[k])} items, first: {str(data[k][0])[:200]}")
                    break
                elif isinstance(data[k], dict):
                    subkeys = list(data[k].keys())[:5]
                    print(f"  '{k}': dict with keys {subkeys}")
    except Exception as e:
        print(f"{label}: FAILED - {e}")

# Approach 2: stats.nba.com with longer timeout and session
print("\n--- Trying stats.nba.com with session ---")
s = requests.Session()
s.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Origin": "https://www.nba.com",
    "Connection": "keep-alive",
    "Host": "stats.nba.com",
})

try:
    r = s.get("https://stats.nba.com/stats/leaguedashplayerstats", params={
        "Season": "2025-26",
        "SeasonType": "Regular Season",
        "PerMode": "Totals",
        "MeasureType": "Base",
        "LeagueID": "00",
    }, timeout=60)
    print(f"stats.nba.com: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        rs = data.get("resultSets", [])
        if rs:
            h = rs[0].get("headers", [])
            rows = rs[0].get("rowSet", [])
            print(f"  {len(rows)} players, {len(h)} columns")
            print(f"  Headers: {h[:10]}")
            if rows:
                pts_i = h.index("PTS") if "PTS" in h else -1
                name_i = h.index("PLAYER_NAME") if "PLAYER_NAME" in h else 1
                team_i = h.index("TEAM_ABBREVIATION") if "TEAM_ABBREVIATION" in h else 3
                top = sorted(rows, key=lambda r: r[pts_i] or 0, reverse=True)[:3]
                for row in top:
                    print(f"  {row[name_i]:25s} {row[team_i]:4s} PTS={row[pts_i]}")
    else:
        print(f"  Body: {r.text[:300]}")
except Exception as e:
    print(f"stats.nba.com: FAILED - {type(e).__name__}: {e}")
