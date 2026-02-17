import requests, json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Origin": "https://www.nba.com",
}

endpoints = [
    ("Standings", "https://stats.nba.com/stats/leaguestandingsv3", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season"}),
    ("Team Stats", "https://stats.nba.com/stats/leaguedashteamstats", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season", "MeasureType": "Base", "PerMode": "PerGame"}),
    ("League Leaders (known working)", "https://stats.nba.com/stats/leagueleaders", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season", "PerMode": "PerGame", "Scope": "S", "StatCategory": "PTS"}),
]

for name, url, params in endpoints:
    print(f"Trying {name} (60s timeout)...")
    try:
        r = requests.get(url, params=params, headers=headers, timeout=60)
        if r.status_code == 200:
            data = r.json()
            rs = data.get("resultSets", data.get("resultSet", []))
            if isinstance(rs, list) and rs:
                h = rs[0].get("headers", [])
                rows = rs[0].get("rowSet", [])
                print(f"  ✓ {len(rows)} rows, {len(h)} cols")
                print(f"  Headers: {h[:20]}")
                if rows:
                    print(f"  Row 1: {rows[0][:12]}...")
                    if len(rows) > 1:
                        print(f"  Row 2: {rows[1][:12]}...")
            else:
                print(f"  ? unusual: keys={list(data.keys())[:5]}")
        else:
            print(f"  ✗ Status {r.status_code}")
    except Exception as e:
        print(f"  ✗ {type(e).__name__}: {e}")
    print()

# Also try nba.com data endpoints (different from CDN)
print("=== Alternative endpoints ===\n")
alt = [
    ("nba.com standings page data", "https://nba.com/stats/standings", None),
    ("data.nba.com standings", "https://data.nba.net/prod/v1/current/standings_conference.json", None),
]
for name, url, _ in alt:
    print(f"Trying {name}...")
    try:
        r = requests.get(url, headers={"User-Agent": headers["User-Agent"]}, timeout=30)
        print(f"  Status: {r.status_code}, length: {len(r.text)}")
        if r.status_code == 200 and r.text.startswith('{'):
            data = r.json()
            print(f"  Keys: {list(data.keys())[:5]}")
    except Exception as e:
        print(f"  ✗ {type(e).__name__}: {e}")
    print()
