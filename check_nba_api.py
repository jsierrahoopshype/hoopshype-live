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
    ("Standings V3", "https://stats.nba.com/stats/leaguestandingsv3", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season"}),
    ("Standings", "https://stats.nba.com/stats/leaguestandings", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season"}),
    ("Team Stats", "https://stats.nba.com/stats/leaguedashteamstats", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season", "MeasureType": "Base", "PerMode": "PerGame"}),
    ("Player Stats", "https://stats.nba.com/stats/leaguedashplayerstats", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season", "MeasureType": "Base", "PerMode": "PerGame"}),
    ("Player Clutch", "https://stats.nba.com/stats/leaguedashplayerclutch", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season", "MeasureType": "Base", "PerMode": "PerGame", "ClutchTime": "Last 5 Minutes", "AheadBehind": "Ahead or Behind", "PointDiff": "5"}),
    ("Team Clutch", "https://stats.nba.com/stats/leaguedashteamclutch", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season", "MeasureType": "Base", "PerMode": "PerGame", "ClutchTime": "Last 5 Minutes", "AheadBehind": "Ahead or Behind", "PointDiff": "5"}),
    ("Hustle Leaders", "https://stats.nba.com/stats/leaguehustlestatsplayer", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season", "PerMode": "PerGame"}),
    ("Shot Dashboard", "https://stats.nba.com/stats/leaguedashplayerptshot", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season", "PerMode": "PerGame"}),
    ("Defense Dashboard", "https://stats.nba.com/stats/leaguedashptdefend", {"LeagueID": "00", "Season": "2024-25", "SeasonType": "Regular Season", "PerMode": "PerGame", "DefenseCategory": "Overall"}),
]

# Also test CDN endpoints
cdn_endpoints = [
    ("CDN Standings", "https://cdn.nba.com/static/json/liveData/standings/standings_00.json", None),
    ("CDN Team Leaders", "https://cdn.nba.com/static/json/liveData/playerstats/00_league_leaders.json", None),
    ("CDN Player Index", "https://cdn.nba.com/static/json/staticData/player-index/player-index-data.json", None),
]

for name, url, params in endpoints:
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            rs = data.get("resultSets", data.get("resultSet", []))
            if isinstance(rs, list) and rs:
                h = rs[0].get("headers", [])
                rows = rs[0].get("rowSet", [])
                print(f"✓ {name}: {r.status_code} — {len(rows)} rows, {len(h)} cols")
                print(f"  Headers: {h[:15]}{'...' if len(h)>15 else ''}")
                if rows:
                    print(f"  Sample: {rows[0][:8]}...")
            elif isinstance(rs, dict):
                h = rs.get("headers", [])
                rows = rs.get("rowSet", [])
                print(f"✓ {name}: {r.status_code} — {len(rows)} rows")
                print(f"  Headers: {h[:15]}")
            else:
                print(f"? {name}: {r.status_code} — unusual structure: {list(data.keys())[:5]}")
        else:
            print(f"✗ {name}: {r.status_code}")
    except Exception as e:
        print(f"✗ {name}: {type(e).__name__}: {e}")
    print()

print("\n=== CDN ENDPOINTS ===\n")
for name, url, _ in cdn_endpoints:
    try:
        r = requests.get(url, headers={"User-Agent": headers["User-Agent"]}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            keys = list(data.keys())[:5]
            print(f"✓ {name}: {r.status_code} — keys: {keys}")
            # Dig one level deep
            for k in keys:
                v = data[k]
                if isinstance(v, dict):
                    print(f"  {k}: dict with keys {list(v.keys())[:8]}")
                elif isinstance(v, list):
                    print(f"  {k}: list of {len(v)} items")
                    if v and isinstance(v[0], dict):
                        print(f"    first item keys: {list(v[0].keys())[:10]}")
                else:
                    print(f"  {k}: {v}")
        else:
            print(f"✗ {name}: {r.status_code}")
    except Exception as e:
        print(f"✗ {name}: {type(e).__name__}: {e}")
    print()
