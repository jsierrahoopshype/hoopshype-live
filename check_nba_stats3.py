import requests, json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json",
}

# Try many possible CDN paths
urls = [
    "https://cdn.nba.com/static/json/liveData/leaders/leagueLeaders_00.json",
    "https://cdn.nba.com/static/json/liveData/playerstats/playerstats_00.json",
    "https://cdn.nba.com/static/json/staticData/leaders/leagueLeaders_00.json",
    "https://cdn.nba.com/static/json/staticData/league/leagueLeaders.json",
    "https://cdn.nba.com/static/json/liveData/league/leagueLeaders.json",
    "https://cdn.nba.com/static/json/liveData/stats/leagueLeaders_00.json",
    "https://cdn.nba.com/static/json/staticData/stats/leagueLeaders_00.json",
    "https://cdn.nba.com/static/json/liveData/leagueleaders/leagueleaders_00.json",
]

for url in urls:
    try:
        r = requests.get(url, headers=headers, timeout=10)
        short = url.split("nba.com/")[1]
        if r.status_code == 200:
            print(f"  ✓ {r.status_code} {short}")
            data = r.json()
            print(f"    Keys: {list(data.keys())[:5]}")
            print(f"    Preview: {str(data)[:300]}")
        else:
            print(f"  ✗ {r.status_code} {short}")
    except Exception as e:
        print(f"  ✗ ERR  {url.split('nba.com/')[1]}: {type(e).__name__}")

# Try leagueleaders on stats.nba.com (different endpoint, might work)
print("\n--- stats.nba.com/stats/leagueleaders ---")
try:
    r = requests.get("https://stats.nba.com/stats/leagueleaders", params={
        "Season": "2025-26",
        "SeasonType": "Regular Season",
        "PerMode": "Totals",
        "Scope": "S",
        "StatCategory": "PTS",
        "LeagueID": "00",
        "ActiveFlag": "",
    }, headers={
        **headers,
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
        "Origin": "https://www.nba.com",
        "Host": "stats.nba.com",
    }, timeout=60)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        rs = data.get("resultSet", {})
        h = rs.get("headers", [])
        rows = rs.get("rowSet", [])
        print(f"  {len(rows)} players, headers: {h[:10]}")
        if rows:
            print(f"  First: {rows[0][:10]}")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

# Try the nba.com data endpoint (used by the website)
print("\n--- nba.com/stats/leaders ---")
try:
    r = requests.get("https://www.nba.com/stats/leaders", headers=headers, timeout=15)
    print(f"Status: {r.status_code}, size: {len(r.text)}")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")
