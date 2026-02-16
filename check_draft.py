import requests, json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Origin": "https://www.nba.com",
}

# Try drafthistory endpoint
print("--- drafthistory ---")
try:
    r = requests.get("https://stats.nba.com/stats/drafthistory", params={
        "LeagueID": "00",
    }, headers=headers, timeout=60)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        rs = data.get("resultSets", [])
        if rs:
            h = rs[0].get("headers", [])
            rows = rs[0].get("rowSet", [])
            print(f"  Headers: {h}")
            print(f"  Total rows: {len(rows)}")
            # Show recent drafts
            # Find column indices
            for row in rows[:5]:
                print(f"  {row}")
            # Show 2024 picks
            year_i = h.index("SEASON") if "SEASON" in h else -1
            name_i = h.index("PLAYER_NAME") if "PLAYER_NAME" in h else -1
            if year_i >= 0:
                picks_2024 = [r for r in rows if str(r[year_i]) == "2024"]
                print(f"\n  2024 draft: {len(picks_2024)} picks")
                for p in picks_2024[:5]:
                    print(f"    {p}")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

# Also try commonallplayers (has FROM_YEAR, TO_YEAR)
print("\n--- commonallplayers ---")
try:
    r = requests.get("https://stats.nba.com/stats/commonallplayers", params={
        "LeagueID": "00",
        "Season": "2025-26",
        "IsOnlyCurrentSeason": "1",
    }, headers=headers, timeout=60)
    print(f"Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        rs = data.get("resultSets", [])
        if rs:
            h = rs[0].get("headers", [])
            rows = rs[0].get("rowSet", [])
            print(f"  Headers: {h}")
            print(f"  Total: {len(rows)} players")
            for row in rows[:3]:
                print(f"  {row}")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")
