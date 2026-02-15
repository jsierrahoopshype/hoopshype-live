import requests, json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Origin": "https://www.nba.com",
    "Host": "stats.nba.com",
}

r = requests.get("https://stats.nba.com/stats/leagueleaders", params={
    "Season": "2025-26",
    "SeasonType": "Regular Season",
    "PerMode": "Totals",
    "Scope": "S",
    "StatCategory": "PTS",
    "LeagueID": "00",
    "ActiveFlag": "",
}, headers=headers, timeout=60)

data = r.json()
rs = data.get("resultSet", {})
h = rs.get("headers", [])
rows = rs.get("rowSet", [])

print(f"=== ALL {len(h)} HEADERS ===")
for i, col in enumerate(h):
    print(f"  {i}: {col}")

print(f"\n=== TOP 5 (already sorted by PTS) ===")
for row in rows[:5]:
    line = {h[i]: row[i] for i in range(len(h)) if row[i]}
    print(f"  {line}")

# Check which stats we need
print(f"\n=== STAT CATEGORIES AVAILABLE ===")
stats_needed = ["PTS", "REB", "AST", "STL", "BLK", "FG3M", "FGM", "FGA", "FTM", "FTA"]
for s in stats_needed:
    idx = h.index(s) if s in h else -1
    if idx >= 0:
        # Show top 3 for this stat
        sorted_rows = sorted(rows, key=lambda r: r[idx] or 0, reverse=True)
        top3 = [(r[h.index("PLAYER")], r[h.index("TEAM")], r[idx]) for r in sorted_rows[:3]]
        print(f"  {s:6s} (col {idx}): {top3}")
    else:
        print(f"  {s:6s}: NOT FOUND")
