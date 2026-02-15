import requests, json

# NBA stats API - league dash player stats (totals)
url = "https://stats.nba.com/stats/leaguedashplayerstats"
params = {
    "Season": "2025-26",
    "SeasonType": "Regular Season",
    "PerMode": "Totals",
    "MeasureType": "Base",
    "LeagueID": "00",
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.nba.com/",
    "Accept": "application/json",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}

r = requests.get(url, params=params, headers=headers, timeout=30)
print(f"Status: {r.status_code}")

if r.status_code == 200:
    data = r.json()
    rs = data.get("resultSets", [])
    if rs:
        headers_list = rs[0].get("headers", [])
        rows = rs[0].get("rowSet", [])
        print(f"Headers: {len(headers_list)}")
        print(f"Players: {len(rows)}")
        print(f"\n=== COLUMN HEADERS ===")
        for i, h in enumerate(headers_list):
            print(f"  {i}: {h}")
        print(f"\n=== TOP 3 BY PTS ===")
        pts_idx = headers_list.index("PTS") if "PTS" in headers_list else -1
        if pts_idx >= 0:
            sorted_rows = sorted(rows, key=lambda r: r[pts_idx] or 0, reverse=True)
            for row in sorted_rows[:3]:
                name_idx = headers_list.index("PLAYER_NAME") if "PLAYER_NAME" in headers_list else 1
                team_idx = headers_list.index("TEAM_ABBREVIATION") if "TEAM_ABBREVIATION" in headers_list else 3
                gp_idx = headers_list.index("GP") if "GP" in headers_list else -1
                reb_idx = headers_list.index("REB") if "REB" in headers_list else -1
                ast_idx = headers_list.index("AST") if "AST" in headers_list else -1
                stl_idx = headers_list.index("STL") if "STL" in headers_list else -1
                blk_idx = headers_list.index("BLK") if "BLK" in headers_list else -1
                fg3m_idx = headers_list.index("FG3M") if "FG3M" in headers_list else -1
                print(f"  {row[name_idx]:25s} {row[team_idx]:4s} GP={row[gp_idx]:3d} PTS={row[pts_idx]:5d} REB={row[reb_idx]:4d} AST={row[ast_idx]:4d} STL={row[stl_idx]:3d} BLK={row[blk_idx]:3d} 3PM={row[fg3m_idx]:3d}")
else:
    print(f"Error: {r.text[:500]}")
