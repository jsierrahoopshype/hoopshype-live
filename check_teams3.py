import requests, csv, io

# Load salary data - TEAM IS COL 2, not col 1
sal_url = "https://docs.google.com/spreadsheets/d/11llk0icQqoi0JwJXat5KO8y2RQeBMN5rS9FhAY56Idc/export?format=csv&gid=0"
r = requests.get(sal_url, timeout=30)
r.encoding = "utf-8"

teams = {}
sal_lookup = {}  # full name -> team
name_map = {}    # abbreviated -> full name

for ri, row in enumerate(csv.reader(io.StringIO(r.text))):
    if ri == 0 or len(row) < 6:
        continue
    player = row[0].strip()
    team = row[2].strip()  # COL 2!
    if not player or not team:
        continue
    sal_lookup[player] = team
    teams[team] = teams.get(team, 0) + 1
    parts = player.split(" ", 1)
    if len(parts) == 2:
        abbr = f"{parts[0][0]}. {parts[1]}"
        name_map[abbr] = player

print(f"Salary data: {len(sal_lookup)} players, {len(teams)} teams, {len(name_map)} abbreviations")
print(f"Teams: {sorted(teams.keys())[:5]}...")

# Quick spot checks
for test in ["LeBron James", "Jalen Brunson", "Anthony Edwards", "Ja Morant", "Jaren Jackson Jr"]:
    print(f"  {test} -> {sal_lookup.get(test, 'NOT FOUND')}")

# Load depth chart
dep_url = "https://docs.google.com/spreadsheets/d/14TQPdQ9mDhHMMMQa5vcs0coL98ZloHORtYElDikKoWY/export?format=csv&gid=24771201"
r = requests.get(dep_url, timeout=30)
r.encoding = "utf-8"
rows = list(csv.reader(io.StringIO(r.text)))

pos_set = {"PG", "SG", "SF", "PF", "C"}
headers = []
for ri, row in enumerate(rows):
    if len(row) >= 5:
        cols = [row[i].strip() for i in range(5)]
        if all(c in pos_set for c in cols):
            headers.append(ri)

print(f"\n=== {len(headers)} depth chart blocks ===")
for hi, hrow in enumerate(headers):
    end = headers[hi+1] if hi+1 < len(headers) else len(rows)
    votes = {}
    matched = 0
    unmatched_names = []
    for ri in range(hrow+1, end):
        row = rows[ri]
        if len(row) < 5: continue
        cols = [row[i].strip() for i in range(5)]
        if any(c.startswith("$") for c in cols if c): continue
        if all(c in pos_set for c in cols if c) and sum(1 for c in cols if c in pos_set) >= 3: continue
        for c in cols:
            if not c or c == "\u2014": continue
            # Expand abbreviated name first
            full = name_map.get(c, c)
            t = sal_lookup.get(full, "")
            if t:
                votes[t] = votes.get(t, 0) + 1
                matched += 1
            else:
                unmatched_names.append(c)
    
    winner = max(votes, key=votes.get) if votes else "UNKNOWN"
    vc = votes.get(winner, 0)
    print(f"  [{hi:2d}] {winner:30s} ({vc}/{matched+len(unmatched_names)} matched)")
    if not votes:
        print(f"        unmatched: {unmatched_names[:5]}")
